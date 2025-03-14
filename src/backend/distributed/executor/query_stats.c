/*-------------------------------------------------------------------------
 *
 * query_stats.c
 *    Statement-level statistics for distributed queries.
 *    Code is mostly taken from postgres/contrib/pg_stat_statements
 *    and adapted to citus.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "safe_lib.h"

#include "access/hash.h"
#include "catalog/pg_authid.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

#include "pg_version_constants.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/function_utils.h"
#include "distributed/hash_helpers.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_stats.h"
#include "distributed/tuplestore.h"
#include "distributed/version_compat.h"

#define CITUS_STATS_DUMP_FILE "pg_stat/citus_query_stats.stat"
#define CITUS_STAT_STATEMENTS_COLS 6
#define CITUS_STAT_STATAMENTS_QUERY_ID 0
#define CITUS_STAT_STATAMENTS_USER_ID 1
#define CITUS_STAT_STATAMENTS_DB_ID 2
#define CITUS_STAT_STATAMENTS_EXECUTOR_TYPE 3
#define CITUS_STAT_STATAMENTS_PARTITION_KEY 4
#define CITUS_STAT_STATAMENTS_CALLS 5


#define USAGE_DECREASE_FACTOR (0.99)    /* decreased every CitusQueryStatsEntryDealloc */
#define STICKY_DECREASE_FACTOR (0.50)   /* factor for sticky entries */
#define USAGE_DEALLOC_PERCENT 5         /* free this % of entries at once */
#define USAGE_INIT (1.0)                /* including initial planning */

#define MAX_KEY_LENGTH NAMEDATALEN

static const uint32 CITUS_QUERY_STATS_FILE_HEADER = 0x0d756e0f;

/* time interval in seconds for maintenance daemon to call CitusQueryStatsSynchronizeEntries */
int StatStatementsPurgeInterval = 10;

/* maximum number of entries in queryStats hash, controlled by GUC citus.stat_statements_max */
int StatStatementsMax = 50000;

/* tracking all or none, for citus_stat_statements, controlled by GUC citus.stat_statements_track */
int StatStatementsTrack = STAT_STATEMENTS_TRACK_NONE;

/*
 * Hashtable key that defines the identity of a hashtable entry.  We use the
 * same hash as pg_stat_statements
 */
typedef struct QueryStatsHashKey
{
	Oid userid;                     /* user OID */
	Oid dbid;                       /* database OID */
	uint64 queryid;                 /* query identifier */
	MultiExecutorType executorType; /* executor type */
	char partitionKey[MAX_KEY_LENGTH];
} QueryStatsHashKey;

/*
 * Statistics per query and executor type
 */
typedef struct queryStatsEntry
{
	QueryStatsHashKey key;   /* hash key of entry - MUST BE FIRST */
	int64 calls;       /* # of times executed */
	double usage;      /* hashtable usage factor */
	slock_t mutex;     /* protects the counters only */
} QueryStatsEntry;

/*
 * Global shared state
 */
typedef struct QueryStatsSharedState
{
	LWLockId lock;                      /* protects hashtable search/modification */
	double cur_median_usage;            /* current median usage in hashtable */
} QueryStatsSharedState;

/* lookup table for existing pg_stat_statements entries */
typedef struct ExistingStatsHashKey
{
	Oid userid;                     /* user OID */
	Oid dbid;                       /* database OID */
	uint64 queryid;                 /* query identifier */
} ExistingStatsHashKey;

/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Links to shared memory state */
static QueryStatsSharedState *queryStats = NULL;
static HTAB *queryStatsHash = NULL;

/*--- Functions --- */

Datum citus_query_stats_reset(PG_FUNCTION_ARGS);
Datum citus_query_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(citus_stat_statements_reset);
PG_FUNCTION_INFO_V1(citus_query_stats);
PG_FUNCTION_INFO_V1(citus_executor_name);


static char * CitusExecutorName(MultiExecutorType executorType);


static void CitusQueryStatsShmemStartup(void);
static void CitusQueryStatsShmemShutdown(int code, Datum arg);
static QueryStatsEntry * CitusQueryStatsEntryAlloc(QueryStatsHashKey *key, bool sticky);
static void CitusQueryStatsEntryDealloc(void);
static void CitusQueryStatsEntryReset(void);
static uint32 CitusQuerysStatsHashFn(const void *key, Size keysize);
static int CitusQuerysStatsMatchFn(const void *key1, const void *key2, Size keysize);

static HTAB * BuildExistingQueryIdHash(void);
static int GetPGStatStatementsMax(void);
static void CitusQueryStatsRemoveExpiredEntries(HTAB *existingQueryIdHash);

void
InitializeCitusQueryStats(void)
{
	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = CitusQueryStatsShmemStartup;
}


static void
CitusQueryStatsShmemStartup(void)
{
	bool found;
	HASHCTL info;
	uint32 header;
	int32 num;
	QueryStatsEntry *buffer = NULL;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	queryStats = ShmemInitStruct(STATS_SHARED_MEM_NAME,
								 sizeof(QueryStatsSharedState),
								 &found);

	if (!found)
	{
		/* First time through ... */
		queryStats->lock = &(GetNamedLWLockTranche(STATS_SHARED_MEM_NAME))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(QueryStatsHashKey);
	info.entrysize = sizeof(QueryStatsEntry);
	info.hash = CitusQuerysStatsHashFn;
	info.match = CitusQuerysStatsMatchFn;

	/* allocate stats shared memory hash */
	queryStatsHash = ShmemInitHash("citus_query_stats hash",
								   StatStatementsMax, StatStatementsMax,
								   &info,
								   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
	{
		on_shmem_exit(CitusQueryStatsShmemShutdown, (Datum) 0);
	}

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
	{
		return;
	}

	/* Load stat file, don't care about locking */
	FILE *file = AllocateFile(CITUS_STATS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			return;         /* ignore not-found error */
		}
		goto error;
	}

	/* check is header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != CITUS_QUERY_STATS_FILE_HEADER)
	{
		goto error;
	}

	/* get number of entries */
	if (fread(&num, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	for (int i = 0; i < num; i++)
	{
		QueryStatsEntry temp;

		if (fread(&temp, sizeof(QueryStatsEntry), 1, file) != 1)
		{
			goto error;
		}

		/* Skip loading "sticky" entries */
		if (temp.calls == 0)
		{
			continue;
		}

		QueryStatsEntry *entry = CitusQueryStatsEntryAlloc(&temp.key, false);

		/* copy in the actual stats */
		entry->calls = temp.calls;
		entry->usage = temp.usage;

		/* don't initialize spinlock, already done */
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replicas, etc. A new file will be
	 * written on next shutdown.
	 */
	unlink(CITUS_STATS_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read citus_query_stats file \"%s\": %m",
					CITUS_STATS_DUMP_FILE)));
	if (buffer)
	{
		pfree(buffer);
	}
	if (file)
	{
		FreeFile(file);
	}

	/* delete bogus file, don't care of errors in this case */
	unlink(CITUS_STATS_DUMP_FILE);
}


/*
 * CitusQueryStatsShmemShutdown is a shmem_shutdown hook,
 * it dumps statistics into file.
 */
static void
CitusQueryStatsShmemShutdown(int code, Datum arg)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;

	/* Don't try to dump during a crash. */
	if (code)
	{
		return;
	}

	if (!queryStats)
	{
		return;
	}

	FILE *file = AllocateFile(CITUS_STATS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
	{
		goto error;
	}

	if (fwrite(&CITUS_QUERY_STATS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	int32 num_entries = hash_get_num_entries(queryStatsHash);

	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(QueryStatsEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file inplace
	 */
	if (rename(CITUS_STATS_DUMP_FILE ".tmp", CITUS_STATS_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename citus_query_stats file \"%s\": %m",
						CITUS_STATS_DUMP_FILE ".tmp")));
	}

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read citus_query_stats file \"%s\": %m",
					CITUS_STATS_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}
	unlink(CITUS_STATS_DUMP_FILE);
}


/*
 * CitusQueryStatsSharedMemSize calculates and returns shared memory size
 * required to keep query statistics.
 */
Size
CitusQueryStatsSharedMemSize(void)
{
	Assert(StatStatementsMax >= 0);

	Size size = MAXALIGN(sizeof(QueryStatsSharedState));
	size = add_size(size, hash_estimate_size(StatStatementsMax, sizeof(QueryStatsEntry)));

	return size;
}


/*
 * CitusQueryStatsExecutorsEntry is the function to update statistics
 * for a given query id.
 */
void
CitusQueryStatsExecutorsEntry(uint64 queryId, MultiExecutorType executorType,
							  char *partitionKey)
{
	QueryStatsHashKey key;

	/* Safety check... */
	if (!queryStats || !queryStatsHash)
	{
		return;
	}

	/* early return if tracking is disabled */
	if (!StatStatementsTrack)
	{
		return;
	}

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.executorType = executorType;
	memset(key.partitionKey, 0, MAX_KEY_LENGTH);
	if (partitionKey != NULL)
	{
		strlcpy(key.partitionKey, partitionKey, MAX_KEY_LENGTH);
	}

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(queryStats->lock, LW_SHARED);

	QueryStatsEntry *entry = (QueryStatsEntry *) hash_search(queryStatsHash, &key,
															 HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(queryStats->lock);
		LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = CitusQueryStatsEntryAlloc(&key, false);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the pg_stat_statements file)
	 */
	volatile QueryStatsEntry *e = (volatile QueryStatsEntry *) entry;

	SpinLockAcquire(&e->mutex);

	/* "Unstick" entry if it was previously sticky */
	if (e->calls == 0)
	{
		e->usage = USAGE_INIT;
	}

	e->calls += 1;

	SpinLockRelease(&e->mutex);

	LWLockRelease(queryStats->lock);
}


/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on queryStats->lock
 */
static QueryStatsEntry *
CitusQueryStatsEntryAlloc(QueryStatsHashKey *key, bool sticky)
{
	bool found;
	long StatStatementsMaxLong = StatStatementsMax;

	/* Make space if needed */
	while (hash_get_num_entries(queryStatsHash) >= StatStatementsMaxLong)
	{
		CitusQueryStatsEntryDealloc();
	}

	/* Find or create an entry with desired hash code */
	QueryStatsEntry *entry = (QueryStatsEntry *) hash_search(queryStatsHash, key,
															 HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* set the appropriate initial usage count */
		entry->usage = sticky ? queryStats->cur_median_usage : USAGE_INIT;

		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	entry->calls = 0;
	entry->usage = (0.0);

	return entry;
}


/*
 * entry_cmp is qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double l_usage = (*(QueryStatsEntry *const *) lhs)->usage;
	double r_usage = (*(QueryStatsEntry *const *) rhs)->usage;

	if (l_usage < r_usage)
	{
		return -1;
	}
	else if (l_usage > r_usage)
	{
		return +1;
	}
	else
	{
		return 0;
	}
}


/*
 * CitusQueryStatsEntryDealloc deallocates least used entries.
 * Caller must hold an exclusive lock on queryStats->lock.
 */
static void
CitusQueryStatsEntryDealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */
	QueryStatsEntry **entries = palloc(hash_get_num_entries(queryStatsHash) *
									   sizeof(QueryStatsEntry *));

	int i = 0;
	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;

		/* "Sticky" entries get a different usage decay rate. */
		if (entry->calls == 0)
		{
			entry->usage *= STICKY_DECREASE_FACTOR;
		}
		else
		{
			entry->usage *= USAGE_DECREASE_FACTOR;
		}
	}

	SafeQsort(entries, i, sizeof(QueryStatsEntry *), entry_cmp);

	if (i > 0)
	{
		/* Record the (approximate) median usage */
		queryStats->cur_median_usage = entries[i / 2]->usage;
	}

	int nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(queryStatsHash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}


/*
 * CitusQueryStatsEntryReset resets statistics.
 */
static void
CitusQueryStatsEntryReset(void)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;

	LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(queryStatsHash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(queryStats->lock);
}


/*
 * CitusQuerysStatsHashFn calculates and returns hash value for a key
 */
static uint32
CitusQuerysStatsHashFn(const void *key, Size keysize)
{
	const QueryStatsHashKey *k = (const QueryStatsHashKey *) key;

	if (k->partitionKey[0] != '\0')
	{
		return hash_uint32((uint32) k->userid) ^
			   hash_uint32((uint32) k->dbid) ^
			   hash_any((const unsigned char *) &(k->queryid), sizeof(uint64)) ^
			   hash_uint32((uint32) k->executorType) ^
			   hash_any((const unsigned char *) (k->partitionKey), strlen(
							k->partitionKey));
	}
	else
	{
		return hash_uint32((uint32) k->userid) ^
			   hash_uint32((uint32) k->dbid) ^
			   hash_any((const unsigned char *) &(k->queryid), sizeof(uint64)) ^
			   hash_uint32((uint32) k->executorType);
	}
}


/*
 * CitusQuerysStatsMatchFn compares two keys - zero means match.
 * See definition of HashCompareFunc in hsearch.h for more info.
 */
static int
CitusQuerysStatsMatchFn(const void *key1, const void *key2, Size keysize)
{
	const QueryStatsHashKey *k1 = (const QueryStatsHashKey *) key1;
	const QueryStatsHashKey *k2 = (const QueryStatsHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid &&
		k1->executorType == k2->executorType)
	{
		return 0;
	}
	return 1;
}


/*
 * Reset statistics.
 */
Datum
citus_stat_statements_reset(PG_FUNCTION_ARGS)
{
	CitusQueryStatsEntryReset();
	PG_RETURN_VOID();
}


/*
 * citus_query_stats returns query stats kept in memory.
 */
Datum
citus_query_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;
	Oid currentUserId = GetUserId();
	bool canSeeStats = superuser();

	if (!queryStats)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("citus_query_stats: shared memory not initialized")));
	}

	if (is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS))
	{
		canSeeStats = true;
	}

	Tuplestorestate *tupstore = SetupTuplestore(fcinfo, &tupdesc);


	/* exclusive lock on queryStats->lock is acquired and released inside the function */
	CitusQueryStatsSynchronizeEntries();

	LWLockAcquire(queryStats->lock, LW_SHARED);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum values[CITUS_STAT_STATEMENTS_COLS];
		bool nulls[CITUS_STAT_STATEMENTS_COLS];

		/* following vars are to keep data for processing after spinlock release */
		uint64 queryid = 0;
		Oid userid = InvalidOid;
		Oid dbid = InvalidOid;
		MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;
		char partitionKey[MAX_KEY_LENGTH];
		int64 calls = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		memset(partitionKey, 0, MAX_KEY_LENGTH);

		SpinLockAcquire(&entry->mutex);

		/*
		 * Skip entry if unexecuted (ie, it's a pending "sticky" entry) or
		 * the user does not have permission to view it.
		 */
		if (entry->calls == 0 || !(currentUserId == entry->key.userid || canSeeStats))
		{
			SpinLockRelease(&entry->mutex);
			continue;
		}

		queryid = entry->key.queryid;
		userid = entry->key.userid;
		dbid = entry->key.dbid;
		executorType = entry->key.executorType;

		if (entry->key.partitionKey[0] != '\0')
		{
			memcpy_s(partitionKey, sizeof(partitionKey), entry->key.partitionKey,
					 sizeof(entry->key.partitionKey));
		}

		calls = entry->calls;

		SpinLockRelease(&entry->mutex);

		values[CITUS_STAT_STATAMENTS_QUERY_ID] = UInt64GetDatum(queryid);
		values[CITUS_STAT_STATAMENTS_USER_ID] = ObjectIdGetDatum(userid);
		values[CITUS_STAT_STATAMENTS_DB_ID] = ObjectIdGetDatum(dbid);
		values[CITUS_STAT_STATAMENTS_EXECUTOR_TYPE] = UInt32GetDatum(
			(uint32) executorType);

		if (partitionKey[0] != '\0')
		{
			values[CITUS_STAT_STATAMENTS_PARTITION_KEY] = CStringGetTextDatum(
				partitionKey);
		}
		else
		{
			nulls[CITUS_STAT_STATAMENTS_PARTITION_KEY] = true;
		}

		values[CITUS_STAT_STATAMENTS_CALLS] = Int64GetDatumFast(calls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(queryStats->lock);

	return (Datum) 0;
}


/*
 * CitusQueryStatsSynchronizeEntries removes all entries in queryStats hash
 * that does not have matching queryId in pg_stat_statements.
 *
 * A function called inside (CitusQueryStatsRemoveExpiredEntries) acquires
 * an exclusive lock on queryStats->lock.
 */
void
CitusQueryStatsSynchronizeEntries(void)
{
	HTAB *existingQueryIdHash = BuildExistingQueryIdHash();
	if (existingQueryIdHash != NULL)
	{
		CitusQueryStatsRemoveExpiredEntries(existingQueryIdHash);
		hash_destroy(existingQueryIdHash);
	}
}


/*
 * BuildExistingQueryIdHash goes over entries in pg_stat_statements and prepare
 * a hash table of queryId's. The function returns null if
 * public.pg_stat_statements(bool) function is not available. Returned hash
 * table is allocated on the CurrentMemoryContext, and caller is responsible
 * for deallocation.
 */
static HTAB *
BuildExistingQueryIdHash(void)
{
	const int userIdAttributeNumber = 1;
	const int dbIdAttributeNumber = 2;
	const int queryIdAttributeNumber = 4;
	Datum commandTypeDatum = (Datum) 0;
	bool missingOK = true;

	Oid pgStatStatementsOid = FunctionOidExtended("public", "pg_stat_statements", 1,
												  missingOK);
	if (!OidIsValid(pgStatStatementsOid))
	{
		return NULL;
	}


	/* fetch pg_stat_statements.max, it is expected to be available, if not bail out */
	int pgStatStatementsMax = GetPGStatStatementsMax();
	if (pgStatStatementsMax == 0)
	{
		ereport(DEBUG1, (errmsg("Cannot access pg_stat_statements.max")));
		return NULL;
	}

	FmgrInfo *fmgrPGStatStatements = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	commandTypeDatum = BoolGetDatum(false);

	fmgr_info(pgStatStatementsOid, fmgrPGStatStatements);

	ReturnSetInfo *statStatementsReturnSet = FunctionCallGetTupleStore1(
		fmgrPGStatStatements->fn_addr,
		pgStatStatementsOid,
		commandTypeDatum);
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(
		statStatementsReturnSet->setDesc,
		&TTSOpsMinimalTuple);

	/*
	 * Allocate more hash slots (twice as much) than necessary to minimize
	 * collisions.
	 */
	assert_valid_hash_key3(ExistingStatsHashKey, userid, dbid, queryid);
	HTAB *queryIdHashTable = CreateSimpleHashSetWithNameAndSize(
		ExistingStatsHashKey,
		"pg_stats_statements queryId hash",
		pgStatStatementsMax * 2);

	/* iterate over tuples in tuple store, and add queryIds to hash table */
	while (true)
	{
		bool isNull = false;

		bool tuplePresent = tuplestore_gettupleslot(statStatementsReturnSet->setResult,
													true,
													false,
													tupleTableSlot);

		if (!tuplePresent)
		{
			break;
		}

		Datum userIdDatum = slot_getattr(tupleTableSlot, userIdAttributeNumber, &isNull);
		Datum dbIdDatum = slot_getattr(tupleTableSlot, dbIdAttributeNumber, &isNull);
		Datum queryIdDatum = slot_getattr(tupleTableSlot, queryIdAttributeNumber,
										  &isNull);


		/*
		 * queryId may be returned as NULL when current user is not authorized to see other
		 * users' stats.
		 */
		if (!isNull)
		{
			ExistingStatsHashKey key;
			key.userid = DatumGetInt32(userIdDatum);
			key.dbid = DatumGetInt32(dbIdDatum);
			key.queryid = DatumGetInt64(queryIdDatum);
			hash_search(queryIdHashTable, (void *) &key, HASH_ENTER, NULL);
		}

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	tuplestore_end(statStatementsReturnSet->setResult);

	pfree(fmgrPGStatStatements);

	return queryIdHashTable;
}


/*
 * GetPGStatStatementsMax returns GUC value pg_stat_statements.max. The
 * function returns 0 if for some reason it can not access
 * pg_stat_statements.max value.
 */
static int
GetPGStatStatementsMax(void)
{
	const char *name = "pg_stat_statements.max";
	int maxValue = 0;

	const char *pgssMax = GetConfigOption(name, true, false);

	/*
	 * Retrieving pg_stat_statements.max can fail if the extension is loaded
	 * after citus in shared_preload_libraries, or not at all.
	 */
	if (pgssMax)
	{
		maxValue = pg_strtoint32(pgssMax);
	}

	return maxValue;
}


/*
 * CitusQueryStatsRemoveExpiredEntries iterates over queryStats hash entries
 * and removes entries with keys that do not exists in the provided hash of
 * queryIds.
 *
 * Acquires and releases exclusive lock on queryStats->lock.
 */
static void
CitusQueryStatsRemoveExpiredEntries(HTAB *existingQueryIdHash)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;
	int removedCount = 0;
	bool canSeeStats = superuser();
	Oid currentUserId = GetUserId();

	if (is_member_of_role(currentUserId, ROLE_PG_READ_ALL_STATS))
	{
		canSeeStats = true;
	}

	LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool found = false;
		ExistingStatsHashKey existingStatsKey = { 0, 0, 0 };

		/*
		 * pg_stat_statements returns NULL in the queryId field for queries
		 * belonging to other users. Those queries are therefore not reflected
		 * in the existingQueryIdHash, but that does not mean that we should
		 * remove them as they are relevant to other users.
		 */
		if (!(currentUserId == entry->key.userid || canSeeStats))
		{
			continue;
		}

		existingStatsKey.userid = entry->key.userid;
		existingStatsKey.dbid = entry->key.dbid;
		existingStatsKey.queryid = entry->key.queryid;

		hash_search(existingQueryIdHash, (void *) &existingStatsKey, HASH_FIND, &found);
		if (!found)
		{
			hash_search(queryStatsHash, &entry->key, HASH_REMOVE, NULL);
			removedCount++;
		}
	}

	LWLockRelease(queryStats->lock);

	if (removedCount > 0)
	{
		elog(DEBUG2, "citus_stat_statements removed %d expired entries", removedCount);
	}
}


/*
 * citus_executor_name is a UDF that returns the name of the executor
 * given the internal enum value.
 */
Datum
citus_executor_name(PG_FUNCTION_ARGS)
{
	MultiExecutorType executorType = PG_GETARG_UINT32(0);

	char *executorName = CitusExecutorName(executorType);

	PG_RETURN_TEXT_P(cstring_to_text(executorName));
}


/*
 * CitusExecutorName returns the name of the executor given the internal
 * enum value.
 */
static char *
CitusExecutorName(MultiExecutorType executorType)
{
	switch (executorType)
	{
		case MULTI_EXECUTOR_ADAPTIVE:
		{
			return "adaptive";
		}

		case MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT:
		{
			return "insert-select";
		}

		default:
		{
			return "unknown";
		}
	}
}
