/*-------------------------------------------------------------------------
 *
 * stats_statements.c
 *    Statement-level statistics for distributed queries.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/stats_statement_executors.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "storage/spin.h"
#include "tcop/utility.h"

#include <unistd.h>

#define PGDS_DUMP_FILE "pg_stat/pg_dist_statements.stat"
#define PG_DIST_STATEMENTS_COLS 5
#define USAGE_DECREASE_FACTOR (0.99)    /* decreased every StatStatementsEntryDealloc */
#define STICKY_DECREASE_FACTOR (0.50)   /* factor for sticky entries */
#define USAGE_DEALLOC_PERCENT 5         /* free this % of entries at once */
#define USAGE_INIT (1.0)                /* including initial planning */

static const uint32 PGDS_FILE_HEADER = 0x0d756e0f;
static int pgds_max = 0;    /* max #queries to store. pg_stat_statements.max is used */

/*
 * Hashtable key that defines the identity of a hashtable entry.  We use the
 * same hash as pg_stat_statements
 */
typedef struct pgdsHashKey
{
	Oid userid;                     /* user OID */
	Oid dbid;                       /* database OID */
	uint32 queryid;                 /* query identifier */
	MultiExecutorType executorType; /* executor type */
} pgdsHashKey;

/*
 * Statistics per query and executor type
 */
typedef struct pgdsEntry
{
	pgdsHashKey key;   /* hash key of entry - MUST BE FIRST */
	int64 calls;       /* # of times executed */
	double usage;      /* hashtable usage factor */
	slock_t mutex;     /* protects the counters only */
} pgdsEntry;

/*
 * Global shared state
 */
typedef struct pgdsSharedState
{
	LWLockId lock;                      /* protects hashtable search/modification */
	double cur_median_usage;            /* current median usage in hashtable */
} pgdsSharedState;

/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Links to shared memory state */
static pgdsSharedState *pgds = NULL;
static HTAB *pgds_hash = NULL;

/*--- Functions --- */

Datum citus_statement_executors_reset(PG_FUNCTION_ARGS);
Datum citus_statement_executors(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(citus_statement_executors_reset);
PG_FUNCTION_INFO_V1(citus_statement_executors);

static void StatStatementsSetMax(void);
static Size StatStatementsMemSize(void);

static void StatStatementsShmemStartup(void);
static void StatStatementsShmemShudown(int code, Datum arg);
static pgdsEntry * StatStatementsEntryAlloc(pgdsHashKey *key, bool sticky);
static void StatStatementsEntryDealloc(void);
static void StatStatementsEntryReset(void);
static uint32 StatStatementsHashFn(const void *key, Size keysize);
static int StatStatementsMatchFn(const void *key1, const void *key2, Size keysize);

void
InitializeStatsStatementExecutors(void)
{
	/* set pgds_max if needed */
	StatStatementsSetMax();
	RequestAddinShmemSpace(StatStatementsMemSize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pg_dist_statements", 1);
#else
	RequestAddinLWLocks(1);
#endif

	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = StatStatementsShmemStartup;
}


static void
StatStatementsShmemStartup(void)
{
	bool found;
	HASHCTL info;
	FILE *file;
	int i;
	uint32 header;
	int32 num;
	pgdsEntry *buffer = NULL;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	/* reset in case this is a restart within the postmaster */
	pgds = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	pgds = ShmemInitStruct("pg_dist_statements",
						   sizeof(pgdsSharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		pgds->lock = &(GetNamedLWLockTranche("pg_dist_statements"))->lock;
#else
		pgds->lock = LWLockAssign();
#endif
	}

	/* set pgds_max if needed */
	StatStatementsSetMax();

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgdsHashKey);
	info.entrysize = sizeof(pgdsEntry);
	info.hash = StatStatementsHashFn;
	info.match = StatStatementsMatchFn;

	/* allocate stats shared memory hash */
	pgds_hash = ShmemInitHash("pg_dist_statements hash",
							  pgds_max, pgds_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
	{
		on_shmem_exit(StatStatementsShmemShudown, (Datum) 0);
	}

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
	{
		return;
	}

	/* Load stat file, don't care about locking */
	file = AllocateFile(PGDS_DUMP_FILE, PG_BINARY_R);
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
		header != PGDS_FILE_HEADER)
	{
		goto error;
	}

	/* get number of entries */
	if (fread(&num, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	for (i = 0; i < num; i++)
	{
		pgdsEntry temp;
		pgdsEntry *entry;

		if (fread(&temp, sizeof(pgdsEntry), 1, file) != 1)
		{
			goto error;
		}

		/* Skip loading "sticky" entries */
		if (temp.calls == 0)
		{
			continue;
		}

		entry = StatStatementsEntryAlloc(&temp.key, false);

		/* copy in the actual stats */
		entry->calls = temp.calls;
		entry->usage = temp.usage;

		/* don't initialize spinlock, already done */
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(PGDS_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_dist_statements file \"%s\": %m",
					PGDS_DUMP_FILE)));
	if (buffer)
	{
		pfree(buffer);
	}
	if (file)
	{
		FreeFile(file);
	}

	/* delete bogus file, don't care of errors in this case */
	unlink(PGDS_DUMP_FILE);
}


/*
 * shmem_shutdown hook: dump statistics into file.
 *
 */
static void
StatStatementsShmemShudown(int code, Datum arg)
{
	FILE *file;
	HASH_SEQ_STATUS hash_seq;
	int32 num_entries;
	pgdsEntry *entry;

	/* Don't try to dump during a crash. */
	if (code)
	{
		return;
	}

	if (!pgds)
	{
		return;
	}

	file = AllocateFile(PGDS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
	{
		goto error;
	}

	if (fwrite(&PGDS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	num_entries = hash_get_num_entries(pgds_hash);

	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	hash_seq_init(&hash_seq, pgds_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgdsEntry), 1, file) != 1)
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
	if (rename(PGDS_DUMP_FILE ".tmp", PGDS_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename pg_dist_statements file \"%s\": %m",
						PGDS_DUMP_FILE ".tmp")));
	}

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_dist_statements file \"%s\": %m",
					PGDS_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}
	unlink(PGDS_DUMP_FILE);
}


/*
 * Retrieve pg_stat_statement.max GUC value and store it into pgds_max, since
 * we want to store the same number of entries as pg_stat_statements. Don't do
 * anything if pgds_max is already set.
 */
static void
StatStatementsSetMax(void)
{
	const char *pgss_max;
	const char *name = "pg_stat_statements.max";

	if (pgds_max != 0)
	{
		return;
	}

	pgss_max = GetConfigOption(name, true, false);

	/*
	 * Retrieving pg_stat_statements.max can fail if pgss is loaded after citus
	 * in shared_preload_libraries, or not at all. Don't do any tracking in such
	 * a case.
	 */
	if (!pgss_max)
	{
		pgds_max = 1000;
	}
	else
	{
		pgds_max = atoi(pgss_max);
	}
}


static Size
StatStatementsMemSize(void)
{
	Size size;

	Assert(pgds_max != 0);

	size = MAXALIGN(sizeof(pgdsSharedState));
	size = add_size(size, hash_estimate_size(pgds_max, sizeof(pgdsEntry)));

	return size;
}


void
StoreStatsStatementExecutorsEntry(uint32 queryId, MultiExecutorType executorType)
{
	volatile pgdsEntry *e;

	pgdsHashKey key;
	pgdsEntry *entry;

	/* Safety check... */
	if (!pgds || !pgds_hash)
	{
		return;
	}

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.executorType = executorType;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgds->lock, LW_SHARED);

	entry = (pgdsEntry *) hash_search(pgds_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgds->lock);
		LWLockAcquire(pgds->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = StatStatementsEntryAlloc(&key, false);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the pg_stat_statements file)
	 */
	e = (volatile pgdsEntry *) entry;

	SpinLockAcquire(&e->mutex);

	/* "Unstick" entry if it was previously sticky */
	if (e->calls == 0)
	{
		e->usage = USAGE_INIT;
	}

	e->calls += 1;

	SpinLockRelease(&e->mutex);

	LWLockRelease(pgds->lock);
}


/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgds->lock
 */
static pgdsEntry *
StatStatementsEntryAlloc(pgdsHashKey *key, bool sticky)
{
	pgdsEntry *entry;
	bool found;

	/* Make space if needed */
	while (hash_get_num_entries(pgds_hash) >= pgds_max)
	{
		StatStatementsEntryDealloc();
	}

	/* Find or create an entry with desired hash code */
	entry = (pgdsEntry *) hash_search(pgds_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* set the appropriate initial usage count */
		entry->usage = sticky ? pgds->cur_median_usage : USAGE_INIT;

		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	entry->calls = 0;
	entry->usage = (0.0);

	return entry;
}


/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double l_usage = (*(pgdsEntry *const *) lhs)->usage;
	double r_usage = (*(pgdsEntry *const *) rhs)->usage;

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
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgds->lock.
 */
static void
StatStatementsEntryDealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgdsEntry **entries;
	pgdsEntry *entry;
	int nvictims;
	int i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */
	entries = palloc(hash_get_num_entries(pgds_hash) * sizeof(pgdsEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgds_hash);
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

	qsort(entries, i, sizeof(pgdsEntry *), entry_cmp);

	if (i > 0)
	{
		/* Record the (approximate) median usage */
		pgds->cur_median_usage = entries[i / 2]->usage;
	}

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgds_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}


static void
StatStatementsEntryReset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgdsEntry *entry;

	LWLockAcquire(pgds->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgds_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgds_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgds->lock);
}


/*
 * Calculate hash value for a key
 */
static uint32
StatStatementsHashFn(const void *key, Size keysize)
{
	const pgdsHashKey *k = (const pgdsHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		   hash_uint32((uint32) k->dbid) ^
		   hash_uint32((uint32) k->queryid) ^
		   hash_uint32((uint32) k->executorType);
}


/*
 * Compare two keys - zero means match
 */
static int
StatStatementsMatchFn(const void *key1, const void *key2, Size keysize)
{
	const pgdsHashKey *k1 = (const pgdsHashKey *) key1;
	const pgdsHashKey *k2 = (const pgdsHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid &&
		k1->executorType == k2->executorType)
	{
		return 0;
	}
	else
	{
		return 1;
	}
}


/*
 * Reset statistics.
 */
Datum
citus_statement_executors_reset(PG_FUNCTION_ARGS)
{
	StatStatementsEntryReset();
	PG_RETURN_VOID();
}


Datum
citus_statement_executors(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	HASH_SEQ_STATUS hash_seq;
	pgdsEntry *entry;

	if (!pgds)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_dist_statements: shared memory not initialized")));
	}

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}
	if (!(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgds->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgds_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum values[PG_DIST_STATEMENTS_COLS];
		bool nulls[PG_DIST_STATEMENTS_COLS];
		int i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		SpinLockAcquire(&entry->mutex);

		/* Skip entry if unexecuted (ie, it's a pending "sticky" entry) */
		if (entry->calls == 0)
		{
			SpinLockRelease(&entry->mutex);
			continue;
		}
		values[i++] = Int64GetDatum(entry->key.queryid);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		values[i++] = ObjectIdGetDatum(entry->key.executorType);
		values[i++] = Int64GetDatumFast(entry->calls);
		SpinLockRelease(&entry->mutex);

		Assert(i == PG_DIST_STATEMENTS_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgds->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
