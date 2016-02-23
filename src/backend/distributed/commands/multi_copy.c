/*-------------------------------------------------------------------------
 *
 * multi_copy.c
 *     This file contains implementation of COPY utility for distributed
 *     tables.
 *
 * Contributed by Konstantin Knizhnik, Postgres Professional
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "plpgsql.h"

#include <string.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "access/sdir.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "commands/extension.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_manager.h"
#include "distributed/worker_protocol.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001


/* the transaction manager to use for COPY commands */
int CopyTransactionManager = TRANSACTION_MANAGER_1PC;


/* Data structures copy.c, to keep track of COPY processing state */
typedef enum CopyDest
{
	COPY_FILE,                  /* to/from file (or a piped program) */
	COPY_OLD_FE,                /* to/from frontend (2.0 protocol) */
	COPY_NEW_FE                 /* to/from frontend (3.0 protocol) */
} CopyDest;

typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest copy_dest;         /* type of copy source/destination */
	FILE *copy_file;            /* used if copy_dest == COPY_FILE */
	StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool fe_eof;                /* true if detected end of copy data */
	EolType eol_type;           /* EOL type of input */
	int file_encoding;          /* file or remote side's character encoding */
	bool need_transcoding;              /* file encoding diff from server? */
	bool encoding_embeds_ascii;         /* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation rel;               /* relation to copy to or from */
	QueryDesc *queryDesc;       /* executable query to copy from */
	List *attnumlist;           /* integer list of attnums to copy */
	char *filename;             /* filename, or NULL for STDIN/STDOUT */
	bool is_program;            /* is 'filename' a program to popen? */
	bool binary;                /* binary format? */
	bool oids;                  /* include OIDs? */
	bool freeze;                /* freeze rows on loading? */
	bool csv_mode;              /* Comma Separated Value format? */
	bool header_line;           /* CSV header line? */
	char *null_print;           /* NULL marker string (server encoding!) */
	int null_print_len;         /* length of same */
	char *null_print_client;            /* same converted to file encoding */
	char *delim;                /* column delimiter (must be 1 byte) */
	char *quote;                /* CSV quote char (must be 1 byte) */
	char *escape;               /* CSV escape char (must be 1 byte) */
	List *force_quote;          /* list of column names */
	bool force_quote_all;           /* FORCE QUOTE *? */
	bool *force_quote_flags;            /* per-column CSV FQ flags */
	List *force_notnull;        /* list of column names */
	bool *force_notnull_flags;          /* per-column CSV FNN flags */
#if PG_VERSION_NUM >= 90400
	List *force_null;           /* list of column names */
	bool *force_null_flags;             /* per-column CSV FN flags */
#endif
	bool convert_selectively;           /* do selective binary conversion? */
	List *convert_select;       /* list of column names (can be NIL) */
	bool *convert_select_flags;         /* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;    /* table name for error messages */
	int cur_lineno;             /* line number for error messages */
	const char *cur_attname;    /* current att for error messages */
	const char *cur_attval;     /* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;  /* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo *out_functions;    /* lookup info for output functions */
	MemoryContext rowcontext;   /* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber num_defaults;
	bool file_has_oids;
	FmgrInfo oid_in_function;
	Oid oid_typioparam;
	FmgrInfo *in_functions;     /* array of input functions for each attrs */
	Oid *typioparams;           /* array of element types for in_functions */
	int *defmap;                /* array of default att numbers */
	ExprState **defexprs;       /* array of default att expressions */
	bool volatile_defexprs;             /* is any of defexprs volatile? */
	List *range_table;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int max_fields;
	char **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool line_buf_converted;            /* converted to server encoding? */
	bool line_buf_valid;        /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).	CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.	 Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536      /* we palloc RAW_BUF_SIZE+1 bytes */
	char *raw_buf;
	int raw_buf_index;          /* next byte to process */
	int raw_buf_len;            /* total # of bytes stored */
} CopyStateData;


/* Data structures for keeping track of connections to placements */
typedef struct PlacementConnection
{
	int64 shardId;
	bool prepared;
	PGconn* connection;
} PlacementConnection;

typedef struct ShardConnections
{
	int64 shardId;
	List *connectionList;
} ShardConnections;


/* Local functions forward declarations */
static HTAB * CreateShardConnectionHash(void);
static bool IsUniformHashDistribution(ShardInterval **shardIntervalArray,
									  int shardCount);
static FmgrInfo * ShardIntervalCompareFunction(Var *partitionColumn, char partitionMethod);
static ShardInterval * FindShardInterval(Datum partitionColumnValue,
										 ShardInterval **shardIntervalCache,
										 int shardCount, char partitionMethod,
										 FmgrInfo *compareFunction,
										 FmgrInfo *hashFunction, bool useBinarySearch);
static ShardInterval * SearchCachedShardInterval(Datum partitionColumnValue,
												 ShardInterval** shardIntervalCache,
												 int shardCount,
												 FmgrInfo *compareFunction);
static void OpenShardConnections(CopyStmt *copyStatement,
								 ShardConnections *shardConnections,
								 int64 shardId);
static char * ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId);
static void AppendColumnNames(StringInfo buf, List *columnList);
static void AppendCopyOptions(StringInfo buf, List *copyOptionList);
static void CopyRowToPlacements(StringInfo lineBuf, ShardConnections *shardConnections);
static List * ConnectionList(HTAB *connectionHash);
static void PrepareCopyTransaction(List *connectionList);
static bool EndRemoteCopy(PGconn *connection);
static void AbortCopyTransaction(List *connectionList);
static void CommitCopyTransaction(List *connectionList);


/*
 * CitusCopyFrom implements the COPY table_name FROM ... for hash-partitioned
 * and range-partitioned tables.
 */
void
CitusCopyFrom(CopyStmt *copyStatement, char* completionTag)
{
	RangeVar *relation = copyStatement->relation;
	Oid tableId = RangeVarGetRelid(relation, NoLock, false);
	char *relationName = get_rel_name(tableId);
	List *shardIntervalList = NULL;
	ListCell *shardIntervalCell = NULL;
	char partitionMethod = '\0';
	Var *partitionColumn = NULL;
	HTAB *shardConnectionHash = NULL;
	List *connectionList = NIL;
	MemoryContext tupleContext = NULL;
	CopyState copyState = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	Relation rel = NULL;
	ShardInterval **shardIntervalCache = NULL;
	bool useBinarySearch = false;
	TypeCacheEntry *typeEntry = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;
	int shardCount = 0;
	uint64 processedRowCount = 0;
	ErrorContextCallback errorCallback;

	/* disallow COPY to/from file or program except for superusers */
	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
	}

	/* load the list of shards and verify that we have shards to copy into */
	shardIntervalList = LoadShardIntervalList(tableId);
	if (shardIntervalList == NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards for query"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	partitionMethod = PartitionMethod(tableId);
	if (partitionMethod != DISTRIBUTE_BY_RANGE && partitionMethod != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("COPY is only supported for hash- and range-partitioned tables")));
	}

	partitionColumn = PartitionColumn(tableId, 0);

	/* resolve hash function for partition column */
	typeEntry = lookup_type_cache(partitionColumn->vartype, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);

	/* resolve compare function for shard intervals */
	compareFunction = ShardIntervalCompareFunction(partitionColumn, partitionMethod);

	/* allocate column values and nulls arrays */
	rel = heap_open(tableId, AccessShareLock);
	tupleDescriptor = RelationGetDescr(rel);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/*
	 * We create a new memory context called tuple context, and read and write
	 * each row's values within this memory context. After each read and write,
	 * we reset the memory context. That way, we immediately release memory
	 * allocated for each row, and don't bloat memory usage with large input
	 * files.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "COPY Row Memory Context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/* create a mapping of shard id to a connection for each of its placements */
	shardConnectionHash = CreateShardConnectionHash();

	/* initialize copy state to read from COPY data source */
	copyState = BeginCopyFrom(rel, copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	if (copyState->binary)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Copy in binary mode is not currently supported")));
	}

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareTasksByShardId);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->shardId;

		/* prevent concurrent changes to number of placements */
		LockShardDistributionMetadata(shardId, ShareLock);

		/* prevent concurrent update/delete statements */
		LockShardResource(shardId, ShareLock);

	}

	/* initialize the shard interval cache */
	shardCount = list_length(shardIntervalList);
	shardIntervalCache = SortedShardIntervalArray(shardIntervalList);

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH ||
		!IsUniformHashDistribution(shardIntervalCache, shardCount))
	{
		useBinarySearch = true;
	}

	/* we use a PG_TRY block to roll back on errors (e.g. in NextCopyFrom) */
	PG_TRY();
	{
		while (true)
		{
			bool nextRowFound = false;
			Datum partitionColumnValue = 0;
			ShardInterval *shardInterval = NULL;
			int64 shardId = 0;
			ShardConnections *shardConnections = NULL;
			bool found = false;
			StringInfo lineBuf = NULL;
			MemoryContext oldContext =  NULL;

			oldContext = MemoryContextSwitchTo(tupleContext);

			/* parse a row from the input */
			nextRowFound = NextCopyFrom(copyState, NULL, columnValues, columnNulls, NULL);

			MemoryContextSwitchTo(oldContext);

			if (!nextRowFound)
			{
				MemoryContextReset(tupleContext);
				break;
			}

			CHECK_FOR_INTERRUPTS();

			/* find the partition column value */

			if (columnNulls[partitionColumn->varattno-1])
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg("cannot copy row with NULL value "
									   "in partition column")));
			}

			partitionColumnValue = columnValues[partitionColumn->varattno-1];

			/* find the shard interval and id for the partition column value */
			shardInterval = FindShardInterval(partitionColumnValue, shardIntervalCache,
											  shardCount, partitionMethod,
											  compareFunction, hashFunction,
											  useBinarySearch);
			if (shardInterval == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("no shard for partition column value")));
			}

			shardId = shardInterval->shardId;

			/* find the connections to the shard placements */
			shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
																&shardInterval->shardId,
																HASH_ENTER,
																&found);
			if (!found)
			{
				OpenShardConnections(copyStatement, shardConnections, shardId);
			}

			/* get the (truncated) line buffer */
			lineBuf = &copyState->line_buf;
			lineBuf->data[lineBuf->len++] = '\n';

			/* Replicate row to all shard placements */
			CopyRowToPlacements(lineBuf, shardConnections);

			processedRowCount += 1;

			MemoryContextReset(tupleContext);
		}

		/* prepare two phase commit in replicas */
		connectionList = ConnectionList(shardConnectionHash);
		PrepareCopyTransaction(connectionList);
	}
	PG_CATCH();
	{
		EndCopyFrom(copyState);
		heap_close(rel, AccessShareLock);

		/* roll back all transactions */
		connectionList = ConnectionList(shardConnectionHash);
		AbortCopyTransaction(connectionList);
		PG_RE_THROW();
	}
	PG_END_TRY();

	EndCopyFrom(copyState);
	heap_close(rel, AccessShareLock);

	error_context_stack = errorCallback.previous;

	if (QueryCancelPending)
	{
		AbortCopyTransaction(connectionList);
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED),
						errmsg("canceling statement due to user request")));
	}
	else
	{
		CommitCopyTransaction(connectionList);
	}

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * CreateShardConnectionHash constructs a hash table used for shardId->Connection
 * mapping.
 */
static HTAB *
CreateShardConnectionHash(void)
{
	HTAB *shardConnectionsHash = NULL;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;

	shardConnectionsHash = hash_create("shardConnectionHash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   HASH_ELEM | HASH_FUNCTION);

	return shardConnectionsHash;
}


/*
 * ShardIntervalCompareFunction returns the appropriate compare function for the
 * partition column type. In case of hash-partitioning, it always returns the compare
 * function for integers.
 */
static FmgrInfo *
ShardIntervalCompareFunction(Var *partitionColumn, char partitionMethod)
{
	FmgrInfo *compareFunction = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		compareFunction = GetFunctionInfo(INT4OID, BTREE_AM_OID, BTORDER_PROC);
	}
	else
	{
		compareFunction = GetFunctionInfo(partitionColumn->vartype,
										  BTREE_AM_OID, BTORDER_PROC);
	}

	return compareFunction;
}


/*
 * IsUniformHashDistribution determines whether the given list of sorted shards
 * hash a uniform hash distribution, as produced by master_create_worker_shards.
 */
static bool
IsUniformHashDistribution(ShardInterval **shardIntervalArray, int shardCount)
{
	uint32 hashTokenIncrement = (uint32) (HASH_TOKEN_COUNT / shardCount);
	int shardIndex = 0;

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);

		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		if (DatumGetInt32(shardInterval->minValue) != shardMinHashToken ||
			DatumGetInt32(shardInterval->maxValue) != shardMaxHashToken)
		{
			return false;
		}

		shardIndex += 1;
	}

	return true;
}


/*
 * FindShardInterval finds a single shard interval in the cache for the
 * given partition column value.
 */
static ShardInterval *
FindShardInterval(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
				  int shardCount, char partitionMethod, FmgrInfo *compareFunction,
				  FmgrInfo *hashFunction, bool useBinarySearch)
{
	ShardInterval *shardInterval = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		int hashedValue = DatumGetInt32(FunctionCall1(hashFunction,
													  partitionColumnValue));
		if (useBinarySearch)
		{
			shardInterval = SearchCachedShardInterval(Int32GetDatum(hashedValue),
													  shardIntervalCache, shardCount,
													  compareFunction);
		}
		else
		{
			uint32 hashTokenIncrement = (uint32) (HASH_TOKEN_COUNT / shardCount);
			int shardHashCode = ((uint32) (hashedValue-INT32_MIN)/hashTokenIncrement);

			shardInterval = shardIntervalCache[shardHashCode];
		}
	}
	else
	{
		shardInterval = SearchCachedShardInterval(partitionColumnValue,
												  shardIntervalCache, shardCount,
												  compareFunction);
	}

	return shardInterval;
}


/*
 * SearchCachedShardInterval performs a binary search for a shard interval matching a
 * given partition column value and returns it.
 */
static ShardInterval *
SearchCachedShardInterval(Datum partitionColumnValue, ShardInterval** shardIntervalCache,
						  int shardCount, FmgrInfo *compareFunction)
{
	int lowerBoundIndex = 0;
	int upperBoundIndex = shardCount;

	while (lowerBoundIndex < upperBoundIndex)
	{
		int middleIndex = (lowerBoundIndex + upperBoundIndex) >> 1;
		if (DatumGetInt32(FunctionCall2Coll(compareFunction,
											DEFAULT_COLLATION_OID,
											partitionColumnValue,
											shardIntervalCache[middleIndex]->minValue)) < 0)
		{
			upperBoundIndex = middleIndex;
		}
		else if (DatumGetInt32(FunctionCall2Coll(compareFunction,
												 DEFAULT_COLLATION_OID,
												 partitionColumnValue,
												 shardIntervalCache[middleIndex]->maxValue)) <= 0)
		{
			return shardIntervalCache[middleIndex];

		}
		else
		{
			lowerBoundIndex = middleIndex + 1;
		}
	}

	return NULL;
}


/*
 * Open connections for each placement of a shard. If a connection cannot be opened,
 * the shard placement is marked as inactive and the COPY continues with the
 * remaining shard placements.
 */
static void
OpenShardConnections(CopyStmt *copyStatement, ShardConnections *shardConnections,
					 int64 shardId)
{
	CitusTransactionManager const *transactionManager =
		&CitusTransactionManagerImpl[CopyTransactionManager];

	List *finalizedPlacementList = NIL;
	List *failedPlacementList = NIL;
	ListCell *placementCell = NULL;
	ListCell *failedPlacementCell = NULL;
	List *connectionList = NIL;

	finalizedPlacementList = ShardPlacementList(shardId);

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;

		PGconn *connection = ConnectToNode(nodeName, nodePort);
		if (connection != NULL)
		{
			char *copyCommand = ConstructCopyStatement(copyStatement, shardId);

			/*
			 * New connection: start transaction with copy command on it.
			 * Append shard id to table name.
			 */
			if (transactionManager->Begin(connection) &&
				ExecuteCommand(connection, PGRES_COPY_IN, copyCommand))
			{
				PlacementConnection *placementConnection =
					(PlacementConnection *) palloc0(sizeof(PlacementConnection));

				placementConnection->shardId = shardId;
				placementConnection->prepared = false;
				placementConnection->connection = connection;

				connectionList = lappend(connectionList, placementConnection);
			}
			else
			{
				failedPlacementList = lappend(failedPlacementList, placement);
				ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
								  errmsg("Failed to start '%s' on node %s:%d",
										 copyCommand, nodeName, nodePort)));
			}
		}
		else
		{
			failedPlacementList = lappend(failedPlacementList, placement);
			ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
							  errmsg("Failed to connect to node %s:%d",
									 nodeName, nodePort)));
		}
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
		uint64 shardLength = 0;

		DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
								failedPlacement->nodePort);
		InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
								failedPlacement->nodeName, failedPlacement->nodePort);
	}

	shardConnections->shardId = shardId;
	shardConnections->connectionList = connectionList;

}


/*
 * ConstructCopyStattement constructs the text of a COPY statement for a particular
 * shard.
 */
static char *
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId)
{
	StringInfo buf = makeStringInfo();
	char *qualifiedName = NULL;

	qualifiedName = quote_qualified_identifier(copyStatement->relation->schemaname,
											   copyStatement->relation->relname);

	appendStringInfo(buf, "COPY %s_%ld ", qualifiedName, (long) shardId);

	if (copyStatement->attlist != NIL)
	{
		AppendColumnNames(buf, copyStatement->attlist);
	}

	appendStringInfoString(buf, "FROM STDIN");

	if (copyStatement->options)
	{
		appendStringInfoString(buf, " WITH ");

		AppendCopyOptions(buf, copyStatement->options);
	}

	return buf->data;
}


/*
 * AppendCopyOptions deparses a list of CopyStmt options and appends them to buf.
 */
static void
AppendCopyOptions(StringInfo buf, List *copyOptionList)
{
	ListCell *optionCell = NULL;
	char separator = '(';

	foreach(optionCell, copyOptionList)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);

		if (strcmp(defel->defname, "header") == 0 && defGetBoolean(defel))
		{
			/* worker should not skip header again */
			continue;
		}

		appendStringInfo(buf, "%c%s ", separator, defel->defname);

		if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (!defel->arg)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname)));
			}
			else if (IsA(defel->arg, A_Star))
			{
				appendStringInfoString(buf, "*");
			}
			else
			{
				AppendColumnNames(buf, (List *) defel->arg);
			}
		}
		else if (strcmp(defel->defname, "force_not_null") == 0 ||
				 strcmp(defel->defname, "force_null") == 0)
		{
			if (!defel->arg)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname)));
			}
			else
			{
				AppendColumnNames(buf, (List *) defel->arg);
			}
		}
		else
		{
			appendStringInfoString(buf, defGetString(defel));
		}

		separator = ',';
	}

	appendStringInfoChar(buf, ')');
}


/*
 * AppendColumnList deparses a list of column names into a StringInfo.
 */
static void
AppendColumnNames(StringInfo buf, List *columnList)
{
	ListCell *attributeCell = NULL;
	char separator = '(';

	foreach(attributeCell, columnList)
	{
		char *columnName = strVal(lfirst(attributeCell));
		appendStringInfo(buf, "%c%s", separator, quote_identifier(columnName));
		separator = ',';
	}

	appendStringInfoChar(buf, ')');
}


/*
 * CopyRowToPlacements copies a row to a list of placements for a shard.
 */
static void
CopyRowToPlacements(StringInfo lineBuf, ShardConnections *shardConnections)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, shardConnections->connectionList)
	{
		PlacementConnection *placementConnection =
			(PlacementConnection *) lfirst(connectionCell);
		PGconn *connection = placementConnection->connection;
		int64 shardId = shardConnections->shardId;

		/* copy the line buffer into the placement */
		int copyResult = PQputCopyData(connection, lineBuf->data, lineBuf->len);
		if (copyResult != 1)
		{
			char *nodeName = ConnectionGetOptionValue(connection, "host");
			char *nodePort = ConnectionGetOptionValue(connection, "port");
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							  errmsg("COPY to shard %ld on %s:%s failed",
									 (long) shardId, nodeName, nodePort)));
		}
	}
}


/*
 * ConnectionList flatten the connection hash to a list of placement connections.
 */
static List *
ConnectionList(HTAB *connectionHash)
{
	List *connectionList = NIL;
	HASH_SEQ_STATUS status;
	ShardConnections* shardConnections = NULL;

	hash_seq_init(&status, connectionHash);
	while ((shardConnections = (ShardConnections *) hash_seq_search(&status)) != NULL)
	{
		ListCell *connectionCell = NULL;
		foreach(connectionCell, shardConnections->connectionList)
		{
			PlacementConnection *placementConnection =
				(PlacementConnection *) lfirst(connectionCell);

			connectionList = lappend(connectionList, placementConnection);
		}
	}

	return connectionList;
}


/*
 * End copy and prepare transaction.
 * This function is applied for each shard placement unless some error happen.
 * Status of this function is stored in ShardConnections::status field
 */
static void
PrepareCopyTransaction(List *connectionList)
{
	CitusTransactionManager const *transactionManager =
		&CitusTransactionManagerImpl[CopyTransactionManager];

	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		PlacementConnection *placementConnection =
			(PlacementConnection *) lfirst(connectionCell);
		PGconn *connection = placementConnection->connection;
		int64 shardId = placementConnection->shardId;

		if (EndRemoteCopy(connection) &&
			transactionManager->Prepare(connection, BuildTransactionId(shardId)))
		{
			placementConnection->prepared = true;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("Failed to prepare transaction for shard %ld",
								   (long) shardId)));
		}
	}
}


/*
 * EndRemoteCopy sends PQputCopyEnd command to the client and checks the result.
 */
static bool
EndRemoteCopy(PGconn *connection)
{
	PGresult *result = NULL;

	int copyEndResult = PQputCopyEnd(connection, NULL);
	if (copyEndResult != 1)
	{
		return false;
	}

	while ((result = PQgetResult(connection)) != NULL)
	{
		int resultStatus = PQresultStatus(result);
		if (resultStatus != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			return false;
		}
		PQclear(result);
	}

	return true;
}


/*
 * AbortCopyTransaction aborts a two-phase commit. It attempts to roll back
 * all transactions even if some of them fail, in which case a warning is given
 * for each of them.
 */
static void
AbortCopyTransaction(List *connectionList)
{
	CitusTransactionManager const *transactionManager =
		&CitusTransactionManagerImpl[CopyTransactionManager];

	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		PlacementConnection *placementConnection =
			(PlacementConnection *) lfirst(connectionCell);
		PGconn *connection = placementConnection->connection;
		int64 shardId = placementConnection->shardId;
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");

		if (placementConnection->prepared)
		{
			char *transactionId = BuildTransactionId(shardId);

			if (!transactionManager->RollbackPrepared(connection, transactionId))
			{
				ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
								  errmsg("Failed to roll back transaction '%s' on %s:%s",
										 transactionId, nodeName, nodePort)));
			}
		}
		else if (!EndRemoteCopy(connection) &&
				 !transactionManager->Rollback(connection))
		{
			ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
							  errmsg("Failed to COPY to shard %ld on %s:%s",
									 shardId, nodeName, nodePort)));
		}

		PQfinish(connection);
	}
}


/*
 * CommitCopyTransaction commits a two-phase commit. It attempts to commit all
 * transactionsm even if some of them fail, in which case a warning is given
 * for each of them.
 */
static void
CommitCopyTransaction(List *connectionList)
{
	CitusTransactionManager const *transactionManager =
		&CitusTransactionManagerImpl[CopyTransactionManager];

	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		PlacementConnection *placementConnection =
			(PlacementConnection *) lfirst(connectionCell);
		PGconn *connection = placementConnection->connection;
		int64 shardId = placementConnection->shardId;
		char *transactionId = BuildTransactionId(shardId);

		Assert(placementConnection->prepared);

		if (!transactionManager->CommitPrepared(connection, transactionId))
		{
			char *nodeName = ConnectionGetOptionValue(connection, "host");
			char *nodePort = ConnectionGetOptionValue(connection, "port");
			ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
							  errmsg("Failed to commit transaction '%s' on %s:%s",
									 transactionId, nodeName, nodePort)));
		}

		PQfinish(connection);
	}
}

