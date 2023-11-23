/*-------------------------------------------------------------------------
 *
 * worker_protocol.h
 *	  Header for shared declarations that are used for caching remote resources
 *	  on worker nodes, and also for applying distributed execution primitives.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_PROTOCOL_H
#define WORKER_PROTOCOL_H

#include "postgres.h"

#include "fmgr.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/fd.h"
#include "utils/array.h"

#include "distributed/shardinterval_utils.h"
#include "distributed/version_compat.h"


/* Number of rows to prefetch when reading data with a cursor */
#define ROW_PREFETCH_COUNT 50

/* Defines that relate to creating tables */
#define GET_TABLE_DDL_EVENTS "SELECT master_get_table_ddl_events('%s')"
#define SET_SEARCH_PATH_COMMAND "SET search_path TO %s"
#define CREATE_TABLE_COMMAND "CREATE TABLE %s (%s)"
#define CREATE_TABLE_AS_COMMAND "CREATE TABLE %s (%s) AS (%s)"


/* Function declarations local to the worker module */
extern uint64 ExtractShardIdFromTableName(const char *tableName, bool missingOk);
extern void SetDefElemArg(AlterSeqStmt *statement, const char *name, Node *arg);


/* Function declarations shared with the master planner */
extern DestReceiver * CreateFileDestReceiver(char *filePath,
											 MemoryContext tupleContext,
											 bool binaryCopyFormat);
extern void FileDestReceiverStats(DestReceiver *dest,
								  uint64 *rowsSent,
								  uint64 *bytesSent);

/* Function declaration for parsing tree node */
extern Node * ParseTreeNode(const char *ddlCommand);
extern Node * ParseTreeRawStmt(const char *ddlCommand);

/* Function declarations for applying distributed execution primitives */
extern Datum worker_apply_shard_ddl_command(PG_FUNCTION_ARGS);

/* Function declarations for fetching regular and foreign tables */
extern Datum worker_append_table_to_shard(PG_FUNCTION_ARGS);

/* Function declaration for calculating hashed value */
extern Datum worker_hash(PG_FUNCTION_ARGS);

/* Function declaration for calculating nextval() in worker */
extern Datum worker_nextval(PG_FUNCTION_ARGS);

#endif   /* WORKER_PROTOCOL_H */
