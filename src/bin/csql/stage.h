/*
 * csql - the Citus interactive terminal
 * stage.h
 *	  Declarations for the csql meta-command \stage. These declarations define a
 *	  protocol for the client to communicate to the master and worker nodes.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 */

#ifndef STAGE_H
#define STAGE_H

#include "postgres_fe.h"


/* Server returned values never equal to zero. */
#define INVALID_UINT64 0
#define MAX_SHARD_UPLOADS 256
#define SHARD_NAME_SEPARATOR '_'

/* Connection parameters set to enable connect timeouts in seconds. */
#define CONN_INFO_TEMPLATE "dbname=%s connect_timeout=%u"
#define CLIENT_CONNECT_TIMEOUT 20

/* Transaction related commands used in talking to the master and primary. */
#define BEGIN_COMMAND "BEGIN"
#define COMMIT_COMMAND "COMMIT"
#define ROLLBACK_COMMAND "ROLLBACK"

/* Names of remote function calls to execute on the master. */
#define MASTER_GET_TABLE_METADATA "SELECT * FROM master_get_table_metadata($1::text)"
#define MASTER_GET_TABLE_DDL_EVENTS "SELECT * FROM master_get_table_ddl_events($1::text)"
#define MASTER_GET_NEW_SHARDID "SELECT * FROM master_get_new_shardid()"
#define MASTER_GET_LOCAL_FIRST_CANDIDATE_NODES \
	"SELECT * FROM master_get_local_first_candidate_nodes()"
#define MASTER_GET_ROUND_ROBIN_CANDIDATE_NODES \
	"SELECT * FROM  master_get_round_robin_candidate_nodes($1::int8)"

#define MASTER_INSERT_SHARD_ROW \
	"INSERT INTO pg_dist_shard  " \
	"(logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue) VALUES  " \
	"($1::oid, $2::int8, $3::char, $4::text, $5::text)"
#define MASTER_INSERT_PLACEMENT_ROW \
	"INSERT INTO pg_dist_shard_placement  " \
	"(shardid, shardstate, shardlength, nodename, nodeport) VALUES  " \
	"($1::int8, $2::int4, $3::int8, $4::text, $5::int4)"

/* Column names used to identify response fields as returned from the master. */
#define LOGICAL_RELID_FIELD "logical_relid"
#define PART_STORAGE_TYPE_FIELD "part_storage_type"
#define PART_METHOD_FIELD "part_method"
#define PART_KEY_FIELD "part_key"
#define PART_REPLICA_COUNT_FIELD "part_replica_count"
#define PART_MAX_SIZE_FIELD "part_max_size"
#define PART_PLACEMENT_POLICY_FIELD "part_placement_policy"
#define NODE_NAME_FIELD "node_name"
#define NODE_PORT_FIELD "node_port"

/* the tablename in the overloaded COPY statement is the to-be-transferred file */
#define TRANSMIT_REGULAR_COMMAND "COPY \"%s\" FROM STDIN WITH (format 'transmit')"
#define SHARD_MIN_MAX_COMMAND "SELECT min(%s), max(%s) FROM %s"
#define SHARD_TABLE_SIZE_COMMAND "SELECT pg_table_size('%s')"
#define SET_FOREIGN_TABLE_FILENAME "ALTER FOREIGN TABLE %s OPTIONS (SET filename '%s')"
#define GET_COLUMNAR_TABLE_FILENAME_OPTION \
	"SELECT * FROM (SELECT (pg_options_to_table(ftoptions)).* FROM pg_foreign_table " \
	"WHERE ftrelid = %u) AS Q WHERE option_name = 'filename';"
#define APPLY_SHARD_DDL_COMMAND \
	"SELECT * FROM worker_apply_shard_ddl_command ($1::int8, $2::text)"
#define REMOTE_FILE_SIZE_COMMAND "SELECT size FROM pg_stat_file('%s')"
#define SHARD_COLUMNAR_TABLE_SIZE_COMMAND "SELECT cstore_table_size('%s')"

/* Types that define table storage type and shard state. */
#define STORAGE_TYPE_TABLE 't'
#define STORAGE_TYPE_FOREIGN 'f'
#define STORAGE_TYPE_COLUMNAR 'c'
#define FILE_FINALIZED "1"

/* Shard placement policy types. */
#define SHARD_PLACEMENT_LOCAL_NODE_FIRST 1
#define SHARD_PLACEMENT_ROUND_ROBIN 2

/* Directory to put foreign table files on secondary nodes. */
#define FOREIGN_CACHED_DIR "pg_foreign_file/cached"


/*
 * TableMetadata keeps table related metadata to which the user requested to
 * stage data. These metadata are retrieved from the master as read-only; and
 * are cached and reused as new shards are created.
 */
typedef struct TableMetadata
{
	uint32 logicalRelid;         /* table's relationId on the master */
	char tableStorageType;       /* relay file, foreign table, or table */
	char partitionMethod;        /* table's partition method */
	char *partitionKey;          /* partition key expression */
	uint32 shardReplicaCount;    /* shard replication factor */
	uint64 shardMaxSize;         /* create new shard when shard reaches max size */
	uint32 shardPlacementPolicy; /* policy to use when choosing nodes to place shards */

	char **ddlEventList;  /* DDL statements used for creating new shard */
	uint32 ddlEventCount; /* DDL statement count; statement list size */
} TableMetadata;


/*
 * ShardMetadata keeps metadata related to one shard for which we are currently
 * staging data. Some parts of these metadata are retrieved from the master as
 * read-only (shardId, node names and port numbers); and other parts are updated
 * by the client as we stage data to clients (shard size and stage statuses).
 */
typedef struct ShardMetadata
{
	uint64 shardId;           /* global shardId; created on the master node */

	char **nodeNameList;   /* candidate node name list for shard uploading */
	uint32 *nodePortList;  /* candidate node port list for shard uploading */
	uint32 nodeCount;      /* candidate node count; node list size */
	bool *nodeStageList;   /* shard uploaded to corresponding candidate node? */

	char *shardMinValue;   /* partition key's minimum value in shard */
	char *shardMaxValue;   /* partition key's maximum value in shard */
	uint64 shardSize;      /* shard size; updated during staging */
} ShardMetadata;


/* Function declaration for staging data to remote nodes */
bool DoStageData(const char *stageCommand);


#endif   /* STAGE_H */
