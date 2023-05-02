/*-------------------------------------------------------------------------
 *
 * ddl_replication.h
 *	  definition of DDL replication functions
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDL_REPLICATION_H
#define DDL_REPLICATION_H


/* citus.enable_database_shard_move_ddl_replication setting */
extern bool EnableDDLReplicationInDatabaseShardMove;


bool ShouldReplicateDDLCommand(Node *parsetree);
void ReplicateDDLCommand(Node *parsetree, const char *ddlCommand, char *searchPath);


#endif
