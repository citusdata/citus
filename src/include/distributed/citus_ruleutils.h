/*-------------------------------------------------------------------------
 *
 * citus_ruleutils.h
 *	  Citus ruleutils wrapper functions and exported PostgreSQL ruleutils
 *	  functions.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_RULEUTILS_H
#define CITUS_RULEUTILS_H

#include "postgres.h" /* IWYU pragma: keep */

#include "catalog/pg_sequence.h"
#include "commands/sequence.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


#define CREATE_SEQUENCE_COMMAND \
	"CREATE SEQUENCE IF NOT EXISTS %s INCREMENT BY " INT64_FORMAT " MINVALUE " \
	INT64_FORMAT " MAXVALUE " INT64_FORMAT " START WITH " INT64_FORMAT " %sCYCLE"

/* Function declarations for version independent Citus ruleutils wrapper functions */
extern char * pg_get_extensiondef_string(Oid tableRelationId);
extern Oid get_extension_schema(Oid ext_oid);
extern char * pg_get_serverdef_string(Oid tableRelationId);
extern char * pg_get_sequencedef_string(Oid sequenceRelid);
extern Form_pg_sequence pg_get_sequencedef(Oid sequenceRelationId);
extern char * pg_get_tableschemadef_string(Oid tableRelationId, bool forShardCreation);
extern void EnsureRelationKindSupported(Oid relationId);
extern char * pg_get_tablecolumnoptionsdef_string(Oid tableRelationId);
extern void deparse_shard_index_statement(IndexStmt *origStmt, Oid distrelid,
										  int64 shardid, StringInfo buffer);
extern void deparse_shard_reindex_statement(ReindexStmt *origStmt, Oid distrelid,
											int64 shardid, StringInfo buffer);
extern char * pg_get_indexclusterdef_string(Oid indexRelationId);
extern List * pg_get_table_grants(Oid relationId);
extern bool contain_nextval_expression_walker(Node *node, void *context);
extern char * pg_get_replica_identity_command(Oid tableRelationId);
extern const char * RoleSpecString(RoleSpec *spec);

/* Function declarations for version dependent PostgreSQL ruleutils functions */
extern void pg_get_query_def(Query *query, StringInfo buffer);
char * pg_get_rule_expr(Node *expression);
extern void deparse_shard_query(Query *query, Oid distrelid, int64 shardid,
								StringInfo buffer);
extern char * generate_relation_name(Oid relid, List *namespaces);
extern char * generate_qualified_relation_name(Oid relid);
extern char * generate_operator_name(Oid operid, Oid arg1, Oid arg2);


#endif /* CITUS_RULEUTILS_H */
