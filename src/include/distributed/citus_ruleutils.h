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

#include "distributed/coordinator_protocol.h"

/* Function declarations for version independent Citus ruleutils wrapper functions */
extern char * pg_get_extensiondef_string(Oid tableRelationId);
extern Oid get_extension_schema(Oid ext_oid);
extern char * get_extension_version(Oid extensionId);
extern char * pg_get_serverdef_string(Oid tableRelationId);
extern char * pg_get_sequencedef_string(Oid sequenceRelid);
extern Form_pg_sequence pg_get_sequencedef(Oid sequenceRelationId);
extern char * pg_get_tableschemadef_string(Oid tableRelationId,
										   IncludeSequenceDefaults includeSequenceDefaults,
										   IncludeIdentities includeIdentityDefaults,
										   char *accessMethod);
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
extern List * pg_get_row_level_security_commands(Oid relationId);
extern const char * RoleSpecString(RoleSpec *spec, bool withQuoteIdentifier);
extern char * flatten_reloptions(Oid relid);

/* Function declarations for version dependent PostgreSQL ruleutils functions */
extern void pg_get_query_def(Query *query, StringInfo buffer);
bool get_merged_argument_list(CallStmt *stmt, List **mergedNamedArgList,
							  Oid **mergedNamedArgTypes, List **mergedArgumentList,
							  int *totalArguments);
char * pg_get_rule_expr(Node *expression);
extern void deparse_shard_query(Query *query, Oid distrelid, int64 shardid,
								StringInfo buffer);
extern char * generate_relation_name(Oid relid, List *namespaces);
extern char * generate_qualified_relation_name(Oid relid);
extern char * generate_operator_name(Oid operid, Oid arg1, Oid arg2);
extern List * getOwnedSequences_internal(Oid relid, AttrNumber attnum, char deptype);
extern void AppendOptionListToString(StringInfo stringData, List *options);


#endif /* CITUS_RULEUTILS_H */
