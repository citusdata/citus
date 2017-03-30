/*-------------------------------------------------------------------------
 *
 * citus_ruleutils.h
 *	  Citus ruleutils wrapper functions and exported PostgreSQL ruleutils
 *	  functions.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_RULEUTILS_H
#define CITUS_RULEUTILS_H

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"

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
extern char * pg_get_tablecolumnoptionsdef_string(Oid tableRelationId);
extern void deparse_shard_index_statement(IndexStmt *origStmt, Oid distrelid,
										  int64 shardid, StringInfo buffer);
extern char * pg_get_indexclusterdef_string(Oid indexRelationId);
extern List * pg_get_table_grants(Oid relationId);

/* Function declarations for version dependent PostgreSQL ruleutils functions */
extern void pg_get_query_def(Query *query, StringInfo buffer);
extern void deparse_shard_query(Query *query, Oid distrelid, int64 shardid,
								StringInfo buffer);
extern char * generate_relation_name(Oid relid, List *namespaces);
extern char * generate_qualified_relation_name(Oid relid);


#endif /* CITUS_RULEUTILS_H */
