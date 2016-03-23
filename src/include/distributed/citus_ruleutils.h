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

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


/* Function declarations for version independent Citus ruleutils wrapper functions */
extern char * pg_get_extensiondef_string(Oid tableRelationId);
extern char * pg_get_serverdef_string(Oid tableRelationId);
extern char * pg_get_tableschemadef_string(Oid tableRelationId);
extern char * pg_get_tablecolumnoptionsdef_string(Oid tableRelationId);
extern char * pg_get_indexclusterdef_string(Oid indexRelationId);

/* Function declarations for version dependent PostgreSQL ruleutils functions */
extern void pg_get_query_def(Query *query, StringInfo buffer);
extern void deparse_shard_query(Query *query, Oid distrelid, int64 shardid, StringInfo
								buffer);
extern char * generate_relation_name(Oid relid, List *namespaces);


#endif /* CITUS_RULEUTILS_H */
