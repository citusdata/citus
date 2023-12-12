/*-------------------------------------------------------------------------
 *
 * pg_dist_database.h
 *	  definition of the relation that holds distributed database information
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PG_DIST_DATABASE_H
#define CITUS_PG_DIST_DATABASE_H

#include "catalog/pg_namespace_d.h"
#include "utils/lsyscache.h"

typedef struct FormData_pg_dist_database
{
	Oid databaseid;
	int groupid;
} FormData_pg_dist_database;

#define Anum_pg_dist_database_databaseid 1
#define Anum_pg_dist_database_groupid 2

/* ----------------
 *      Form_pg_dist_database corresponds to a pointer to a tuple with
 *      the format of pg_dist_database relation.
 * ----------------
 */
typedef FormData_pg_dist_database *Form_pg_dist_database;


#define PgDistDatabaseRelationId() (get_relname_relid("pg_dist_database", \
													  PG_CATALOG_NAMESPACE))
#endif /* CITUS_PG_DIST_DATABASE_H */
