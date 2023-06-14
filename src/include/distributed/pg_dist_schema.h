/*-------------------------------------------------------------------------
 *
 * pg_dist_schema.h
 *	  definition of the system catalog for the schemas used for schema-based
 *	  sharding in Citus.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_SCHEMA_H
#define PG_DIST_SCHEMA_H

#include "postgres.h"


/* ----------------
 *		pg_dist_schema definition.
 * ----------------
 */
typedef struct FormData_pg_dist_schema
{
	Oid schemaid;
	uint32 colocationid;
} FormData_pg_dist_schema;

/* ----------------
 *      Form_pg_dist_schema corresponds to a pointer to a tuple with
 *      the format of pg_dist_schema relation.
 * ----------------
 */
typedef FormData_pg_dist_schema *Form_pg_dist_schema;

/* ----------------
 *      compiler constants for pg_dist_schema
 * ----------------
 */
#define Natts_pg_dist_schema 2
#define Anum_pg_dist_schema_schemaid 1
#define Anum_pg_dist_schema_colocationid 2

#endif /* PG_DIST_SCHEMA_H */
