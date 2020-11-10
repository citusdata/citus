/*-------------------------------------------------------------------------
 *
 * pg_cimv.h
 *	  TODO
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CIMV_H
#define PG_CIMV_H

/* ----------------
 *		pg_cimv definition.
 * ----------------
 */
typedef struct FormData_pg_cimv
{
	Oid userview;
	Oid basetable;
	Oid mattable;
	Oid refreshview;
	NameData triggerfnnamespace;
	NameData triggerfnname;
	Oid landingtable;
	int64 jobid;
} FormData_pg_cimv;

/* ----------------
 *      FormData_pg_cimv corresponds to a pointer to a tuple with
 *      the format of pg_cimv relation.
 * ----------------
 */
typedef FormData_pg_cimv *Form_pg_cimv;

/* ----------------
 *      compiler constants for pg_cimv
 * ----------------
 */
#define Natts_pg_cimv 8
#define Anum_pg_cimv_userview 1
#define Anum_pg_cimv_basetable 2
#define Anum_pg_cimv_mattable 3
#define Anum_pg_cimv_refreshview 4
#define Anum_pg_cimv_triggernamespace 5
#define Anum_pg_cimv_triggername 6
#define Anum_pg_cimv_landingtable 7
#define Anum_pg_cimv_jobid 8

#endif   /* PG_CIMV_H */
