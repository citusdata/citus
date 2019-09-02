/*-------------------------------------------------------------------------
 *
 * pg_dist_transaction.h
 *	  definition of the "transaction" relation (pg_dist_transaction).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_TRANSACTION_H
#define PG_DIST_TRANSACTION_H


/* ----------------
 *		pg_dist_transaction definition.
 * ----------------
 */
typedef struct FormData_pg_dist_transaction
{
	int32 groupid;             /* id of the replication group */
	text gid;                  /* global transaction identifier */
} FormData_pg_dist_transaction;


/* ----------------
 *      Form_pg_dist_transactions corresponds to a pointer to a tuple with
 *      the format of pg_dist_transactions relation.
 * ----------------
 */
typedef FormData_pg_dist_transaction *Form_pg_dist_transaction;


/* ----------------
 *      compiler constants for pg_dist_transaction
 * ----------------
 */
#define Natts_pg_dist_transaction 2
#define Anum_pg_dist_transaction_groupid 1
#define Anum_pg_dist_transaction_gid 2


#endif   /* PG_DIST_TRANSACTION_H */
