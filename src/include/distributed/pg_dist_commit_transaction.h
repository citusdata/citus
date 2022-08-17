/*-------------------------------------------------------------------------
 *
 * pg_dist_commit_transaction.h
 *	  definition of the "commit-transaction" relation (pg_dist_commit_transaction).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_COMMIT_TRANSACTION_H
#define PG_DIST_COMMIT_TRANSACTION_H

typedef struct cluster_clock
{
	uint64 clockLogical;     /* cluster clock logical timestamp at the commit */
	uint32 clockCounter;     /* cluster clock counter value at the commit */
} cluster_clock;


/* ----------------
 *		pg_dist_commit_transaction definition.
 * ----------------
 */
typedef struct FormData_pg_dist_commit_transaction
{
	text transaction_id;               /* id of the current transaction committed */
	cluster_clock cluster_clock_value; /* logical clock timestamp */
	uint64 timestamp;                  /* epoch timestamp in milliseconds */
} FormData_pg_dist_commit_transaction;


/* ----------------
 *      Form_pg_dist_commit_transactions corresponds to a pointer to a tuple with
 *      the format of pg_dist_commit_transactions relation.
 * ----------------
 */
typedef FormData_pg_dist_commit_transaction *Form_pg_dist_commit_transaction;


/* ----------------
 *      compiler constants for pg_dist_commit_transaction
 * ----------------
 */
#define Natts_pg_dist_commit_transaction 3
#define Anum_pg_dist_commit_transaction_transaction_id 1
#define Anum_pg_dist_commit_transaction_cluster_clock 2
#define Anum_pg_dist_commit_transaction_timestamp 3

#endif   /* PG_DIST_COMMIT_TRANSACTION_H */
