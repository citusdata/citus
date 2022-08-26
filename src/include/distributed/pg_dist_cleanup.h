/*-------------------------------------------------------------------------
 *
 * pg_dist_cleanup.h
 *	  definition of the relation that holds the resources to be cleaned up
 *	  in cluster (pg_dist_cleanup).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_CLEANUP_H
#define PG_DIST_CLEANUP_H

/* ----------------
 *      compiler constants for pg_dist_cleanup
 * ----------------
 */

#define Natts_pg_dist_cleanup 6
#define Anum_pg_dist_cleanup_record_id 1
#define Anum_pg_dist_cleanup_operation_id 2
#define Anum_pg_dist_cleanup_object_type 3
#define Anum_pg_dist_cleanup_object_name 4
#define Anum_pg_dist_cleanup_node_group_id 5
#define Anum_pg_dist_cleanup_policy_type 6

#define PG_CATALOG "pg_catalog"
#define PG_DIST_CLEANUP "pg_dist_cleanup"
#define OPERATIONID_SEQUENCE_NAME "pg_dist_operationid_seq"
#define CLEANUPRECORDID_SEQUENCE_NAME "pg_dist_cleanup_recordid_seq"

#endif /* PG_DIST_CLEANUP_H */
