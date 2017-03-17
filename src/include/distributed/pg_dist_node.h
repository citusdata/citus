/*-------------------------------------------------------------------------
 *
 * pg_dist_node.h
 *	  definition of the relation that holds the nodes on the cluster (pg_dist_node).
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_NODE_H
#define PG_DIST_NODE_H

/* ----------------
 *		pg_dist_node definition.
 * ----------------
 */
typedef struct FormData_pg_dist_node
{
	int nodeid;
	int groupid;
#ifdef CATALOG_VARLEN
	text nodename;
	int nodeport;
	bool hasmetadata;
	bool isactive
#endif
} FormData_pg_dist_node;

/* ----------------
 *      Form_pg_dist_partitions corresponds to a pointer to a tuple with
 *      the format of pg_dist_partitions relation.
 * ----------------
 */
typedef FormData_pg_dist_node *Form_pg_dist_node;

/* ----------------
 *      compiler constants for pg_dist_node
 * ----------------
 */
#define Natts_pg_dist_node 7
#define Anum_pg_dist_node_nodeid 1
#define Anum_pg_dist_node_groupid 2
#define Anum_pg_dist_node_nodename 3
#define Anum_pg_dist_node_nodeport 4
#define Anum_pg_dist_node_noderack 5
#define Anum_pg_dist_node_hasmetadata 6
#define Anum_pg_dist_node_isactive 7

#define GROUPID_SEQUENCE_NAME "pg_dist_groupid_seq"
#define NODEID_SEQUENCE_NAME "pg_dist_node_nodeid_seq"

#endif /* PG_DIST_NODE_H */
