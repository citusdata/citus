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
 *      compiler constants for pg_dist_node
 * ----------------
 *
 *  n.b. master_add_node, master_add_inactive_node, and master_activate_node all
 *  directly return pg_dist_node tuples. This means their definitions (and
 *  in particular their OUT parameters) must be changed whenever the definition of
 *  pg_dist_node changes.
 */
#define Natts_pg_dist_node 10
#define Anum_pg_dist_node_nodeid 1
#define Anum_pg_dist_node_groupid 2
#define Anum_pg_dist_node_nodename 3
#define Anum_pg_dist_node_nodeport 4
#define Anum_pg_dist_node_noderack 5
#define Anum_pg_dist_node_hasmetadata 6
#define Anum_pg_dist_node_isactive 7
#define Anum_pg_dist_node_noderole 8
#define Anum_pg_dist_node_nodecluster 9
#define Anum_pg_dist_node_shouldhavedata 10

#define GROUPID_SEQUENCE_NAME "pg_dist_groupid_seq"
#define NODEID_SEQUENCE_NAME "pg_dist_node_nodeid_seq"

#endif /* PG_DIST_NODE_H */
