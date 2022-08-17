/*-------------------------------------------------------------------------
 *
 * type_utils.h
 *	  Utility functions related to types.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef TYPE_UTILS_H
#define TYPE_UTILS_H

typedef struct ClusterClock
{
	uint64 logical;     /* cluster clock logical timestamp at the commit */
	uint32 counter;     /* cluster clock counter value at the commit */
} ClusterClock;


extern ClusterClock * ParseClusterClockPGresult(PGresult *result,
												int rowIndex, int colIndex);
extern int cluster_clock_cmp_internal(ClusterClock *clusterClock1,
									  ClusterClock *clusterClock2);

#endif /* TYPE_UTILS_H */
