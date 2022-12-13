
#ifndef CITUS_SHARDGROUP_H
#define CITUS_SHARDGROUP_H

#include "postgres.h"

#include "distributed/listutils.h"

extern List * ShardgroupForColocationId(uint32 colocationId);

#endif /*CITUS_SHARDGROUP_H */
