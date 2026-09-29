#ifndef CLONENODE_UTILS_H
#define CLONENODE_UTILS_H

#include "distributed/metadata_cache.h"

extern void EnsureValidStreamingReplica(WorkerNode *primaryWorkerNode, char *
										replicaHostname, int replicaPort);
extern void EnsureValidCloneMode(WorkerNode *primaryWorkerNode, char *cloneHostname, int
								 clonePort, char *operation);

#endif /* CLONE_UTILS_H */
