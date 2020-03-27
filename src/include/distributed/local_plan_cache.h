#ifndef LOCAL_PLAN_CACHE
#define LOCAL_PLAN_CACHE

extern bool IsLocalPlanCachingSupported(Job *currentJob,
										DistributedPlan *originalDistributedPlan);
extern PlannedStmt * GetCachedLocalPlan(Task *task, DistributedPlan *distributedPlan);
extern void CacheLocalPlanForShardQuery(Task *task,
										DistributedPlan *originalDistributedPlan);

#endif /* LOCAL_PLAN_CACHE */
