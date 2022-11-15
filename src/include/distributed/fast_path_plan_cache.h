#ifndef FAST_PATH_PLAN_CACHE
#define FAST_PATH_PLAN_CACHE

extern bool IsFastPathPlanCachingSupported(Job *currentJob,
										   DistributedPlan *originalDistributedPlan);
extern PlannedStmt * GetFastPathLocalPlan(Task *task, DistributedPlan *distributedPlan);
extern void CacheFastPathPlanForShardQuery(Task *task,
										   DistributedPlan *originalDistributedPlan,
										   ParamListInfo paramListInfo);

#endif /* FAST_PATH_PLAN_CACHE */
