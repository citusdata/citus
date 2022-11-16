#ifndef FAST_PATH_PLAN_CACHE
#define FAST_PATH_PLAN_CACHE

extern bool IsFastPathPlanCachingSupported(Job *currentJob,
										   DistributedPlan *originalDistributedPlan);
extern PlannedStmt * GetCachedFastPathLocalPlan(Task *task,
												DistributedPlan *distributedPlan);
extern FastPathPlanCache * CacheFastPathPlanForShardQuery(Task *task, Job *evaluatedJob,
														  DistributedPlan *
														  originalDistributedPlan,
														  ParamListInfo paramListInfo);

#endif /* FAST_PATH_PLAN_CACHE */
