#ifndef LOCAL_PLAN_CACHE
#define LOCAL_PLAN_CACHE

extern LocalPlannedStatement * GetCachedLocalPlan(Task *task,
												  DistributedPlan *distributedPlan);
extern LocalPlannedStatement * CacheLocalPlanForShardQuery(Job *currentJob,
														   DistributedPlan *
														   originalDistributedPlan,
														   ParamListInfo paramListInfo,
														   bool *planAddedToCached);

#endif /* LOCAL_PLAN_CACHE */
