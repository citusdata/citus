/*-------------------------------------------------------------------------
 *
 * repartition.h
 *	  Execution logic for repartition queries.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef REPARTITION_H
#define REPARTITION_H

#include "nodes/pg_list.h"

extern void ExecuteDependentTasks(List *taskList, Job *topLevelJob);
extern void CleanUpSchemas(void);


#endif /* REPARTITION_H */
