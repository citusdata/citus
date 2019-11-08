

#ifndef ADAPTIVE_EXECUTOR_REPARTITIONING_H
#define ADAPTIVE_EXECUTOR_REPARTITIONING_H

#include "nodes/pg_list.h"

extern void ExecuteDependedTasks(List *taskList);
extern void CleanUpSchemas();


#endif /* ADAPTIVE_EXECUTOR_REPARTITIONING_H */
