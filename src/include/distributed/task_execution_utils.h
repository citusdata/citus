#ifndef TASK_EXECUTION_UTILS_H
#define TASK_EXECUTION_UTILS_H

extern List * CreateTaskListForJobTree(List *jobTaskList);
extern List * TaskListForDistributedDataDump(List *taskList);
extern List * TaskPlacementListForDistributedDataDump(List *taskPlacementList);

#endif /* TASK_EXECUTION_UTILS_H */
