/*-------------------------------------------------------------------------
 *
 * executor_util.h
 *	  Utility functions for executing task lists.
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECUTOR_UTIL_H
#define EXECUTOR_UTIL_H

#include "fmgr.h"
#include "funcapi.h"

#include "access/tupdesc.h"
#include "nodes/params.h"
#include "nodes/pg_list.h"

#include "distributed/multi_physical_planner.h"


/* utility functions for dealing with tasks in the executor */
extern bool TaskListModifiesDatabase(RowModifyLevel modLevel, List *taskList);
extern bool TaskListRequiresRollback(List *taskList);
extern bool TaskListRequires2PC(List *taskList);
extern bool TaskListCannotBeExecutedInTransaction(List *taskList);
extern bool SelectForUpdateOnReferenceTable(List *taskList);
extern bool ReadOnlyTask(TaskType taskType);
extern bool ModifiedTableReplicated(List *taskList);
extern bool ShouldRunTasksSequentially(List *taskList);

/* utility functions for handling parameters in the executor */
extern void ExtractParametersForRemoteExecution(ParamListInfo paramListInfo,
												Oid **parameterTypes,
												const char ***parameterValues);
extern void ExtractParametersFromParamList(ParamListInfo paramListInfo,
										   Oid **parameterTypes,
										   const char ***parameterValues, bool
										   useOriginalCustomTypeOids);

/* utility functions for processing tuples in the executor */
extern AttInMetadata * TupleDescGetAttBinaryInMetadata(TupleDesc tupdesc);
extern HeapTuple BuildTupleFromBytes(AttInMetadata *attinmeta, fmStringInfo *values);


#endif /* EXECUTOR_UTIL_H */
