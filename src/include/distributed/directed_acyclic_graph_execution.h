/*-------------------------------------------------------------------------
 *
 * directed_acyclic_graph_execution.h
 *	  Execution logic for directed acyclic graph tasks.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DIRECTED_ACYCLIC_GRAPH_EXECUTION_H
#define DIRECTED_ACYCLIC_GRAPH_EXECUTION_H

#include "postgres.h"

#include "nodes/pg_list.h"

extern void ExecuteTasksInDependencyOrder(List *allTasks, List *excludedTasks,
										  List *jobIds);


#endif /* DIRECTED_ACYCLIC_GRAPH_EXECUTION_H */
