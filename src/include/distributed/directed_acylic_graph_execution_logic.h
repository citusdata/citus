/*-------------------------------------------------------------------------
 *
 * directed_acylic_graph_execution_logic.h
 *	  Execution logic for directed acylic graph tasks.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DIRECTED_ACYLIC_GRAPH_EXECUTION_LOGIC_H
#define DIRECTED_ACYLIC_GRAPH_EXECUTION_LOGIC_H

#include "postgres.h"

#include "nodes/pg_list.h"

extern void ExecuteTasksInDependencyOrder(List *allTasks, List *excludedTasks);


#endif /* DIRECTED_ACYLIC_GRAPH_EXECUTION_LOGIC_H */
