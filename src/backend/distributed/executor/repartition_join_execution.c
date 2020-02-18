/*-------------------------------------------------------------------------
 *
 * repartition_join_execution.c
 *
 * This file contains repartition specific logic.
 * ExecuteDependentTasks takes a list of top level tasks. Its logic is as follows:
 * - It generates all the tasks by descending in the tasks tree. Note that each task
 *  has a dependentTaskList.
 * - It generates FetchTask queryStrings with the MapTask queries. It uses the first replicate to
 *  fetch data when replication factor is > 1. Note that if a task fails in any replica adaptive executor
 *  gives an error, so if we come to a fetchTask we know for sure that its dependedMapTask is executed in all
 *  replicas.
 * - It creates schemas in each worker in a single transaction to store intermediate results.
 * - It iterates all tasks and finds the ones whose dependencies are already executed, and executes them with
 *  adaptive executor logic.
 *
 *
 * Repartition queries do not begin a transaction even if we are in
 * a transaction block. As we don't begin a transaction, they won't see the
 * DDLs that happened earlier in the transaction because we don't have that
 * transaction id with repartition queries. Therefore we error in this case.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"
#include "access/hash.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "distributed/hash_helpers.h"

#include "distributed/directed_acyclic_graph_execution.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/adaptive_executor.h"
#include "distributed/worker_manager.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_server_executor.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/worker_transaction.h"
#include "distributed/worker_manager.h"
#include "distributed/transaction_management.h"
#include "distributed/multi_task_tracker_executor.h"
#include "distributed/worker_transaction.h"
#include "distributed/metadata_cache.h"
#include "distributed/listutils.h"
#include "distributed/transmit.h"


static List * CreateTemporarySchemasForMergeTasks(Job *topLevelJob);
static List * ExtractJobsInJobTree(Job *job);
static void TraverseJobTree(Job *curJob, List **jobs);
static char * GenerateCreateSchemasCommand(List *jobIds, char *schemaOwner);
static char * GenerateJobCommands(List *jobIds, char *templateCommand);
static char * GenerateDeleteJobsCommand(List *jobIds);


/*
 * ExecuteDependentTasks executes all tasks except the top level tasks
 * in order from the task tree. At a time, it can execute different tasks from
 * different jobs.
 */
List *
ExecuteDependentTasks(List *topLevelTasks, Job *topLevelJob)
{
	EnsureNoModificationsHaveBeenDone();

	List *allTasks = TaskAndExecutionList(topLevelTasks);

	List *jobIds = CreateTemporarySchemasForMergeTasks(topLevelJob);

	ExecuteTasksInDependencyOrder(allTasks, topLevelTasks, jobIds);

	return jobIds;
}


/*
 * CreateTemporarySchemasForMergeTasks creates the necessary schemas that will be used
 * later in each worker. Single transaction is used to create the schemas.
 */
static List *
CreateTemporarySchemasForMergeTasks(Job *topLeveLJob)
{
	List *jobIds = ExtractJobsInJobTree(topLeveLJob);
	char *createSchemasCommand = GenerateCreateSchemasCommand(jobIds, CurrentUserName());
	SendCommandToAllWorkers(createSchemasCommand, CitusExtensionOwnerName());
	return jobIds;
}


/*
 * ExtractJobsInJobTree returns all job ids in the job tree
 * where the given job is root.
 */
static List *
ExtractJobsInJobTree(Job *job)
{
	List *jobIds = NIL;
	TraverseJobTree(job, &jobIds);
	return jobIds;
}


/*
 * TraverseJobTree does a dfs in the current job and adds
 * all of its job ids.
 */
static void
TraverseJobTree(Job *curJob, List **jobIds)
{
	ListCell *jobCell = NULL;
	*jobIds = lappend(*jobIds, (void *) curJob->jobId);

	foreach(jobCell, curJob->dependentJobList)
	{
		Job *childJob = (Job *) lfirst(jobCell);
		TraverseJobTree(childJob, jobIds);
	}
}


/*
 * GenerateCreateSchemasCommand returns concatanated create schema commands.
 */
static char *
GenerateCreateSchemasCommand(List *jobIds, char *ownerName)
{
	StringInfo createSchemaCommand = makeStringInfo();
	ListCell *jobIdCell = NULL;

	foreach(jobIdCell, jobIds)
	{
		uint64 jobId = (uint64) lfirst(jobIdCell);
		appendStringInfo(createSchemaCommand, WORKER_CREATE_SCHEMA_QUERY,
						 jobId, quote_literal_cstr(ownerName));
	}
	return createSchemaCommand->data;
}


/*
 * GenerateJobCommands returns concatenated commands with the given template
 * command for each job id from the given job ids. The returned command is
 * exactly list_length(jobIds) subcommands.
 *  E.g create_schema(jobId1); create_schema(jobId2); ...
 * This way we can send the command in just one latency to a worker.
 */
static char *
GenerateJobCommands(List *jobIds, char *templateCommand)
{
	StringInfo createSchemaCommand = makeStringInfo();
	ListCell *jobIdCell = NULL;

	foreach(jobIdCell, jobIds)
	{
		uint64 jobId = (uint64) lfirst(jobIdCell);
		appendStringInfo(createSchemaCommand, templateCommand, jobId);
	}
	return createSchemaCommand->data;
}


/*
 * DoRepartitionCleanup removes the temporary job directories and schemas that are
 * used for repartition queries for the given job ids.
 */
void
DoRepartitionCleanup(List *jobIds)
{
	SendCommandToWorkersOptionalInParallel(ALL_WORKERS, GenerateDeleteJobsCommand(jobIds),
										   CitusExtensionOwnerName());
}


/*
 * GenerateDeleteJobsCommand returns concatanated remove job dir commands.
 */
static char *
GenerateDeleteJobsCommand(List *jobIds)
{
	return GenerateJobCommands(jobIds, WORKER_REPARTITION_CLEANUP_QUERY);
}
