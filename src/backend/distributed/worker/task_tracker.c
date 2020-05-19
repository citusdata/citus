/*-------------------------------------------------------------------------
 *
 * task_tracker.c
 *
 * The task tracker background process runs on every worker node. The process
 * wakes up at regular intervals, reads information from a shared hash, and
 * checks if any new tasks are assigned to this node. If they are, the process
 * runs task-specific logic, and sends queries to the postmaster for execution.
 * The task tracker then tracks the execution of these queries, and updates the
 * shared hash with task progress information.
 *
 * The task tracker is started by the postmaster when the startup process
 * finishes. The process remains alive until the postmaster commands it to
 * terminate. Normal termination is by SIGTERM, which instructs the task tracker
 * to exit(0). Emergency termination is by SIGQUIT; like any backend, the task
 * tracker will simply abort and exit on SIGQUIT.
 *
 * For details on how the task tracker manages resources during process start-up
 * and shutdown, please see the writeboard on our Basecamp project website.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include <unistd.h>

#include "commands/dbcommands.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/task_tracker.h"
#include "distributed/transmit.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "libpq/hba.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"


int TaskTrackerDelay = 200;       /* process sleep interval in millisecs */
int MaxRunningTasksPerNode = 16;  /* max number of running tasks */
int MaxTrackedTasksPerNode = 1024; /* max number of tracked tasks */
int MaxTaskStringSize = 12288; /* max size of a worker task call string in bytes */
