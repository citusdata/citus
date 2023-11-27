/*-------------------------------------------------------------------------
 *
 * priority.c
 *	  Utilities for managing CPU priority.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"

#include "distributed/priority.h"

int CpuPriority = 0;
int CpuPriorityLogicalRepSender = CPU_PRIORITY_INHERIT;
int MaxHighPriorityBackgroundProcesess = 2;


/*
 * SetOwnPriority changes the CPU priority of the current backend to the given
 * priority. If the OS disallows us to set the priority to the given value, we
 * only warn about it.
 */
void
SetOwnPriority(int priority)
{
	if (priority == CPU_PRIORITY_INHERIT)
	{
		return;
	}

	if (setpriority(PRIO_PROCESS, getpid(), priority) == -1)
	{
		ereport(WARNING, (
					errmsg("could not set cpu priority to %d: %m", priority),
					errhint("Try changing the 'nice' resource limit by changing "
							"/etc/security/limits.conf for the postgres user "
							"and/or by setting LimitNICE in your the systemd "
							"service file (depending on how you start "
							"postgres)."
							)));
	}
}


/*
 * GetOwnPriority returns the current CPU priority value of the backend.
 */
int
GetOwnPriority(void)
{
	errno = 0;
	int result = getpriority(PRIO_PROCESS, getpid());

	/*
	 * We explicitly check errno too because getpriority can return -1 on
	 * success too, if the actual priority value is -1
	 */
	if (result == -1 && errno != 0)
	{
		ereport(WARNING, (errmsg("could not get current cpu priority value, "
								 "assuming 0: %m")));
		return 0;
	}
	return result;
}
