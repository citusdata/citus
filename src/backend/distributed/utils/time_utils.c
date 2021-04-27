

#include "postgres.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "distributed/time_utils.h"

/*
 * WaitForMiliseconds waits for given timeout and then checks for some
 * interrupts.
 */
void WaitForMiliseconds(long timeout)
{
	int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;

	/* wait until timeout, or until somebody wakes us up */
	int rc = WaitLatch(MyLatch, latchFlags, timeout, PG_WAIT_EXTENSION);

	/* emergency bailout if postmaster has died */
	if (rc & WL_POSTMASTER_DEATH)
	{
		proc_exit(1);
	}

	if (rc & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}