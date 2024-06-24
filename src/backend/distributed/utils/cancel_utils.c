/*
 * cancel_utils.c
 *
 * Utilities related to query cancellation
 *
 * Copyright (c) Citus Data, Inc.
 */


#include "postgres.h"

#include "miscadmin.h"

#include "distributed/cancel_utils.h"


/*
 * IsHoldOffCancellationReceived returns true if a cancel signal
 * was sent and HOLD_INTERRUPTS was called prior to this. The motivation
 * here is that since our queries can take a long time, in some places
 * we do not want to wait even if HOLD_INTERRUPTS was called.
 */
bool
IsHoldOffCancellationReceived()
{
	return InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending);
}
