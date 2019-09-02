/*
 * run_from_same_connection.h
 *
 * Sending commands from same connection to test transactions initiated from
 * worker nodes in the isolation framework.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RUN_FROM_SAME_CONNECTION_H
#define RUN_FROM_SAME_CONNECTION_H

/*
 * Config variables which will be used by isolation framework to check transactions
 * initiated from worker nodes.
 */
extern int IsolationTestSessionRemoteProcessID;
extern int IsolationTestSessionProcessID;

bool AllowNonIdleTransactionOnXactHandling(void);

#endif /* RUN_FROM_SAME_CONNECTION_H */
