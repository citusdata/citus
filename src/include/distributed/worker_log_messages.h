/*-------------------------------------------------------------------------
 *
 * worker_log_messages.h
 *   Functions for handling log messages from the workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_LOG_MESSAGES_H
#define WORKER_LOG_MESSAGES_H


#include "distributed/connection_management.h"


/* minimum log level for worker messages to be propagated */
extern int WorkerMinMessages;

void SetCitusNoticeReceiver(MultiConnection *connection);
void EnableWorkerMessagePropagation(void);
void DisableWorkerMessagePropagation(void);
void ErrorIfWorkerErrorIndicationReceived(void);
void ResetWorkerErrorIndication(void);


#endif /* WORKER_LOG_MESSAGES_H */
