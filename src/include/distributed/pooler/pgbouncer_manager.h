/*-------------------------------------------------------------------------
 *
 * pgbouncer_manager.h
 *   Functions for managing outbound pgbouncers
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGBOUNCER_MANAGER_H
#define PGBOUNCER_MANAGER_H

/* default number of inbound pgbouncer processes (0 is disabled) */
#define PGBOUNCER_INBOUND_PROCS_DEFAULT 0

/* bits reserved for local pgbouncer ID in the pgbouncer peer_id */
#define PGBOUNCER_PEER_ID_LOCAL_ID_BITS 5

/* maximum number of inbound pgbouncer processes */
#define PGBOUNCER_INBOUND_PROCS_MAX ((1 << PGBOUNCER_PEER_ID_LOCAL_ID_BITS) - 1)


/* GUC variable that sets the number of inbound pgbouncer procs */
extern int PgBouncerInboundProcs;

/* GUC variable that sets the inbound pgbouncer port */
extern int PgBouncerInboundPort;

/* GUC variable that sets the path to pgbouncer executable */
extern char *PgBouncerPath;

void InitializeSharedPgBouncerManager(void);
size_t SharedPgBouncerManagerShmemSize(void);
void PgBouncerManagerMain(Datum arg);
void EnsurePgBouncersRunning(void);


#endif /* PGBOUNCER_MANAGER_H */
