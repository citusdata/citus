/*-------------------------------------------------------------------------
 *
 * transmit.h
 *	  Shared declarations for transmitting files between remote nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSMIT_H
#define TRANSMIT_H

#include "c.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/fd.h"


/* Function declarations for transmitting files between two nodes */
extern void RedirectCopyDataToRegularFile(const char *filename);
extern void SendRegularFile(const char *filename);
extern File FileOpenForTransmit(const char *filename, int fileFlags, int fileMode);

/* Function declaration local to commands and worker modules */
extern void FreeStringInfo(StringInfo stringInfo);

/* Local functions forward declarations for Transmit statement */
extern bool IsTransmitStmt(Node *parsetree);
extern char * TransmitStatementUser(CopyStmt *copyStatement);
extern void VerifyTransmitStmt(CopyStmt *copyStatement);


#endif   /* TRANSMIT_H */
