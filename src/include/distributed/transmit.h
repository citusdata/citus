/*-------------------------------------------------------------------------
 *
 * transmit.h
 *	  Shared declarations for transmitting files between remote nodes.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSMIT_H
#define TRANSMIT_H

#include "lib/stringinfo.h"
#include "storage/fd.h"


/* Function declarations for transmitting files between two nodes */
extern void RedirectCopyDataToRegularFile(const char *filename);
extern void SendRegularFile(const char *filename);
extern File FileOpenForTransmit(const char *filename, int fileFlags, int fileMode);

/* Function declaration local to commands and worker modules */
extern void FreeStringInfo(StringInfo stringInfo);


#endif   /* TRANSMIT_H */
