/*-------------------------------------------------------------------------
 *
 * remote_publication.h
 *	  definition of remote_publication data types
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_PUBLICATION_H
#define REMOTE_PUBLICATION_H


#include "libpq-fe.h"
#include "nodes/pg_list.h"
#include "utils/pg_lsn.h"

#include "distributed/connection_management.h"


char * CreateRemoteReplicationSlot(MultiConnection *conn, const char *slotName,
								   bool exportSnapshot, XLogRecPtr *lsn);
void DropRemoteReplicationSlot(MultiConnection *conn, const char *slotName);
void DropRemoteReplicationSlotOverReplicationConnection(MultiConnection *replicationConn,
														const char *slotName);
void CreateRemotePublication(MultiConnection *conn, char *publicationName);
void CreateRemotePublicationForTables(MultiConnection *conn, char *publicationName,
									  List *tableList);
void DropRemotePublication(MultiConnection *conn, char *publicationName);


#endif
