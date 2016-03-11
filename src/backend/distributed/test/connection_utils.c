/*-------------------------------------------------------------------------
 *
 * test/src/connection_utils.c
 *
 * This file isolates a test function which modifies private connection
 * state, ensuring the correct ("internal") headers are included, rather
 * than the version-specific server ones. Without this kludge, builds on
 * certain platforms (those which install a single libpq version but can
 * have multiple PostgreSQL server versions) will faile.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "internal/c.h"
#include "libpq-fe.h"
#include "internal/libpq-int.h"

#include "distributed/test_helper_functions.h"

/*
 * SetConnectionStatus simply uses the internal headers to access "private"
 * fields of the connection struct in order to force a cache connection to a
 * particular status.
 */
void
SetConnectionStatus(PGconn *connection, ConnStatusType status)
{
	connection->status = status;
}
