/*-------------------------------------------------------------------------
 *
 * connection_management.c
 *   Regression tests for the metadata connection of a transaction.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "utils/builtins.h"
#include "utils/memutils.h"

#include "distributed/connection_management.h"
#include "distributed/remote_transaction.h"
#include "distributed/transaction_management.h"


PG_FUNCTION_INFO_V1(test_metadata_connection_selection);
PG_FUNCTION_INFO_V1(test_metadata_connection_id);

static void ExpectMetadataConnectionError(const char *hostname, int32 port,
										  uint32 flags);


/*
 * ExpectMetadataConnectionError errors out unless a metadata request with the
 * given flags errors out because the metadata connection is not available.
 */
static void
ExpectMetadataConnectionError(const char *hostname, int32 port, uint32 flags)
{
	MemoryContext savedContext = CurrentMemoryContext;
	volatile bool failed = false;

	PG_TRY();
	{
		GetNodeConnection(flags | REQUIRE_METADATA_CONNECTION, hostname, port);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *error = CopyErrorData();
		if (error->sqlerrcode != ERRCODE_OBJECT_IN_USE)
		{
			PG_RE_THROW();
		}

		FlushErrorState();
		FreeErrorData(error);
		failed = true;
	}
	PG_END_TRY();

	if (!failed)
	{
		elog(ERROR, "an unavailable metadata connection was replaced");
	}
}


/*
 * test_metadata_connection_selection checks that metadata requests use a single
 * connection in a transaction, and error out rather than use another connection
 * when it is not available.
 */
Datum
test_metadata_connection_selection(PG_FUNCTION_ARGS)
{
	char *hostname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32 port = PG_GETARG_INT32(1);

	MultiConnection *connection = GetNodeConnection(0, hostname, port);
	ClaimConnectionExclusively(connection);
	MultiConnection *metadataConnection =
		GetNodeConnection(REQUIRE_METADATA_CONNECTION, hostname, port);
	UnclaimConnection(connection);

	/* metadata requests keep using it, other requests keep the list order */
	if (metadataConnection == connection ||
		GetNodeConnection(REQUIRE_METADATA_CONNECTION, hostname, port) !=
		metadataConnection ||
		GetNodeConnection(0, hostname, port) != connection)
	{
		elog(ERROR, "the metadata connection was not kept for the transaction");
	}

	CloseConnection(connection);
	connection = GetNodeConnection(OUTSIDE_TRANSACTION, hostname, port);
	if (connection == metadataConnection)
	{
		elog(ERROR, "an outside transaction request got the metadata connection");
	}
	CloseConnection(connection);

	ClaimConnectionExclusively(metadataConnection);
	ExpectMetadataConnectionError(hostname, port, 0);
	UnclaimConnection(metadataConnection);

	/* a connection marked for closing is kept during its remote transaction */
	UseCoordinatedTransaction();
	RemoteTransactionBeginIfNecessary(metadataConnection);
	ForceConnectionCloseAtTransactionEnd(metadataConnection);
	if (GetNodeConnection(REQUIRE_METADATA_CONNECTION, hostname, port) !=
		metadataConnection)
	{
		elog(ERROR, "the metadata connection was replaced during its transaction");
	}
	ExpectMetadataConnectionError(hostname, port, OUTSIDE_TRANSACTION);

	ShutdownConnection(metadataConnection);
	ResetRemoteTransaction(metadataConnection);
	ExpectMetadataConnectionError(hostname, port, 0);

	PG_RETURN_VOID();
}


/*
 * test_metadata_connection_id returns the ID of the connection returned for a
 * metadata request, or for a request outside of the transaction.
 */
Datum
test_metadata_connection_id(PG_FUNCTION_ARGS)
{
	char *hostname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32 port = PG_GETARG_INT32(1);
	uint32 flags = PG_GETARG_BOOL(2) ? OUTSIDE_TRANSACTION :
				   REQUIRE_METADATA_CONNECTION;

	MultiConnection *connection = GetNodeConnection(flags, hostname, port);

	PG_RETURN_INT64(connection->connectionId);
}
