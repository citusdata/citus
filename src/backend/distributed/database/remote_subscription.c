#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/pg_subscription.h"
#include "commands/subscriptioncmds.h"
#include "distributed/connection_management.h"
#include "distributed/database/remote_subscription.h"
#include "distributed/database/migrator.h"
#include "distributed/remote_commands.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/syscache.h"


/*
 * RemoteSubscriptionExists returns whether the given subscription exists
 */
bool
RemoteSubscriptionExists(MultiConnection *conn, char *subscriptionName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command,
					 "SELECT true FROM pg_catalog.pg_subscription WHERE subname = %s",
					 quote_literal_cstr(subscriptionName));

	if (!SendRemoteCommand(conn, command->data))
	{
		ReportConnectionError(conn, ERROR);
	}

	bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(conn, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(conn, result, ERROR);
	}

	bool subscriptionExists = PQntuples(result) > 0;

	PQclear(result);
	ClearResults(conn, raiseErrors);

	return subscriptionExists;
}


/*
 * CreateRemoteSubscription creates a remote subscription, equivalent to running
 * CREATE SUBSCRIPTION.
 */
void
CreateRemoteSubscription(MultiConnection *conn,
						 char *sourceConnectionString, char *subscriptionName,
						 char *publicationName, char *slotName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command,
					 "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
					 "WITH (slot_name = %s, create_slot = false)",
					 quote_identifier(subscriptionName),
					 quote_literal_cstr(sourceConnectionString),
					 quote_identifier(publicationName),
					 quote_literal_cstr(slotName));

	ExecuteCriticalRemoteCommand(conn, command->data);
}


/*
 * DropRemoteSubscription drops a subscription without dropping the remote
 * replication slot by first disabling it, resetting the slot_name, and then
 * dropping the subscription.
 */
void
DropRemoteSubscription(MultiConnection *conn, char *subscriptionName)
{
	StringInfo disableCommand = makeStringInfo();

	appendStringInfo(disableCommand,
					 "ALTER SUBSCRIPTION %s DISABLE",
					 quote_identifier(subscriptionName));

	ExecuteCriticalRemoteCommand(conn, disableCommand->data);

	StringInfo setSlotNameCommand = makeStringInfo();

	appendStringInfo(setSlotNameCommand,
					 "ALTER SUBSCRIPTION %s SET (slot_name = 'none')",
					 quote_identifier(subscriptionName));

	ExecuteCriticalRemoteCommand(conn, setSlotNameCommand->data);

	StringInfo dropCommand = makeStringInfo();

	appendStringInfo(dropCommand,
					 "ALTER SUBSCRIPTION %s SET (slot_name = 'none')",
					 quote_identifier(subscriptionName));

	ExecuteCriticalRemoteCommand(conn, dropCommand->data);
}
