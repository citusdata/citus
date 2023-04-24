#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xlogdefs.h"
#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/pg_lsn.h"

#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/database/remote_publication.h"
#include "distributed/database/source_database_info.h"


static char * CreatePublicationCommand(char *publicationName);
static char * CreatePublicationCommandForTables(char *publicationName, List *tableList);
static char * DropPublicationCommand(char *publicationName);


/*
 * CreateRemoteReplicationSlot creates a replication slot via the given replication
 * connection and, exportSnapshot is set, returns the name of the exported snapshot.
 */
char *
CreateRemoteReplicationSlot(MultiConnection *conn, const char *slotName,
							bool exportSnapshot, XLogRecPtr *lsn)
{
	StringInfo command = makeStringInfo();
	char *snapshot = NULL;

	appendStringInfo(command, "CREATE_REPLICATION_SLOT %s", quote_identifier(slotName));
	appendStringInfoString(command, " LOGICAL pgoutput");

	if (exportSnapshot)
	{
		appendStringInfoString(command, " EXPORT_SNAPSHOT");
	}

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

	char *lsnString = PQgetvalue(result, 0, 1);
	*lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
											   CStringGetDatum(lsnString)));
	if (!PQgetisnull(result, 0, 2))
	{
		snapshot = pstrdup(PQgetvalue(result, 0, 2));
	}
	else
	{
		snapshot = NULL;
	}

	PQclear(result);
	ClearResults(conn, raiseErrors);

	return snapshot;
}


/*
 * DropRemoteReplicationSlot drops a replication slot with the given name.
 */
void
DropRemoteReplicationSlotOverReplicationConnection(MultiConnection *replicationConn,
												   const char *slotName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "DROP_REPLICATION_SLOT %s",
					 quote_identifier(slotName));

	ExecuteCriticalRemoteCommand(replicationConn, command->data);
}


/*
 * DropRemoteReplicationSlot drops a replication slot with the given name.
 */
void
DropRemoteReplicationSlot(MultiConnection *conn, const char *slotName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command,
					 "SELECT pg_catalog.pg_drop_replication_slot(%s) "
					 "FROM pg_catalog.pg_replication_slots WHERE slot_name = %s",
					 quote_literal_cstr(slotName),
					 quote_literal_cstr(slotName));

	ExecuteCriticalRemoteCommand(conn, command->data);
}


/*
 * CreateRemotePublication creates a publication over the given connection.
 */
void
CreateRemotePublication(MultiConnection *conn, char *publicationName)
{
	char *command = CreatePublicationCommand(publicationName);
	ExecuteCriticalRemoteCommand(conn, command);
}


/*
 * CreatePublicationCommand returns a CREATE PUBLICATION command for a given
 * publication name with FOR ALL TABLES
 */
static char *
CreatePublicationCommand(char *publicationName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "CREATE PUBLICATION %s FOR ALL TABLES",
					 quote_identifier(publicationName));

	return command->data;
}


/*
 * CreateRemotePublicationForTables creates a publication over the given connection.
 */
void
CreateRemotePublicationForTables(MultiConnection *conn, char *publicationName,
								 List *tableList)
{
	List *regularTableNames = ListSourceDatabaseRegularTableNames(tableList);
	char *command = CreatePublicationCommandForTables(publicationName, regularTableNames);
	ExecuteCriticalRemoteCommand(conn, command);
}


/*
 * CreatePublicationCommandForTables returns a CREATE PUBLICATION command for a given
 * publication name and list of table names.
 */
static char *
CreatePublicationCommandForTables(char *publicationName, List *tableNameList)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "CREATE PUBLICATION %s FOR TABLE ",
					 quote_identifier(publicationName));

	bool addComma = false;

	ListCell *tableNameCell = NULL;
	foreach(tableNameCell, tableNameList)
	{
		char *qualifiedTableName = lfirst(tableNameCell);

		if (addComma)
		{
			appendStringInfoString(command, ", ");
		}
		else
		{
			addComma = true;
		}

		appendStringInfoString(command, qualifiedTableName);
	}

	return command->data;
}


/*
 * DropRemotePublication drops a publication over the given connection.
 */
void
DropRemotePublication(MultiConnection *conn, char *publicationName)
{
	char *command = DropPublicationCommand(publicationName);

	ExecuteCriticalRemoteCommand(conn, command);
}


/*
 * DropPublicationCommand returns the command for dropping a publication.
 */
static char *
DropPublicationCommand(char *publicationName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "DROP PUBLICATION IF EXISTS %s",
					 quote_identifier(publicationName));

	return command->data;
}
