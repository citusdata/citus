#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "distributed/connection_management.h"
#include "distributed/database/source_database_info.h"
#include "distributed/listutils.h"
#include "distributed/remote_commands.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"


static List * SplitString(const char *str, char delimiter, int maxLength);
static bool ParseBool(char *boolString);


/*
 * GetSourceDatabaseVersion gets the version number of the source database.
 */
SemanticVersion *
GetSourceDatabaseVersion(MultiConnection *conn)
{
	char *versionQuery = "SHOW server_version";

	if (!SendRemoteCommand(conn, versionQuery))
	{
		ReportConnectionError(conn, ERROR);
	}

	bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(conn, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(conn, result, ERROR);
	}

	int tupleCount = PQntuples(result);
	if (tupleCount != 1)
	{
		PQclear(result);
		ClearResults(conn, raiseErrors);

		ereport(ERROR, (errmsg("unexpected number of tuples returned for query: %s",
							   versionQuery)));
	}

	char *versionStr = PQgetvalue(result, 0, 0);
	List *versionParts = SplitString(versionStr, '.', 16);

	PQclear(result);
	ClearResults(conn, raiseErrors);

	SemanticVersion *version = palloc0(sizeof(SemanticVersion));

	if (list_length(versionParts) == 2)
	{
		/* modern versioning schema */
		version->major = pg_strtoint32((char *) linitial(versionParts));
		version->minor = NO_MINOR_VERSION;
		version->patch = pg_strtoint32((char *) llast(versionParts));
	}
	else if (list_length(versionParts) == 3)
	{
		/* old versioning schema */
		version->major = pg_strtoint32((char *) linitial(versionParts));
		version->minor = pg_strtoint32((char *) list_nth(versionParts, 1));
		version->patch = pg_strtoint32((char *) llast(versionParts));
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized server_verion scheme: %s",
							   versionStr)));
	}

	return version;
}


/*
 * SplitString splits the given string by the given delimiter.
 */
static List *
SplitString(const char *str, char delimiter, int maxLength)
{
	size_t len = strnlen(str, maxLength);
	if (len == 0)
	{
		return NIL;
	}

	List *tokenList = NIL;
	StringInfo token = makeStringInfo();

	for (size_t index = 0; index < len; index++)
	{
		if (str[index] == delimiter)
		{
			tokenList = lappend(tokenList, token->data);
			token = makeStringInfo();
		}
		else
		{
			appendStringInfoChar(token, str[index]);
		}
	}

	/* append last token */
	tokenList = lappend(tokenList, token->data);

	return tokenList;
}


/*
 * SemanticVersionToString returns the PostgreSQL version as a string.
 */
char *
SemanticVersionToString(SemanticVersion *version)
{
	StringInfo versionStr = makeStringInfo();

	if (version->minor == NO_MINOR_VERSION)
	{
		appendStringInfo(versionStr, "%d.%d", version->major, version->patch);
	}
	else
	{
		appendStringInfo(versionStr, "%d.%d.%d", version->major, version->minor,
						 version->patch);
	}

	return versionStr->data;
}


/*
 * ListSourceDatabaseSchemas lists schemas in the remote database that are not
 * system tables or owned by an extension.
 */
List *
ListSourceDatabaseSchemas(MultiConnection *conn)
{
	char *schemaListQuery =
		"SELECT nspname "
		"FROM pg_namespace n "
		"WHERE ((nspname = 'public' OR nspowner <> 10) "
		"OR current_setting('is_superuser')::bool) "
		"AND nspname NOT LIKE 'pg_%' "
		"AND nspname <> 'information_schema' "
		"AND n.oid NOT IN (SELECT objid FROM pg_depend "
		"WHERE deptype = 'e' AND classid = 'pg_namespace'::regclass)";

	if (!SendRemoteCommand(conn, schemaListQuery))
	{
		ReportConnectionError(conn, ERROR);
	}

	bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(conn, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(conn, result, ERROR);
	}

	int schemaCount = PQntuples(result);
	List *schemaList = NIL;

	for (int row = 0; row < schemaCount; row++)
	{
		char *schemaName = PQgetvalue(result, row, 0);

		schemaList = lappend(schemaList, pstrdup(schemaName));
	}

	PQclear(result);
	ClearResults(conn, raiseErrors);

	return schemaList;
}


/*
 * ListSourceDatabaseTables lists all tables in the remote database that or not
 * system tables or owned by an extension.
 */
List *
ListSourceDatabaseTables(MultiConnection *conn)
{
	StringInfo tableListQuery = makeStringInfo();

	appendStringInfoString(tableListQuery,
						   "SELECT nspname, relname, relkind,"
						   " (SELECT count(*) > 0 FROM pg_index "
						   "  WHERE indrelid = c.oid AND (indisreplident OR indisprimary)) AS hasreplident "
						   "FROM pg_class c "
						   "JOIN pg_namespace n ON (c.relnamespace = n.oid) "
						   "WHERE (relkind = 'r' OR relkind = 'p') "
						   "AND nspname <> 'pg_catalog' "
						   "AND nspname <> 'information_schema' "
						   "AND c.oid NOT IN (SELECT objid FROM pg_depend "
						   "WHERE deptype = 'e' AND classid = 'pg_class'::regclass)");

	if (!SendRemoteCommand(conn, tableListQuery->data))
	{
		ReportConnectionError(conn, ERROR);
	}

	bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(conn, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(conn, result, ERROR);
	}

	int tableCount = PQntuples(result);
	List *tableList = NIL;

	for (int row = 0; row < tableCount; row++)
	{
		SourceDatabaseTable *userTable = palloc0(sizeof(SourceDatabaseTable));
		userTable->schemaName = pstrdup(PQgetvalue(result, row, 0));
		userTable->tableName = pstrdup(PQgetvalue(result, row, 1));
		userTable->relkind = *(PQgetvalue(result, row, 2));
		userTable->hasReplicaIdentity = ParseBool(PQgetvalue(result, row, 3));

		tableList = lappend(tableList, userTable);
	}

	PQclear(result);
	ClearResults(conn, raiseErrors);

	return tableList;
}


/*
 * ListSourceDatabaseRegularTableNames returns the names of regular tables from
 * the given table list.
 */
List *
ListSourceDatabaseRegularTableNames(List *tableList)
{
	List *tableNameList = NIL;

	ListCell *tableCell = NULL;
	foreach(tableCell, tableList)
	{
		SourceDatabaseTable *userTable = (SourceDatabaseTable *) lfirst(tableCell);

		if (userTable->relkind != RELKIND_RELATION)
		{
			/*
			 * Logical replication is only supported (across versions) for regular
			 * tables.
			 */
			continue;
		}

		char *qualifiedTableName = quote_qualified_identifier(userTable->schemaName,
															  userTable->tableName);

		tableNameList = lappend(tableNameList, qualifiedTableName);
	}

	return tableNameList;
}


/*
 * ListSourceDatabaseSequences returns a list of SourceDatabaseSequence structs
 * for each sequence in the source database.
 */
List *
ListSourceDatabaseSequences(MultiConnection *conn)
{
	char *sequenceListQuery =
		"SELECT nspname, relname "
		"FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) "
		"WHERE relkind = 'S'";

	if (!SendRemoteCommand(conn, sequenceListQuery))
	{
		ReportConnectionError(conn, ERROR);
	}

	bool raiseErrors = true;
	PGresult *sequenceListRes = GetRemoteCommandResult(conn, raiseErrors);
	if (!IsResponseOK(sequenceListRes))
	{
		ReportResultError(conn, sequenceListRes, ERROR);
	}

	int sequenceCount = PQntuples(sequenceListRes);
	List *sequenceList = NIL;

	for (int row = 0; row < sequenceCount; row++)
	{
		char *schemaName = PQgetvalue(sequenceListRes, row, 0);
		char *sequenceName = PQgetvalue(sequenceListRes, row, 1);

		SourceDatabaseSequence *sequence =
			(SourceDatabaseSequence *) palloc0(sizeof(SourceDatabaseSequence));
		sequence->schemaName = pstrdup(schemaName);
		sequence->sequenceName = pstrdup(sequenceName);
		sequence->lastValue = 0L;

		sequenceList = lappend(sequenceList, sequence);
	}

	PQclear(sequenceListRes);
	ClearResults(conn, raiseErrors);

	SourceDatabaseSequence *sequence = NULL;
	foreach_ptr(sequence, sequenceList)
	{
		char *schemaName = sequence->schemaName;
		char *sequenceName = sequence->sequenceName;

		StringInfo getSequenceValueQuery = makeStringInfo();

		appendStringInfo(getSequenceValueQuery,
						 "SELECT last_value FROM %s",
						 quote_qualified_identifier(schemaName, sequenceName));

		if (!SendRemoteCommand(conn, getSequenceValueQuery->data))
		{
			ReportConnectionError(conn, ERROR);
		}

		PGresult *seqRes = GetRemoteCommandResult(conn, raiseErrors);
		if (!IsResponseOK(sequenceListRes))
		{
			/* TODO: error */
			ReportResultError(conn, seqRes, WARNING);
			PQclear(seqRes);
			continue;
		}

		sequence->lastValue = pg_strtoint64(PQgetvalue(seqRes, 0, 0));
		PQclear(seqRes);
		ClearResults(conn, raiseErrors);
	}

	return sequenceList;
}


/*
 * ListSourceMigrationPublications lists all publications starting with the
 * given prefix.
 */
List *
ListSourceMigrationPublications(MultiConnection *conn, char *publicationNamePrefix)
{
	StringInfo command = makeStringInfo();

	/* TODO: does the quoting work correctly? */
	appendStringInfo(command,
					 "SELECT pubname "
					 "FROM pg_catalog.pg_publication "
					 "WHERE pubname LIKE '%s%%'",
					 quote_identifier(publicationNamePrefix));

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

	int tupleCount = PQntuples(result);
	List *publicationNameList = NIL;

	for (int row = 0; row < tupleCount; row++)
	{
		char *publicationName = pstrdup(PQgetvalue(result, row, 0));

		publicationNameList = lappend(publicationNameList, publicationName);
	}

	PQclear(result);
	ClearResults(conn, raiseErrors);

	return publicationNameList;
}


/*
 * ParseBool parses the string representation of a boolean.
 */
static bool
ParseBool(char *boolString)
{
	Datum boolStringDatum = CStringGetDatum(boolString);
	Datum boolDatum = DirectFunctionCall1(boolin, boolStringDatum);
	return DatumGetBool(boolDatum);
}


/*
 * CreateDDLReplicationTable creates the table for DDL replication.
 * We make sure regular users cannot create this table themselves
 * by creating it in the pg_catalog schema.
 */
void
CreateDDLReplicationTable(MultiConnection *conn)
{
	char *createCommand =
		"DO LANGUAGE plpgsql $do$\n"
		"BEGIN\n"
		"DROP TABLE IF EXISTS pg_catalog.ddl_statements;\n"
		"CREATE SCHEMA citus_db_migration_tmp;\n"
		"CREATE TABLE citus_db_migration_tmp.ddl_statements (\n"
		" ddl_id bigserial primary key,\n"
		" ddl_command text not null,\n"
		" search_path text not null,\n"
		" user_name name not null,\n"
		" execution_time timestamptz not null default now()\n"
		");\n"
		"ALTER TABLE citus_db_migration_tmp.ddl_statements SET SCHEMA pg_catalog;\n"
		"DROP SCHEMA citus_db_migration_tmp;\n"
		"END\n"
		"$do$;";

	ExecuteCriticalRemoteCommand(conn, createCommand);
}


/*
 * CreateDDLReplicationTrigger creates the trigger for DDL replication on the
 * source database.
 */
void
CreateDDLReplicationTrigger(MultiConnection *conn)
{
	char *createTriggerCommand =
		"DO LANGUAGE plpgsql $do$\n"
		"BEGIN\n"
		"DROP FUNCTION IF EXISTS pg_catalog.ddl_trigger();\n"
		"CREATE FUNCTION pg_catalog.ddl_trigger()\n"
		"RETURNS trigger LANGUAGE C STRICT AS 'citus', $$database_shard_move_trigger$$;\n"
		"CREATE TRIGGER ddl_insert BEFORE INSERT ON pg_catalog.ddl_statements "
		"FOR EACH ROW EXECUTE FUNCTION pg_catalog.ddl_trigger();\n"
		"ALTER TABLE pg_catalog.ddl_statements "
		"ENABLE REPLICA TRIGGER ddl_insert;\n"
		"END\n"
		"$do$;";

	ExecuteCriticalRemoteCommand(conn, createTriggerCommand);
}
