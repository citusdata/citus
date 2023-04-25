#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port.h"

#include "distributed/database/migrator.h"
#include "distributed/database/pg_dump.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"


#define RUN_PROGRAM_IMPLEMENTATION
#include "distributed/runprogram.h"


#define BUFSIZE 1024


static char ** StringListToArray(List *stringList);


/*
 * GetPgDumpPath return the full path of pg_dump or NULL if not found.
 */
char *
GetPgDumpPath(void)
{
	char pgDumpPath[MAXPGPATH];

	if (find_my_exec("pg_dump", pgDumpPath) < 0)
	{
		return NULL;
	}

	return pstrdup(pgDumpPath);
}


/*
 * RunPgDump runs pg_dump against the source connection string and returns the
 * output.
 *
 * If schemaList is not NIL, then we pass --schema for each schema.
 *
 * If snapshotName is not NULL, then we pass --snapshot to use an exported
 * snapshot.
 *
 * If includeData is true, we also include data in the dump.
 *
 * If dropIfExists is specified, we pass --clean to pg_dump to get DROP statements
 * for every object.
 */
char *
RunPgDump(char *sourceConnectionString, char *snapshotName, List *schemaList,
		  List *excludeTableList, bool includeData, bool dropIfExists)
{
	char *pgDumpPath = GetPgDumpPath();

	if (pgDumpPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pg_dump")));
	}

	List *argList = NIL;

	argList = lappend(argList, pgDumpPath);
	argList = lappend(argList, "--no-publications");
	argList = lappend(argList, "--no-subscriptions");
	argList = lappend(argList, "--no-tablespaces");

	if (!includeData)
	{
		argList = lappend(argList, "--schema-only");
	}

	if (snapshotName != NULL)
	{
		StringInfo snap = makeStringInfo();
		appendStringInfo(snap, "%s", snapshotName);

		argList = lappend(argList, "--snapshot");
		argList = lappend(argList, snap->data);
	}

	if (dropIfExists)
	{
		argList = lappend(argList, "--clean");
		argList = lappend(argList, "--if-exists");
	}

	ListCell *schemaCell = NULL;
	foreach(schemaCell, schemaList)
	{
		argList = lappend(argList, "--schema");
		argList = lappend(argList, lfirst(schemaCell));
	}

	ListCell *tableNameCell = NULL;
	foreach(tableNameCell, excludeTableList)
	{
		argList = lappend(argList, "--exclude-table");
		argList = lappend(argList, lfirst(tableNameCell));
	}

	argList = lappend(argList, sourceConnectionString);

	char **args = StringListToArray(argList);

	Program program = initialize_program(args, false);

	char command[BUFSIZE];
	int commandSize = snprintf_program_command_line(&program, command, BUFSIZE);
	if (commandSize >= BUFSIZE)
	{
		ereport(DEBUG2, (errmsg("Running: %s...", command)));
	}
	else
	{
		ereport(DEBUG2, (errmsg("Running: %s", command)));
	}

	(void) execute_subprogram(&program);

	if (program.returnCode != 0)
	{
		char *errorMessage = NULL;

		if (program.stdErr != NULL)
		{
			errorMessage = pstrdup(program.stdErr);
		}

		free_program(&program);

		if (errorMessage != NULL)
		{
			ereport(ERROR, (errmsg("%s", errorMessage)));
		}
		else
		{
			ereport(ERROR, (errmsg("failed to run pg_dump")));
		}
	}

	/* obtain the full pg_dump output */
	char *databaseDump = pstrdup(program.stdOut);

	free_program(&program);

	return databaseDump;
}


/*
 * StringListToArray converts a list of strings to a NULL-terminated
 * array of strings.
 */
static char **
StringListToArray(List *stringList)
{
	int arrayLength = list_length(stringList) + 1;
	char **stringArray = (char **) palloc0(sizeof(char *) * arrayLength);
	int stringIndex = 0;

	ListCell *stringCell = NULL;
	foreach(stringCell, stringList)
	{
		stringArray[stringIndex++] = (char *) lfirst(stringCell);
	}

	stringArray[stringIndex] = NULL;

	return stringArray;
}
