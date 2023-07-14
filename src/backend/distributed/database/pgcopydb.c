#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port.h"

#include "distributed/database/pgcopydb.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"


//#define RUN_PROGRAM_IMPLEMENTATION
#include "distributed/runprogram.h"


#define BUFSIZE 1024


static char ** StringListToArray(List *stringList);


/*
 * GetPgcopydbPath return the full path of pgcopydb or NULL if not found.
 */
char *
GetPgcopydbPath(void)
{
	char pgcopydbPath[MAXPGPATH];

	if (find_my_exec("pgcopydb", pgcopydbPath) < 0)
	{
		return NULL;
	}

	return pstrdup(pgcopydbPath);
}


/*
 * RunPgcopydb runs pgcopydb against the source and target connection strings
 * and returns the output.
 */
char *
RunPgcopydb(char *sourceConnectionString, char *targetConnectionString)
{
	char *pgcopydbPath = GetPgcopydbPath();

	if (pgcopydbPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pgcopydb")));
	}

	List *argList = NIL;

	argList = lappend(argList, pgcopydbPath);
	argList = lappend(argList, "clone");
	argList = lappend(argList, "--source");
	argList = lappend(argList, sourceConnectionString);
	argList = lappend(argList, "--target");
	argList = lappend(argList, targetConnectionString);
	argList = lappend(argList, "--restart");
	argList = lappend(argList, "--skip-extensions");

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
			ereport(ERROR, (errmsg("failed to run pgcopydb")));
		}
	}

	/* obtain the full pgcopydb output */
	char *output = pstrdup(program.stdOut);

	free_program(&program);

	return output;
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
