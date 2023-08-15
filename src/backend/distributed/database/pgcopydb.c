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


#define RUN_PROGRAM_IMPLEMENTATION 1
#include "distributed/runprogram.h"


#define BUFSIZE 1024


static char * ExecuteProgram(List *argList);
static void ProcessBufferCallback(const char *buffer, bool error);
static int SplitLines(char *buffer, char **linesArray, int size);
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
 * RunPgcopydbClone runs pgcopydb clone against the source and target connection strings
 * and returns the output.
 */
char *
RunPgcopydbClone(char *sourceConnectionString, char *targetConnectionString,
				 char *migrationName, bool useFollow)
{
	char *pgcopydbPath = GetPgcopydbPath();

	if (pgcopydbPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pgcopydb")));
	}

	List *argList = NIL;

	argList = lappend(argList, pgcopydbPath);
	argList = lappend(argList, "clone");
	argList = lappend(argList, "--dir");
	/* TODO: escaping? */
	argList = lappend(argList, psprintf("/tmp/%s", migrationName));
	argList = lappend(argList, "--source");
	argList = lappend(argList, sourceConnectionString);
	argList = lappend(argList, "--target");
	argList = lappend(argList, targetConnectionString);
	argList = lappend(argList, "--slot-name");
	argList = lappend(argList, migrationName);
	argList = lappend(argList, "--origin");
	argList = lappend(argList, migrationName);
	argList = lappend(argList, "--restart");
	argList = lappend(argList, "--skip-extensions");

	if (useFollow)
	{
		argList = lappend(argList, "--follow");
	}

	putenv(psprintf("PGCOPYDB_LOG_FILENAME=/tmp/%s.log", migrationName));
	return ExecuteProgram(argList);
}


/*
 * ExecuteProgram executes a program with the given arguments and returns
 * the stdout.
 *
 * TODO: what to do with stderr?
 */
static char *
ExecuteProgram(List *argList)
{
	char **args = StringListToArray(argList);

	Program program = initialize_program(args, false);
	program.processBuffer = &ProcessBufferCallback;

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
	char *output = program.stdOut != NULL ? pstrdup(program.stdOut) : NULL;

	free_program(&program);

	return output;
}


/*
* ProcessBufferCallback is a function callback to use with the subcommands.c
* library when we want to output a command's output as it's running.
*/
static void
ProcessBufferCallback(const char *buffer, bool error)
{
	char *outLines[BUFSIZE] = { 0 };
	int lineCount = SplitLines((char *) buffer, outLines, BUFSIZE);
	int lineNumber = 0;

	for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
	{
		if (strcmp(outLines[lineNumber], "") != 0)
		{
			ereport(DEBUG1, (errmsg("%s", outLines[lineNumber])));
		}
	}
}


/*
 * SplitLines prepares a multi-line error message in a way that calling code
 * can loop around one line at a time and call log_error() or log_warn() on
 * individual lines.
 */
static int
SplitLines(char *buffer, char **linesArray, int size)
{
	int lineNumber = 0;
	char *currentLine = buffer;

	if (buffer == NULL)
	{
		return 0;
	}

	if (linesArray == NULL)
	{
		return -1;
	}

	do {
		char *newLinePtr = strchr(currentLine, '\n');

		if (newLinePtr == NULL)
		{
			/* strlen(currentLine) > 0 */
			if (*currentLine != '\0')
			{
				linesArray[lineNumber++] = currentLine;
			}

			currentLine = NULL;
		}
		else
		{
			*newLinePtr = '\0';

			linesArray[lineNumber++] = currentLine;

			currentLine = ++newLinePtr;
		}
	} while (currentLine != NULL && *currentLine != '\0' && lineNumber < size);

	return lineNumber;
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


/*
 * RunPgcopydbListProgress runs pgcopydb list progress against the source
 * and target connection strings and returns the output.
 */
char *
RunPgcopydbListProgress(char *sourceConnectionString, char *migrationName)
{
	char *pgcopydbPath = GetPgcopydbPath();

	if (pgcopydbPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pgcopydb")));
	}

	List *argList = NIL;

	argList = lappend(argList, pgcopydbPath);
	argList = lappend(argList, "list");
	argList = lappend(argList, "progress");
	argList = lappend(argList, "--dir");
	/* TODO: escaping? */
	argList = lappend(argList, psprintf("/tmp/%s", migrationName));
	argList = lappend(argList, "--source");
	argList = lappend(argList, sourceConnectionString);
	argList = lappend(argList, "--json");

	return ExecuteProgram(argList);
}


/*
 * RunPgcopydbStreamSentinelSetEndpos runs pgcopydb stream sentinel set endpos
 * against the source and target connection strings and returns the output.
 */
char *
RunPgcopydbStreamSentinelSetEndpos(char *sourceConnectionString, char *migrationName)
{
	char *pgcopydbPath = GetPgcopydbPath();

	if (pgcopydbPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pgcopydb")));
	}

	List *argList = NIL;

	argList = lappend(argList, pgcopydbPath);
	argList = lappend(argList, "stream");
	argList = lappend(argList, "sentinel");
	argList = lappend(argList, "set");
	argList = lappend(argList, "endpos");
	argList = lappend(argList, "--source");
	argList = lappend(argList, sourceConnectionString);
	argList = lappend(argList, "--current");

	return ExecuteProgram(argList);
}
