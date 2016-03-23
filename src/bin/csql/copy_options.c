/*
 * csql - the Citus interactive terminal
 * copy_options.c
 *	  Routines for parsing copy and stage meta commands.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 */

#include "postgres_fe.h"
#include "copy_options.h"

#include "common.h"
#include "settings.h"
#include "stringutils.h"


/* *INDENT-OFF* */
void
free_copy_options(copy_options * ptr)
{
	if (!ptr)
		return;
	free(ptr->before_tofrom);
	free(ptr->after_tofrom);
	free(ptr->file);
	free(ptr->tableName);
	free(ptr->columnList);
	free(ptr);
}


/* concatenate "more" onto "var", freeing the original value of *var */
static void
xstrcat(char **var, const char *more)
{
	char	   *newvar;

	newvar = psprintf("%s%s", *var, more);
	free(*var);
	*var = newvar;
}


/*
 * parse_slash_copy parses copy options from the given meta-command line. The
 * function then returns a dynamically allocated structure with the options, or
 * Null on parsing error.
 */
copy_options *
parse_slash_copy(const char *args)
{
	struct copy_options *result;
	char	   *token;
	const char *whitespace = " \t\n\r";
	char		nonstd_backslash = standard_strings() ? 0 : '\\';

	if (!args)
	{
		psql_error("\\copy: arguments required\n");
		return NULL;
	}

	result = pg_malloc0(sizeof(struct copy_options));

	result->before_tofrom = pg_strdup("");		/* initialize for appending */

	token = strtokx(args, whitespace, ".,()", "\"",
					0, false, false, pset.encoding);
	if (!token)
		goto error;

	/* The following can be removed when we drop 7.3 syntax support */
	if (pg_strcasecmp(token, "binary") == 0)
	{
		xstrcat(&result->before_tofrom, token);
		token = strtokx(NULL, whitespace, ".,()", "\"",
						0, false, false, pset.encoding);
		if (!token)
			goto error;
	}

	/* Handle COPY (SELECT) case */
	if (token[0] == '(')
	{
		int			parens = 1;

		while (parens > 0)
		{
			xstrcat(&result->before_tofrom, " ");
			xstrcat(&result->before_tofrom, token);
			token = strtokx(NULL, whitespace, "()", "\"'",
							nonstd_backslash, true, false, pset.encoding);
			if (!token)
				goto error;
			if (token[0] == '(')
				parens++;
			else if (token[0] == ')')
				parens--;
		}
	}

	xstrcat(&result->before_tofrom, " ");
	xstrcat(&result->before_tofrom, token);
	token = strtokx(NULL, whitespace, ".,()", "\"",
					0, false, false, pset.encoding);
	if (!token)
		goto error;

	/*
	 * strtokx() will not have returned a multi-character token starting with
	 * '.', so we don't need strcmp() here.  Likewise for '(', etc, below.
	 */
	if (token[0] == '.')
	{
		/* handle schema . table */
		xstrcat(&result->before_tofrom, token);
		token = strtokx(NULL, whitespace, ".,()", "\"",
						0, false, false, pset.encoding);
		if (!token)
			goto error;
		xstrcat(&result->before_tofrom, token);
		token = strtokx(NULL, whitespace, ".,()", "\"",
						0, false, false, pset.encoding);
		if (!token)
			goto error;
	}

	if (token[0] == '(')
	{
		/* handle parenthesized column list */
		for (;;)
		{
			xstrcat(&result->before_tofrom, " ");
			xstrcat(&result->before_tofrom, token);
			token = strtokx(NULL, whitespace, "()", "\"",
							0, false, false, pset.encoding);
			if (!token)
				goto error;
			if (token[0] == ')')
				break;
		}
		xstrcat(&result->before_tofrom, " ");
		xstrcat(&result->before_tofrom, token);
		token = strtokx(NULL, whitespace, ".,()", "\"",
						0, false, false, pset.encoding);
		if (!token)
			goto error;
	}

	if (pg_strcasecmp(token, "from") == 0)
		result->from = true;
	else if (pg_strcasecmp(token, "to") == 0)
		result->from = false;
	else
		goto error;

	/* { 'filename' | PROGRAM 'command' | STDIN | STDOUT | PSTDIN | PSTDOUT } */
	token = strtokx(NULL, whitespace, ";", "'",
					0, false, false, pset.encoding);
	if (!token)
		goto error;

	if (pg_strcasecmp(token, "program") == 0)
	{
		int			toklen;

		token = strtokx(NULL, whitespace, ";", "'",
						0, false, false, pset.encoding);
		if (!token)
			goto error;

		/*
		 * The shell command must be quoted. This isn't fool-proof, but
		 * catches most quoting errors.
		 */
		toklen = strlen(token);
		if (token[0] != '\'' || toklen < 2 || token[toklen - 1] != '\'')
			goto error;

		strip_quotes(token, '\'', 0, pset.encoding);

		result->program = true;
		result->file = pg_strdup(token);
	}
	else if (pg_strcasecmp(token, "stdin") == 0 ||
			 pg_strcasecmp(token, "stdout") == 0)
	{
		result->file = NULL;
	}
	else if (pg_strcasecmp(token, "pstdin") == 0 ||
			 pg_strcasecmp(token, "pstdout") == 0)
	{
		result->psql_inout = true;
		result->file = NULL;
	}
	else
	{
		/* filename can be optionally quoted */
		strip_quotes(token, '\'', 0, pset.encoding);
		result->file = pg_strdup(token);
		expand_tilde(&result->file);
	}

	/* Collect the rest of the line (COPY options) */
	token = strtokx(NULL, "", NULL, NULL,
					0, false, false, pset.encoding);
	if (token)
		result->after_tofrom = pg_strdup(token);

	/* set data staging options to null */
	result->tableName = NULL;
	result->columnList = NULL;

	return result;

error:
	if (token)
		psql_error("\\copy: parse error at \"%s\"\n", token);
	else
		psql_error("\\copy: parse error at end of line\n");
	free_copy_options(result);

	return NULL;
}

/* *INDENT-ON* */

/* Frees copy options. */

/*
 * ParseStageOptions takes the given copy options, parses the additional options
 * needed for the \stage command, and sets them in the copy options structure.
 * The additional parsed options are the table name and the column list.
 */
copy_options *
ParseStageOptions(copy_options *copyOptions)
{
	copy_options *stageOptions = NULL;
	const char *whitespace = " \t\n\r";
	char *tableName = NULL;
	char *columnList = NULL;
	char *token = NULL;

	const char *beforeToFrom = copyOptions->before_tofrom;
	Assert(beforeToFrom != NULL);

	token = strtokx(beforeToFrom, whitespace, ".,()", "\"",
					0, false, false, pset.encoding);

	/*
	 * We should have errored out earlier if the token were null. Similarly, we
	 * should have errored out on the "\stage (select) to" case.
	 */
	Assert(token != NULL);
	Assert(token[0] != '(');

	/* we do not support PostgreSQL's 7.3 syntax */
	if (pg_strcasecmp(token, "binary") == 0)
	{
		psql_error("\\stage: binary keyword before to/from is not supported\n");
		Assert(false);
	}

	/* init table name and append either the table name or schema name */
	tableName = pg_strdup("");
	xstrcat(&tableName, token);

	/* check for the schema.table use case */
	token = strtokx(NULL, whitespace, ".,()", "\"", 0, false, false, pset.encoding);

	if (token != NULL && token[0] == '.')
	{
		/* append the dot token */
		xstrcat(&tableName, token);

		token = strtokx(NULL, whitespace, ".,()", "\"", 0, false, false, pset.encoding);
		Assert(token != NULL);

		/* append the table name token */
		xstrcat(&tableName, token);

		token = strtokx(NULL, whitespace, ".,()", "\"", 0, false, false, pset.encoding);
	}

	/* check for the column list use case */
	if (token != NULL && token[0] == '(')
	{
		/* init column list, and add columns */
		columnList = pg_strdup("");
		for (;;)
		{
			xstrcat(&columnList, " ");
			xstrcat(&columnList, token);

			token = strtokx(NULL, whitespace, "()", "\"", 0, false, false, pset.encoding);
			Assert(token != NULL);

			if (token[0] == ')')
			{
				break;
			}
		}
		xstrcat(&columnList, " ");
		xstrcat(&columnList, token);
	}

	/* finally set additional stage options */
	stageOptions = copyOptions;
	stageOptions->tableName = tableName;
	stageOptions->columnList = columnList;

	return stageOptions;
}
