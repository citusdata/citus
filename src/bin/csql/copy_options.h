/*
 * csql - the Citus interactive terminal
 * copy_options.h
 *	  Shared declarations for parsing copy and stage meta-commands. The stage
 *	  meta-command borrows from copy's syntax, but does not yet support
 *	  outputting table data to a file. Further, the stage command reuses copy's
 *	  declarations to maintain compatibility with the copy command.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 */

#ifndef COPY_OPTIONS_H
#define COPY_OPTIONS_H

#include "libpq-fe.h"


/*
 * The documented syntax is:
 *	\copy tablename [(columnlist)] from|to filename [options]
 *	\copy ( select stmt ) to filename [options]
 *
 * where 'filename' can be one of the following:
 *	'<file path>' | PROGRAM '<command>' | stdin | stdout | pstdout | pstdout
 *
 * An undocumented fact is that you can still write BINARY before the
 * tablename; this is a hangover from the pre-7.3 syntax.  The options
 * syntax varies across backend versions, but we avoid all that mess
 * by just transmitting the stuff after the filename literally.
 *
 * table name can be double-quoted and can have a schema part.
 * column names can be double-quoted.
 * filename can be single-quoted like SQL literals.
 * command must be single-quoted like SQL literals.
 *
 * returns a malloc'ed structure with the options, or NULL on parsing error
 */
typedef struct copy_options
{
	char	   *before_tofrom;	/* COPY string before TO/FROM */
	char	   *after_tofrom;	/* COPY string after TO/FROM filename */
	char	   *file;			/* NULL = stdin/stdout */
	bool		program;		/* is 'file' a program to popen? */
	bool		psql_inout;		/* true = use psql stdin/stdout */
	bool		from;			/* true = FROM, false = TO */

	char	   *tableName;		/* table name to stage data to */
	char	   *columnList;		/* optional column list used in staging */
} copy_options;


/* Function declarations for parsing and freeing copy options */
copy_options * parse_slash_copy(const char *args);
void free_copy_options(copy_options * ptr);
copy_options * ParseStageOptions(copy_options *copyOptions);


#endif   /* COPY_OPTIONS_H */
