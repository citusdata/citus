/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2015, PostgreSQL Global Development Group
 *
 * src/bin/psql/copy.c
 */
#include "postgres_fe.h"
#include "copy.h"

#include <signal.h>
#include <sys/stat.h>
#ifndef WIN32
#include <unistd.h>				/* for isatty */
#else
#include <io.h>					/* I think */
#endif

#include "libpq-fe.h"
#include "pqexpbuffer.h"
#include "dumputils.h"

#include "settings.h"
#include "common.h"
#include "prompt.h"


/*
 * Execute a \copy command (frontend copy). We have to open a file (or execute
 * a command), then submit a COPY query to the backend and either feed it data
 * from the file or route its response into the file.
 */
bool
do_copy(const char *args)
{
	copy_options *options = NULL;
	PQExpBufferData query = { NULL, 0, 0 };
	FILE *copystream = NULL;
	bool success = false;
	bool fileClosed = false;

	/* parse options */
	options = parse_slash_copy(args);

	if (!options)
		return false;

	/* open file stream to copy data into or out of */
	copystream = OpenCopyStream(options);
	if (copystream == NULL)
	{
		free_copy_options(options);

		return false;
	}

	/* build the command we will send to the backend */
	initPQExpBuffer(&query);
	printfPQExpBuffer(&query, "COPY ");
	appendPQExpBufferStr(&query, options->before_tofrom);
	if (options->from)
		appendPQExpBufferStr(&query, " FROM STDIN ");
	else
		appendPQExpBufferStr(&query, " TO STDOUT ");
	if (options->after_tofrom)
		appendPQExpBufferStr(&query, options->after_tofrom);

	/* run it like a user command, but with copystream as data source/sink */
	pset.copyStream = copystream;
	success = SendQuery(query.data);
	pset.copyStream = NULL;
	termPQExpBuffer(&query);

	/* close file stream */
	fileClosed = CloseCopyStream(options, copystream);
	if (!fileClosed)
	{
		success = false;
	}

	free_copy_options(options);
	return success;
}


/*
 * HandleCopyData executes client-side copy data protocols by dispatching the
 * call to the appropriate copy protocol function. On successful execution of
 * the protocol, the function returns true. Otherwise, the function returns
 * false.
 *
 * Please note that we refactored this function from a previous version (v9.1)
 * of PostgreSQL so that copy.c and stage.c could share the same code path. Now
 * that do_copy uses SendQuery(), we should move or re-refactor this function.
 */
bool
HandleCopyData(PGconn *connection, ExecStatusType copyStatus, bool copyIsBinary,
			   FILE *copyStream, uint64 copySizeLimit)
{
	ExecStatusType drainStatus = 0;
	PGresult *drainResult = NULL;
	bool copyOK = true;

	if (copyStatus == PGRES_COPY_OUT)
	{
		SetCancelConn();
		copyOK = handleCopyOut(connection, copyStream, &drainResult);
		ResetCancelConn();
	}
	else if (copyStatus == PGRES_COPY_IN)
	{
		SetCancelConn();
		copyOK = handleCopyIn(connection, copyStream, copyIsBinary,
							  &drainResult, copySizeLimit);
		ResetCancelConn();
	}
	else if (copyStatus == PGRES_BAD_RESPONSE ||
			 copyStatus == PGRES_NONFATAL_ERROR ||
			 copyStatus == PGRES_FATAL_ERROR)
	{
		psql_error("\\copy: %s", PQerrorMessage(connection));
		copyOK = false;
	}
	else
	{
		psql_error("\\copy: unexpected response (%d)\n", copyStatus);
		copyOK = false;
	}

	PQclear(drainResult);

	/*
	 * Make sure we drain all results from libpq. Otherwise, the connection may
	 * still be in ASYNC_BUSY state, leading to false readings in get_prompt().
	 */
	drainResult = PQgetResult(connection);
	while (drainResult != NULL)
	{
		copyOK = false;

		drainStatus = PQresultStatus(drainResult);
		psql_error("\\copy: unexpected response (%d)\n", drainStatus);

		/* if we are still in COPY IN state, try to get out of it */
		if (drainStatus == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection, _("trying to exit copy mode"));
		}

		PQclear(drainResult);
		drainResult = PQgetResult(connection);
	}

	return copyOK;	
}


/* Opens input or output stream to be used during copy command. */
FILE *
OpenCopyStream(const copy_options *options)
{
	FILE *copyStream = NULL;

	/* prepare to read or write the target file */
	if (options->file && !options->program)
		canonicalize_path(options->file);

	if (options->from)
	{
		if (options->file)
		{
			if (options->program)
			{
				fflush(stdout);
				fflush(stderr);
				errno = 0;
				copyStream = popen(options->file, PG_BINARY_R);
			}
			else
				copyStream = fopen(options->file, PG_BINARY_R);
		}
		else if (!options->psql_inout)
			copyStream = pset.cur_cmd_source;
		else
			copyStream = stdin;
	}
	else
	{
		if (options->file)
		{
			if (options->program)
			{
				fflush(stdout);
				fflush(stderr);
				errno = 0;
#ifndef WIN32
				pqsignal(SIGPIPE, SIG_IGN);
#endif
				copyStream = popen(options->file, PG_BINARY_W);
			}
			else
				copyStream = fopen(options->file, PG_BINARY_W);
		}
		else if (!options->psql_inout)
			copyStream = pset.queryFout;
		else
			copyStream = stdout;
	}

	if (!copyStream)
	{
		if (options->program)
			psql_error("could not execute command \"%s\": %s\n",
					   options->file, strerror(errno));
		else
			psql_error("%s: %s\n",
					   options->file, strerror(errno));

		return NULL;
	}

	if (!options->program)
	{
		struct stat st;
		int			result;

		/* make sure the specified file is not a directory */
		if ((result = fstat(fileno(copyStream), &st)) < 0)
			psql_error("could not stat file \"%s\": %s\n",
					   options->file, strerror(errno));

		if (result == 0 && S_ISDIR(st.st_mode))
			psql_error("%s: cannot copy from/to a directory\n",
					   options->file);

		if (result < 0 || S_ISDIR(st.st_mode))
		{
			fclose(copyStream);

			return NULL;
		}
	}

	return copyStream;
}


/* Closes file stream used during copy command, if any. */
bool
CloseCopyStream(const copy_options *options, FILE *copyStream)
{
	bool success = true;

	if (options->file != NULL)
	{
		if (options->program)
		{
			int			pclose_rc = pclose(copyStream);

			if (pclose_rc != 0)
			{
				if (pclose_rc < 0)
					psql_error("could not close pipe to external command: %s\n",
							   strerror(errno));
				else
				{
					char	   *reason = wait_result_to_str(pclose_rc);

					psql_error("%s: %s\n", options->file,
							   reason ? reason : "");
					if (reason)
						free(reason);
				}
				success = false;
			}
#ifndef WIN32
			pqsignal(SIGPIPE, SIG_DFL);
#endif
		}
		else
		{
			if (fclose(copyStream) != 0)
			{
				psql_error("%s: %s\n", options->file, strerror(errno));
				success = false;
			}
		}
	}

	return success;
}


/*
 * Functions for handling COPY IN/OUT data transfer.
 *
 * If you want to use COPY TO STDOUT/FROM STDIN in your application,
 * this is the code to steal ;)
 */

/*
 * handleCopyOut
 * receives data as a result of a COPY ... TO STDOUT command
 *
 * conn should be a database connection that you just issued COPY TO on
 * and got back a PGRES_COPY_OUT result.
 * copystream is the file stream for the data to go to.
 * The final status for the COPY is returned into *res (but note
 * we already reported the error, if it's not a success result).
 *
 * result is true if successful, false if not.
 */
bool
handleCopyOut(PGconn *conn, FILE *copystream, PGresult **res)
{
	bool		OK = true;
	char	   *buf;
	int			ret;

	for (;;)
	{
		ret = PQgetCopyData(conn, &buf, 0);

		if (ret < 0)
			break;				/* done or server/connection error */

		if (buf)
		{
			if (OK && fwrite(buf, 1, ret, copystream) != ret)
			{
				psql_error("could not write COPY data: %s\n",
						   strerror(errno));
				/* complain only once, keep reading data from server */
				OK = false;
			}
			PQfreemem(buf);
		}
	}

	if (OK && fflush(copystream))
	{
		psql_error("could not write COPY data: %s\n",
				   strerror(errno));
		OK = false;
	}

	if (ret == -2)
	{
		psql_error("COPY data transfer failed: %s", PQerrorMessage(conn));
		OK = false;
	}

	/*
	 * Check command status and return to normal libpq state.
	 *
	 * If for some reason libpq is still reporting PGRES_COPY_OUT state, we
	 * would like to forcibly exit that state, since our caller would be
	 * unable to distinguish that situation from reaching the next COPY in a
	 * command string that happened to contain two consecutive COPY TO STDOUT
	 * commands.  However, libpq provides no API for doing that, and in
	 * principle it's a libpq bug anyway if PQgetCopyData() returns -1 or -2
	 * but hasn't exited COPY_OUT state internally.  So we ignore the
	 * possibility here.
	 */
	*res = PQgetResult(conn);
	if (PQresultStatus(*res) != PGRES_COMMAND_OK)
	{
		psql_error("%s", PQerrorMessage(conn));
		OK = false;
	}

	return OK;
}

/*
 * handleCopyIn
 * sends data to complete a COPY ... FROM STDIN command
 *
 * conn should be a database connection that you just issued COPY FROM on
 * and got back a PGRES_COPY_IN result.
 * copystream is the file stream to read the data from.
 * isbinary can be set from PQbinaryTuples().
 * The final status for the COPY is returned into *res (but note
 * we already reported the error, if it's not a success result).
 *
 * result is true if successful, false if not.
 */

/* read chunk size for COPY IN - size set to double that of Hadoop's default */
#define COPYBUFSIZ 32768

bool
handleCopyIn(PGconn *conn, FILE *copystream, bool isbinary,
			 PGresult **res, uint64 copySizeLimit)
{
	bool		OK;
	const char *prompt;
	char		buf[COPYBUFSIZ];
	uint64		bytesCopied = 0;

	/*
	 * Establish longjmp destination for exiting from wait-for-input. (This is
	 * only effective while sigint_interrupt_enabled is TRUE.)
	 */
	if (sigsetjmp(sigint_interrupt_jmp, 1) != 0)
	{
		/* got here with longjmp */

		/* Terminate data transfer */
		PQputCopyEnd(conn,
					 (PQprotocolVersion(conn) < 3) ? NULL :
					 _("canceled by user"));

		OK = false;
		goto copyin_cleanup;
	}

	/* Prompt if interactive input */
	if (isatty(fileno(copystream)))
	{
		if (!pset.quiet)
			puts(_("Enter data to be copied followed by a newline.\n"
				   "End with a backslash and a period on a line by itself."));
		prompt = get_prompt(PROMPT_COPY);
	}
	else
		prompt = NULL;

	OK = true;

	if (isbinary)
	{
		/* interactive input probably silly, but give one prompt anyway */
		if (prompt)
		{
			fputs(prompt, stdout);
			fflush(stdout);
		}

		for (;;)
		{
			int			buflen;

			/* enable longjmp while waiting for input */
			sigint_interrupt_enabled = true;

			buflen = fread(buf, 1, COPYBUFSIZ, copystream);

			sigint_interrupt_enabled = false;

			if (buflen <= 0)
				break;

			if (PQputCopyData(conn, buf, buflen) <= 0)
			{
				OK = false;
				break;
			}

			/* if size limit is set, copy at most that many bytes*/
			bytesCopied += buflen;
			if (copySizeLimit > 0 && bytesCopied >= copySizeLimit)
			{
				break;
			}
		}
	}
	else
	{
		bool		copydone = false;

		while (!copydone)
		{						/* for each input line ... */
			bool		firstload;
			bool		linedone;

			if (prompt)
			{
				fputs(prompt, stdout);
				fflush(stdout);
			}

			firstload = true;
			linedone = false;

			while (!linedone)
			{					/* for each bufferload in line ... */
				int			linelen = 0;
				char	   *fgresult;

				/* enable longjmp while waiting for input */
				sigint_interrupt_enabled = true;

				fgresult = fgets(buf, sizeof(buf), copystream);

				sigint_interrupt_enabled = false;

				if (!fgresult)
				{
					copydone = true;
					break;
				}

				linelen = strlen(buf);

				/* current line is done? */
				if (linelen > 0 && buf[linelen - 1] == '\n')
					linedone = true;

				/* check for EOF marker, but not on a partial line */
				if (firstload)
				{
					/*
					 * This code erroneously assumes '\.' on a line alone
					 * inside a quoted CSV string terminates the \copy.
					 * http://www.postgresql.org/message-id/E1TdNVQ-0001ju-GO@w
					 * rigleys.postgresql.org
					 */
					if (strcmp(buf, "\\.\n") == 0 ||
						strcmp(buf, "\\.\r\n") == 0)
					{
						copydone = true;
						break;
					}

					firstload = false;
				}

				if (PQputCopyData(conn, buf, linelen) <= 0)
				{
					OK = false;
					copydone = true;
					break;
				}
				else
				{
					bytesCopied += linelen;
				}
			}

			if (copystream == pset.cur_cmd_source)
				pset.lineno++;

			/* if size limit is set, copy at most that many bytes */
			if (copySizeLimit > 0 && bytesCopied >= copySizeLimit)
			{
				break;
			}
		}
	}

	/* Check for read error */
	if (ferror(copystream))
		OK = false;

	/*
	 * Terminate data transfer.  We can't send an error message if we're using
	 * protocol version 2.
	 */
	if (PQputCopyEnd(conn,
					 (OK || PQprotocolVersion(conn) < 3) ? NULL :
					 _("aborted because of read failure")) <= 0)
		OK = false;

copyin_cleanup:

	/*
	 * Check command status and return to normal libpq state.
	 *
	 * We do not want to return with the status still PGRES_COPY_IN: our
	 * caller would be unable to distinguish that situation from reaching the
	 * next COPY in a command string that happened to contain two consecutive
	 * COPY FROM STDIN commands.  We keep trying PQputCopyEnd() in the hope
	 * it'll work eventually.  (What's actually likely to happen is that in
	 * attempting to flush the data, libpq will eventually realize that the
	 * connection is lost.  But that's fine; it will get us out of COPY_IN
	 * state, which is what we need.)
	 */
	while (*res = PQgetResult(conn), PQresultStatus(*res) == PGRES_COPY_IN)
	{
		OK = false;
		PQclear(*res);
		/* We can't send an error message if we're using protocol version 2 */
		PQputCopyEnd(conn,
					 (PQprotocolVersion(conn) < 3) ? NULL :
					 _("trying to exit copy mode"));
	}
	if (PQresultStatus(*res) != PGRES_COMMAND_OK)
	{
		psql_error("%s", PQerrorMessage(conn));
		OK = false;
	}

	return OK;
}
