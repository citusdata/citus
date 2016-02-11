/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2015, PostgreSQL Global Development Group
 *
 * src/bin/psql/copy.h
 */
#ifndef COPY_H
#define COPY_H

#include "libpq-fe.h"

#include "copy_options.h"
#include "pqexpbuffer.h"


/* handler for \copy */
extern bool do_copy(const char *args);

/* lower level processors for copy in/out streams */

extern bool handleCopyOut(PGconn *conn, FILE *copystream,
			  PGresult **res);
extern bool handleCopyIn(PGconn *conn, FILE *copystream, bool isbinary,
			  PGresult **res, uint64 copySizeLimit);

/* Function declarations shared between copy and stage commands */
bool HandleCopyData(PGconn *connection, ExecStatusType copyStatus, 
					bool copyIsBinary, FILE *copyStream, uint64 copySizeLimit);
FILE * OpenCopyStream(const copy_options *options);
bool CloseCopyStream(const copy_options *options, FILE *copyStream);

#endif
