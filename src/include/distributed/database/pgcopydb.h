/*-------------------------------------------------------------------------
 *
 * pgcopydb.h
 *	  definition of pgcopydb functions
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGCOPYDB_H
#define PGCOPYDB_H

char * GetPgcopydbPath(void);
char * RunPgcopydbClone(char *sourceConnectionString, char *targetConnectionString,
						char *migrationName);

#endif
