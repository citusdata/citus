/*-------------------------------------------------------------------------
 *
 * citus_read.c
 *	  Citus version of postgres' read.c, using a different state variable for
 *	  citus_pg_strtok.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * NOTES
 *    Unfortunately we have to copy this file as the state variable for
 *    pg_strtok is not externally accessible. That prevents creating a a
 *    version of stringToNode() that calls CitusNodeRead() instead of
 *    nodeRead().  Luckily these functions seldomly change.
 *
 *    Keep as closely aligned with the upstream version as possible.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "nodes/pg_list.h"
#include "nodes/readfuncs.h"
#include "distributed/citus_nodefuncs.h"
#include "nodes/value.h"


/*
 * For 9.6 onwards, we use 9.6's extensible node system, thus there's no need
 * to copy various routines anymore. In that case, replace these functions
 * with plain wrappers.
 */
void *
CitusStringToNode(char *str)
{
	return stringToNode(str);
}


char *
citus_pg_strtok(int *length)
{
	return pg_strtok(length);
}


void *
CitusNodeRead(char *token, int tok_len)
{
	return nodeRead(token, tok_len);
}
