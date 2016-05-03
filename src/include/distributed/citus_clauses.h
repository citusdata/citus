/*-------------------------------------------------------------------------
 *
 * citus_clauses.h
 * 	Routines roughly equivalent to postgres' util/clauses. 
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NODEFUNCS_H
#define CITUS_NODEFUNCS_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

extern void ExecuteFunctions(Query *query);

#endif /* CITUS_NODEFUNCS_H */
