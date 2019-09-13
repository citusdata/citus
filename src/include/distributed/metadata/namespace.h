/*-------------------------------------------------------------------------
 *
 * namespace.h
 *    Helper functions for citus to work with postgres namespaces/schemas
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NAMESPACE_H
#define CITUS_NAMESPACE_H

#include "postgres.h"

#include "nodes/primnodes.h"

extern List * MakeNameListFromRangeVar(const RangeVar *rel);

#endif /*CITUS_NAMESPACE_H */
