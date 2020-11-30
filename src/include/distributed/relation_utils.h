/*-------------------------------------------------------------------------
 *
 * relation_utils.h
 *   Utilities related to Relation objects.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef RELATION_UTILS_H
#define RELATION_UTILS_H

#include "postgres.h"

#include "utils/relcache.h"

extern char * RelationGetNamespaceName(Relation relation);

#endif /* RELATION_UTILS_H */
