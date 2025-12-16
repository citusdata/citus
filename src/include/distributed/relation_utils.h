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

#include "parser/parse_relation.h"
#include "utils/relcache.h"

#include "pg_version_constants.h"

extern char * RelationGetNamespaceName(Relation relation);
extern RTEPermissionInfo * GetFilledPermissionInfo(Oid relid, bool inh,
												   AclMode requiredPerms);

#endif /* RELATION_UTILS_H */
