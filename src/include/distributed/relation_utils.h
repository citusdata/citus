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

#include "pg_version_constants.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "parser/parse_relation.h"
#endif
#include "utils/relcache.h"

extern char * RelationGetNamespaceName(Relation relation);
#if PG_VERSION_NUM >= PG_VERSION_16
extern RTEPermissionInfo * GetFilledPermissionInfo(Oid relid, bool inh,
												   AclMode requiredPerms);
#endif

#endif /* RELATION_UTILS_H */
