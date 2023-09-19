/*-------------------------------------------------------------------------
 *
 * relation_utils.c
 *
 * This file contains functions similar to rel.h to perform useful
 * operations on Relation objects.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/relation_utils.h"

#if PG_VERSION_NUM >= PG_VERSION_16
#include "miscadmin.h"
#endif
#include "utils/lsyscache.h"
#include "utils/rel.h"


/*
 * RelationGetNamespaceName returns the relation's namespace name.
 */
char *
RelationGetNamespaceName(Relation relation)
{
	Oid namespaceId = RelationGetNamespace(relation);
	char *namespaceName = get_namespace_name(namespaceId);
	return namespaceName;
}


#if PG_VERSION_NUM >= PG_VERSION_16

/*
 * GetFilledPermissionInfo creates RTEPermissionInfo for a given RTE
 * and fills it with given data and returns this RTEPermissionInfo object.
 * Added this function since Postgres's addRTEPermissionInfo doesn't fill the data.
 *
 * Given data consists of relid, inh and requiredPerms
 * Took a quick look around Postgres, unless specified otherwise,
 * we are dealing with GetUserId().
 * Currently the following entries are filled like this:
 *      perminfo->checkAsUser = GetUserId();
 */
RTEPermissionInfo *
GetFilledPermissionInfo(Oid relid, bool inh, AclMode requiredPerms)
{
	RTEPermissionInfo *perminfo = makeNode(RTEPermissionInfo);
	perminfo->relid = relid;
	perminfo->inh = inh;
	perminfo->requiredPerms = requiredPerms;
	perminfo->checkAsUser = GetUserId();
	return perminfo;
}


#endif
