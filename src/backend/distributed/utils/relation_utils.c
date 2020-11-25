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
