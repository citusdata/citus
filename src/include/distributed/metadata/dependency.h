/*-------------------------------------------------------------------------
 *
 * dependency.c
 *    Functions to follow and record dependencies for objects to be
 *    created in the right order.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPENDENCY_H
#define CITUS_DEPENDENCY_H

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_depend.h"
#include "nodes/pg_list.h"

#include "distributed/errormessage.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

/*
 * ObjectDependencyEdge is a single directed edge of the object dependency graph:
 * prereq must be (re)created before dependent. It is produced by
 * OrderObjectAddressListInDependencyOrderWithEdges() so that a caller can build
 * an in-degree graph and schedule independent objects concurrently (metadata
 * sync connection pool) while still respecting real creation-order edges.
 *
 * Edges are direct (parent -> directly-followed dependency in the pg_depend
 * traversal), not transitive, and may reference objects that are not in the
 * ordered output list (e.g. dependencies that end up filtered out or created
 * out of band); such edges are simply ignored by callers that restrict the
 * graph to the set of objects they actually create.
 */
typedef struct ObjectDependencyEdge
{
	ObjectAddress prereq;
	ObjectAddress dependent;
} ObjectDependencyEdge;

extern List * GetUniqueDependenciesList(List *objectAddressesList);
extern List * GetDependenciesForObject(const ObjectAddress *target);
extern List * GetAllSupportedDependenciesForObject(const ObjectAddress *target);
extern List * GetAllDependenciesForObject(const ObjectAddress *target);
extern bool ErrorOrWarnIfAnyObjectHasUnsupportedDependency(List *objectAddresses);
extern DeferredErrorMessage * DeferErrorIfAnyObjectHasUnsupportedDependency(const List *
																			objectAddresses);
extern List * GetAllCitusDependedDependenciesForObject(const ObjectAddress *target);
extern List * OrderObjectAddressListInDependencyOrder(List *objectAddressList,
													  bool flushCaches);
extern List * OrderObjectAddressListInDependencyOrderWithEdges(List *objectAddressList,
															   bool flushCaches,
															   List **edgeList);
extern bool SupportedDependencyByCitus(const ObjectAddress *address);
extern List * GetPgDependTuplesForDependingObjects(Oid targetObjectClassId,
												   Oid targetObjectId);
extern List * GetDependingViews(Oid relationId);
extern Oid GetDependingView(Form_pg_depend pg_depend);
extern List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate,
												 bool flushCaches);

#endif /* CITUS_DEPENDENCY_H */
