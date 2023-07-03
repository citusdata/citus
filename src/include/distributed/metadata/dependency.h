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
#include "distributed/errormessage.h"
#include "nodes/pg_list.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

extern List * GetUniqueDependenciesList(List *objectAddressesList);
extern List * GetDependenciesForObject(const ObjectAddress *target);
extern List * GetAllSupportedDependenciesForObject(const ObjectAddress *target);
extern List * GetAllDependenciesForObject(const ObjectAddress *target);
extern bool ErrorOrWarnIfAnyObjectHasUnsupportedDependency(List *objectAddresses);
extern DeferredErrorMessage * DeferErrorIfAnyObjectHasUnsupportedDependency(const List *
																			objectAddresses);
extern List * GetAllCitusDependedDependenciesForObject(const ObjectAddress *target);
extern List * OrderObjectAddressListInDependencyOrder(List *objectAddressList);
extern bool SupportedDependencyByCitus(const ObjectAddress *address);
extern List * GetPgDependTuplesForDependingObjects(Oid targetObjectClassId,
												   Oid targetObjectId);
extern List * GetDependingViews(Oid relationId);
extern Oid GetDependingView(Form_pg_depend pg_depend);
extern List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate);

extern void InitTxDistObjectContextAndHash(void);
extern void AddToTxDistObjects(const ObjectAddress *objectAddress);
extern void ResetTxDistObjects(void);
extern bool HasDependencyToTxDistObject(const ObjectAddress *objectAddress);

#endif /* CITUS_DEPENDENCY_H */
