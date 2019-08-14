/*-------------------------------------------------------------------------
 *
 * dependency.c
 *    Functions to follow and record dependencies for objects to be
 *    created in the right order.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPENDENCY_H
#define CITUS_DEPENDENCY_H

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

extern void GetDependenciesForObject(const ObjectAddress *target, List **dependencyList);
extern List * OrderObjectAddressListInDependencyOrder(List *objectAddressList);

#endif /* CITUS_DEPENDENCY_H */
