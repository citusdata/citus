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
#include "nodes/pg_list.h"

extern List * GetDependenciesForObject(const ObjectAddress *target);
extern List * OrderObjectAddressListInDependencyOrder(List *objectAddressList);
extern bool SupportedDependencyByCitus(const ObjectAddress *address);

#endif /* CITUS_DEPENDENCY_H */
