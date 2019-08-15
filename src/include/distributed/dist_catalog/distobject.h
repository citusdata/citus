/*-------------------------------------------------------------------------
 *
 * distobject.h
 *	  Declarations for functions to work with pg_dist_object
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CATALOG_DISTOBJECT_H
#define CITUS_CATALOG_DISTOBJECT_H

#include "postgres.h"

#include "catalog/objectaddress.h"


extern ObjectAddress * getObjectAddressByIdentifier(Oid classId, const char *identifier);
extern bool isObjectDistributed(const ObjectAddress *address);
extern void markObjectDistributed(const ObjectAddress *distAddress);
extern void unmarkObjectDistributed(const ObjectAddress *address);

extern List * GetDistributedObjectAddressList(void);

#endif /* CITUS_CATALOG_DISTOBJECT_H */
