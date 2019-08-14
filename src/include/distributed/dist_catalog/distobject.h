/*-------------------------------------------------------------------------
 *
 * distobject.h
 *	  Declarations for mapping between postgres' ObjectAddress and citus'
 *	  DistObjectAddress.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CATALOG_DISTOBJECTADDRESS_H
#define CITUS_CATALOG_DISTOBJECTADDRESS_H

#include "postgres.h"

#include "catalog/objectaddress.h"


extern ObjectAddress * getObjectAddressByIdentifier(Oid classId, const char *identifier);
extern bool isObjectDistributed(const ObjectAddress *address);
extern void recordObjectDistributed(const ObjectAddress *address);
extern void dropObjectDistributed(const ObjectAddress *address);

extern List * GetDistributedObjectAddressList(void);

#endif /* CITUS_CATALOG_DISTOBJECTADDRESS_H */
