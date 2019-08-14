/*-------------------------------------------------------------------------
 *
 * distobjectaddress.h
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

#define RowToDistObjectAddress(address, form) \
	do { \
		(addr).classId = (form).classid; \
		(addr).objectId = (form).objid; \
		(addr).classId = text_to_cstring(&((form).identifier)); \
	} while (0)


/*
 * DistObjectAddress is the citus equivalent of a postgres ObjectAddress. They both
 * uniquely address and object within postgres. The big reason for citus to represent this
 * differently is the portability between postres versions.
 *
 * Postgres addresses objects by their classId, objectId (and sub object id). The objectId
 * is an Oid that is not stable between postgres upgrades. Instead of referencing the
 * object's by their Oid citus identifies the objects by their qualified identifier.
 *
 * Mapping functions that can map between postgres and citus object addresses are provided
 * to easily work with them.
 */
typedef struct DistObjectAddress
{
	Oid classId;
	Oid objectId;
	const char *identifier;
} DistObjectAddress;


extern DistObjectAddress * getDistObjectAddressFromPg(const ObjectAddress *address);
extern ObjectAddress * getObjectAddresFromCitus(const DistObjectAddress *distAddress);
extern DistObjectAddress * makeDistObjectAddress(Oid classid, Oid objectid, const
												 char *identifier);
extern bool isObjectDistributedByAddress(const ObjectAddress *address);
extern bool isObjectDistributed(const DistObjectAddress *distAddress);
extern void recordObjectDistributedByAddress(const ObjectAddress *address);
extern void recordObjectDistributed(const DistObjectAddress *distAddress);
extern void dropObjectDistributedByAddress(const ObjectAddress *address);
extern void dropObjectDistributed(const DistObjectAddress *distAddress);

extern List * GetDistributedObjectAddressList(void);

#endif /* CITUS_CATALOG_DISTOBJECTADDRESS_H */
