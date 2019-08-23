/*-------------------------------------------------------------------------
 *
 * distobject.h
 *	  Declarations for functions to work with pg_dist_object
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_METADATA_DISTOBJECT_H
#define CITUS_METADATA_DISTOBJECT_H

#include "postgres.h"

#include "catalog/objectaddress.h"


extern bool ObjectExsists(const ObjectAddress *address);
extern bool IsObjectDistributed(const ObjectAddress *address);
extern void MarkObjectDistributed(const ObjectAddress *distAddress);
extern void UnmarkObjectDistributed(const ObjectAddress *address);

extern List * GetDistributedObjectAddressList(void);

#endif /* CITUS_METADATA_DISTOBJECT_H */
