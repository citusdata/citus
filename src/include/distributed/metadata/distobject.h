/*-------------------------------------------------------------------------
 *
 * distobject.h
 *	  Declarations for functions to work with pg_dist_object
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_METADATA_DISTOBJECT_H
#define CITUS_METADATA_DISTOBJECT_H

#include "postgres.h"

#include "catalog/objectaddress.h"

#define INVALID_DISTRIBUTION_ARGUMENT_INDEX -1
#define NO_FORCE_PUSHDOWN 0

extern bool ObjectExists(const ObjectAddress *address);
extern bool CitusExtensionObject(const ObjectAddress *objectAddress);
extern bool IsObjectDistributed(const ObjectAddress *address);
extern bool ClusterHasDistributedFunctionWithDistArgument(void);
extern void MarkObjectDistributed(const ObjectAddress *distAddress);
extern void MarkObjectDistributedViaSuperUser(const ObjectAddress *distAddress);
extern void MarkObjectDistributedLocally(const ObjectAddress *distAddress);
extern void UnmarkObjectDistributed(const ObjectAddress *address);
extern bool IsTableOwnedByExtension(Oid relationId);
extern bool IsObjectAddressOwnedByExtension(const ObjectAddress *target,
											ObjectAddress *extensionAddress);
extern ObjectAddress PgGetObjectAddress(char *ttype, ArrayType *namearr,
										ArrayType *argsarr);
extern List * GetDistributedObjectAddressList(void);
extern RoleSpec * GetRoleSpecObjectForUser(Oid roleOid);
extern void UpdateDistributedObjectColocationId(uint32 oldColocationId, uint32
												newColocationId);
#endif /* CITUS_METADATA_DISTOBJECT_H */
