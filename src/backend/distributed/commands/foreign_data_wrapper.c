/*-------------------------------------------------------------------------
 *
 * foreign_data_wrapper.c
 *    Commands for FOREIGN DATA WRAPPER statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_foreign_data_wrapper.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"

static ObjectAddress GetObjectAddressByFDWName(char *FDWName, bool missing_ok);


/*
 * NameListHasFDWOwnedByDistributedExtension takes a namelist of FDWs and returns true
 * if at least one of them depends on a distributed extension. Returns false otherwise.
 */
bool
NameListHasFDWOwnedByDistributedExtension(List *FDWNames)
{
	String *FDWValue = NULL;
	foreach_ptr(FDWValue, FDWNames)
	{
		/* captures the extension address during lookup */
		ObjectAddress *extensionAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddress FDWAddress = GetObjectAddressByFDWName(strVal(FDWValue), false);

		ObjectAddress *copyFDWAddress = palloc0(sizeof(ObjectAddress));
		*copyFDWAddress = FDWAddress;
		if (IsAnyObjectAddressOwnedByExtension(list_make1(copyFDWAddress),
											   extensionAddress))
		{
			if (IsAnyObjectDistributed(list_make1(extensionAddress)))
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * GetObjectAddressByFDWName takes a FDW name and returns the object address.
 */
static ObjectAddress
GetObjectAddressByFDWName(char *FDWName, bool missing_ok)
{
	ForeignDataWrapper *FDW = GetForeignDataWrapperByName(FDWName, missing_ok);
	Oid FDWId = FDW->fdwid;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ForeignDataWrapperRelationId, FDWId);

	return address;
}


/*
 * GetPrivilegesForFDW takes a FDW object id and returns the privileges granted
 * on that FDW as a Acl object. Returns NULL if there is no privilege granted.
 */
Acl *
GetPrivilegesForFDW(Oid FDWOid)
{
	HeapTuple fdwtup = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(FDWOid));

	bool isNull = true;
	Datum aclDatum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, fdwtup,
									 Anum_pg_foreign_data_wrapper_fdwacl, &isNull);
	if (isNull)
	{
		ReleaseSysCache(fdwtup);
		return NULL;
	}

	Acl *aclEntry = DatumGetAclPCopy(aclDatum);

	ReleaseSysCache(fdwtup);

	return aclEntry;
}
