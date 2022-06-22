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
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/syscache.h"

static bool NameListHasFDWOwnedByDistributedExtension(List *FDWNames);
static ObjectAddress GetObjectAddressByFDWName(char *FDWName, bool missing_ok);


/*
 * PreprocessGrantOnFDWStmt is executed before the statement is applied to the
 * local postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on foreign data wrappers.
 */
List *
PreprocessGrantOnFDWStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_FDW);

	if (!NameListHasFDWOwnedByDistributedExtension(stmt->objects))
	{
		/*
		 * We propagate granted privileges on a FDW only if it belongs to a distributed
		 * extension. For now, we skip for custom FDWs, as most of the users prefer
		 * extension FDWs.
		 */
		return NIL;
	}

	if (list_length(stmt->objects) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot grant on FDW with other FDWs"),
						errhint("Try granting on each object in separate commands")));
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	Assert(list_length(stmt->objects) == 1);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * NameListHasFDWOwnedByDistributedExtension takes a namelist of FDWs and returns true
 * if at least one of them depends on a distributed extension. Returns false otherwise.
 */
static bool
NameListHasFDWOwnedByDistributedExtension(List *FDWNames)
{
	String *FDWValue = NULL;
	foreach_ptr(FDWValue, FDWNames)
	{
		/* captures the extension address during lookup */
		ObjectAddress extensionAddress = { 0 };
		ObjectAddress FDWAddress = GetObjectAddressByFDWName(strVal(FDWValue), false);

		if (IsObjectAddressOwnedByExtension(&FDWAddress, &extensionAddress))
		{
			if (IsObjectDistributed(&extensionAddress))
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
