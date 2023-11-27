/*-------------------------------------------------------------------------
 *
 * objectaddress.c
 *    Parstrees almost always target a object that postgres can address by
 *    an ObjectAddress. Here we have a walker for parsetrees to find the
 *    address of the object targeted.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_extension_d.h"
#include "commands/extension.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"


/*
 * GetObjectAddressListFromParseTree returns the list of ObjectAddress of the main target of the parse
 * tree.
 */
List *
GetObjectAddressListFromParseTree(Node *parseTree, bool missing_ok, bool isPostprocess)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(parseTree);

	if (!ops->address)
	{
		ereport(ERROR, (errmsg("unsupported statement to get object address for")));
	}

	return ops->address(parseTree, missing_ok, isPostprocess);
}


List *
RenameAttributeStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return RenameTypeAttributeStmtObjectAddress(node, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter rename attribute statement to get "
								   "object address for")));
		}
	}
}


/*
 * CreateExtensionStmtObjectAddress finds the ObjectAddress for the extension described
 * by the CreateExtensionStmt. If missing_ok is false, then this function throws an
 * error if the extension does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
List *
CreateExtensionStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CreateExtensionStmt *stmt = castNode(CreateExtensionStmt, node);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	const char *extensionName = stmt->extname;

	Oid extensionoid = get_extension_oid(extensionName, missing_ok);

	/* if we couldn't find the extension, error if missing_ok is false */
	if (!missing_ok && extensionoid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

	return list_make1(address);
}
