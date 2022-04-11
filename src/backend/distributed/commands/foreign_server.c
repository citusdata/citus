/*-------------------------------------------------------------------------
 *
 * foreign_server.c
 *    Commands for FOREIGN SERVER statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_foreign_server.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

static Node * RecreateForeignServerStmt(Oid serverId);
static ObjectAddress GetObjectAddressByServerName(char *serverName, bool missing_ok);


/*
 * CreateForeignServerStmtObjectAddress finds the ObjectAddress for the server
 * that is created by given CreateForeignServerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
CreateForeignServerStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateForeignServerStmt *stmt = castNode(CreateForeignServerStmt, node);

	return GetObjectAddressByServerName(stmt->servername, missing_ok);
}


/*
 * AlterForeignServerStmtObjectAddress finds the ObjectAddress for the server that is
 * changed by given AlterForeignServerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
AlterForeignServerStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterForeignServerStmt *stmt = castNode(AlterForeignServerStmt, node);

	return GetObjectAddressByServerName(stmt->servername, missing_ok);
}


/*
 * RenameForeignServerStmtObjectAddress finds the ObjectAddress for the server that is
 * renamed by given RenmaeStmt. If missingOk is false and if the server does not exist,
 * then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
RenameForeignServerStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_FOREIGN_SERVER);

	return GetObjectAddressByServerName(strVal(stmt->object), missing_ok);
}


/*
 * AlterForeignServerOwnerStmtObjectAddress finds the ObjectAddress for the server
 * given in AlterOwnerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
AlterForeignServerOwnerStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	char *serverName = strVal(stmt->object);

	return GetObjectAddressByServerName(serverName, missing_ok);
}


/*
 * GetForeignServerCreateDDLCommand returns a list that includes the CREATE SERVER
 * command that would recreate the given server on a new node.
 */
List *
GetForeignServerCreateDDLCommand(Oid serverId)
{
	/* generate a statement for creation of the server in "if not exists" construct */
	Node *stmt = RecreateForeignServerStmt(serverId);

	/* capture ddl command for the create statement */
	const char *ddlCommand = DeparseTreeNode(stmt);

	List *ddlCommands = list_make1((void *) ddlCommand);

	return ddlCommands;
}


/*
 * RecreateForeignServerStmt returns a parsetree for a CREATE SERVER statement
 * that would recreate the given server on a new node.
 */
static Node *
RecreateForeignServerStmt(Oid serverId)
{
	ForeignServer *server = GetForeignServer(serverId);

	CreateForeignServerStmt *createStmt = makeNode(CreateForeignServerStmt);

	/* set server name and if_not_exists fields */
	createStmt->servername = pstrdup(server->servername);
	createStmt->if_not_exists = true;

	/* set foreign data wrapper */
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	createStmt->fdwname = pstrdup(fdw->fdwname);

	/* set all fields using the existing server */
	if (server->servertype != NULL)
	{
		createStmt->servertype = pstrdup(server->servertype);
	}

	if (server->serverversion != NULL)
	{
		createStmt->version = pstrdup(server->serverversion);
	}

	createStmt->options = NIL;

	int location = -1;
	DefElem *option = NULL;
	foreach_ptr(option, server->options)
	{
		DefElem *copyOption = makeDefElem(option->defname, option->arg, location);
		createStmt->options = lappend(createStmt->options, copyOption);
	}

	return (Node *) createStmt;
}


static ObjectAddress
GetObjectAddressByServerName(char *serverName, bool missing_ok)
{
	ForeignServer *server = GetForeignServerByName(serverName, missing_ok);
	Oid serverOid = server->serverid;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ForeignServerRelationId, serverOid);

	return address;
}
