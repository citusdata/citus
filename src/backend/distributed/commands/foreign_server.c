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

#include "miscadmin.h"

#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"

static char * GetForeignServerAlterOwnerCommand(Oid serverId);
static Node * RecreateForeignServerStmt(Oid serverId);
static bool NameListHasDistributedServer(List *serverNames);
static List * GetObjectAddressByServerName(char *serverName, bool missing_ok);


/*
 * CreateForeignServerStmtObjectAddress finds the ObjectAddress for the server
 * that is created by given CreateForeignServerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
List *
CreateForeignServerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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
List *
AlterForeignServerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterForeignServerStmt *stmt = castNode(AlterForeignServerStmt, node);

	return GetObjectAddressByServerName(stmt->servername, missing_ok);
}


/*
 * PreprocessGrantOnForeignServerStmt is executed before the statement is applied to the
 * local postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on servers.
 */
List *
PreprocessGrantOnForeignServerStmt(Node *node, const char *queryString,
								   ProcessUtilityContext processUtilityContext)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_FOREIGN_SERVER);

	bool includesDistributedServer = NameListHasDistributedServer(stmt->objects);

	if (!includesDistributedServer)
	{
		return NIL;
	}

	if (list_length(stmt->objects) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot grant on distributed server with other servers"),
						errhint("Try granting on each object in separate commands")));
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	/*  the code-path only supports a single object */
	Assert(list_length(stmt->objects) == 1);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * RenameForeignServerStmtObjectAddress finds the ObjectAddress for the server that is
 * renamed by given RenmaeStmt. If missingOk is false and if the server does not exist,
 * then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
List *
RenameForeignServerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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
List *
AlterForeignServerOwnerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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
	const char *createCommand = DeparseTreeNode(stmt);
	const char *alterOwnerCommand = GetForeignServerAlterOwnerCommand(serverId);

	List *ddlCommands = list_make2((void *) createCommand,
								   (void *) alterOwnerCommand);

	return ddlCommands;
}


/*
 * GetForeignServerAlterOwnerCommand returns "ALTER SERVER .. OWNER TO .." statement
 * for the specified foreign server.
 */
static char *
GetForeignServerAlterOwnerCommand(Oid serverId)
{
	ForeignServer *server = GetForeignServer(serverId);
	Oid ownerId = server->owner;
	char *ownerName = GetUserNameFromId(ownerId, false);

	StringInfo alterCommand = makeStringInfo();

	appendStringInfo(alterCommand, "ALTER SERVER %s OWNER TO %s;",
					 quote_identifier(server->servername),
					 quote_identifier(ownerName));

	return alterCommand->data;
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
	foreach_declared_ptr(option, server->options)
	{
		DefElem *copyOption = makeDefElem(option->defname, option->arg, location);
		createStmt->options = lappend(createStmt->options, copyOption);
	}

	return (Node *) createStmt;
}


/*
 * NameListHasDistributedServer takes a namelist of servers and returns true if at least
 * one of them is distributed. Returns false otherwise.
 */
static bool
NameListHasDistributedServer(List *serverNames)
{
	String *serverValue = NULL;
	foreach_declared_ptr(serverValue, serverNames)
	{
		List *addresses = GetObjectAddressByServerName(strVal(serverValue), false);

		/*  the code-path only supports a single object */
		Assert(list_length(addresses) == 1);

		/* We have already asserted that we have exactly 1 address in the addresses. */
		ObjectAddress *address = linitial(addresses);

		if (IsAnyObjectDistributed(list_make1(address)))
		{
			return true;
		}
	}

	return false;
}


static List *
GetObjectAddressByServerName(char *serverName, bool missing_ok)
{
	ForeignServer *server = GetForeignServerByName(serverName, missing_ok);
	Oid serverOid = (server) ? server->serverid : InvalidOid;
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, ForeignServerRelationId, serverOid);

	return list_make1(address);
}
