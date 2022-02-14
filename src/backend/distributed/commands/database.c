/*-------------------------------------------------------------------------
 *
 * database.c
 *    Commands to interact with the database object in a distributed
 *    environment.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_transaction.h"

static AlterOwnerStmt * RecreateAlterDatabaseOwnerStmt(Oid databaseOid);
static Oid get_database_owner(Oid db_oid);

/* controlled via GUC */
bool EnableAlterDatabaseOwner = false;


/*
 * PreprocessAlterDatabaseOwnerStmt is called during the utility hook before the alter
 * command is applied locally on the coordinator. This will verify if the command needs to
 * be propagated to the workers and if so prepares a list of ddl commands to execute.
 */
List *
PreprocessAlterDatabaseOwnerStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	if (!EnableAlterDatabaseOwner)
	{
		/* don't propagate if GUC is turned off */
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialMode(OBJECT_DATABASE);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDatabaseOwnerStmt is called during the utility hook after the alter
 * database command has been applied locally.
 *
 * Its main purpose is to propagate the newly formed dependencies onto the nodes before
 * applying the change of owner of the databse. This ensures, for systems that have role
 * management, that the roles will be created before applying the alter owner command.
 */
List *
PostprocessAlterDatabaseOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	if (!EnableAlterDatabaseOwner)
	{
		/* don't propagate if GUC is turned off */
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&typeAddress);
	return NIL;
}


/*
 * AlterDatabaseOwnerObjectAddress returns the ObjectAddress of the database that is the
 * object of the AlterOwnerStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterDatabaseOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	Oid databaseOid = get_database_oid(strVal((Value *) stmt->object), missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, DatabaseRelationId, databaseOid);

	return address;
}


/*
 * DatabaseOwnerDDLCommands returns a list of sql statements to idempotently apply a
 * change of the database owner on the workers so that the database is owned by the same
 * user on all nodes in the cluster.
 */
List *
DatabaseOwnerDDLCommands(const ObjectAddress *address)
{
	Node *stmt = (Node *) RecreateAlterDatabaseOwnerStmt(address->objectId);
	return list_make1(DeparseTreeNode(stmt));
}


/*
 * RecreateAlterDatabaseOwnerStmt creates an AlterOwnerStmt that represents the operation
 * of changing the owner of the database to its current owner.
 */
static AlterOwnerStmt *
RecreateAlterDatabaseOwnerStmt(Oid databaseOid)
{
	AlterOwnerStmt *stmt = makeNode(AlterOwnerStmt);

	stmt->objectType = OBJECT_DATABASE;
	stmt->object = (Node *) makeString(get_database_name(databaseOid));

	Oid ownerOid = get_database_owner(databaseOid);
	stmt->newowner = makeNode(RoleSpec);
	stmt->newowner->roletype = ROLESPEC_CSTRING;
	stmt->newowner->rolename = GetUserNameFromId(ownerOid, false);

	return stmt;
}


/*
 * get_database_owner returns the Oid of the role owning the database
 */
static Oid
get_database_owner(Oid db_oid)
{
	HeapTuple tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(db_oid));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
						errmsg("database with OID %u does not exist", db_oid)));
	}

	Oid dba = ((Form_pg_database) GETSTRUCT(tuple))->datdba;

	ReleaseSysCache(tuple);

	return dba;
}
