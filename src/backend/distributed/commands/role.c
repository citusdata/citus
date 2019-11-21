/*-------------------------------------------------------------------------
 *
 * role.c
 *    Commands for ALTER ROLE statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/master_protocol.h"
#include "distributed/worker_transaction.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static const char * ExtractEncryptedPassword(Oid roleOid);
static const char * CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt);
static DefElem * makeDefElemInt(char *name, int value);

/* controlled via GUC */
bool EnableAlterRolePropagation = false;

/*
 * ProcessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement. We need to do it this way because we need to use the encrypted
 * password, which is, in some cases, created at standardProcessUtility.
 */
List *
ProcessAlterRoleStmt(AlterRoleStmt *stmt, const char *queryString)
{
	ListCell *optionCell = NULL;
	List *commands = NIL;

	if (!EnableAlterRolePropagation || !IsCoordinator())
	{
		return NIL;
	}

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	foreach(optionCell, stmt->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (strcasecmp(option->defname, "password") == 0)
		{
			Oid roleOid = get_rolespec_oid(stmt->role, true);
			const char *encryptedPassword = ExtractEncryptedPassword(roleOid);

			if (encryptedPassword != NULL)
			{
				Value *encryptedPasswordValue = makeString((char *) encryptedPassword);
				option->arg = (Node *) encryptedPasswordValue;
			}
			else
			{
				option->arg = NULL;
			}

			break;
		}
	}
	commands = list_make1((void *) CreateAlterRoleIfExistsCommand(stmt));

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * CreateAlterRoleIfExistsCommand creates ALTER ROLE command, from the alter role node
 *  using the alter_role_if_exists() UDF.
 */
static const char *
CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt)
{
	StringInfoData alterRoleQueryBuffer = { 0 };
	const char *roleName = RoleSpecString(stmt->role, false);
	const char *alterRoleQuery = DeparseTreeNode((Node *) stmt);

	initStringInfo(&alterRoleQueryBuffer);
	appendStringInfo(&alterRoleQueryBuffer,
					 "SELECT alter_role_if_exists(%s, %s)",
					 quote_literal_cstr(roleName),
					 quote_literal_cstr(alterRoleQuery));

	return alterRoleQueryBuffer.data;
}


/*
 * ExtractEncryptedPassword extracts the encrypted password of a role. The function
 * gets the password from the pg_authid table.
 */
static const char *
ExtractEncryptedPassword(Oid roleOid)
{
	Relation pgAuthId = heap_open(AuthIdRelationId, AccessShareLock);
	TupleDesc pgAuthIdDescription = RelationGetDescr(pgAuthId);
	HeapTuple tuple = SearchSysCache1(AUTHOID, roleOid);
	bool isNull = true;
	Datum passwordDatum;

	if (!HeapTupleIsValid(tuple))
	{
		return NULL;
	}

	passwordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
								 pgAuthIdDescription, &isNull);

	heap_close(pgAuthId, AccessShareLock);
	ReleaseSysCache(tuple);

	if (isNull)
	{
		return NULL;
	}

	return pstrdup(TextDatumGetCString(passwordDatum));
}


/*
 * GenerateAlterRoleIfExistsCommand generate ALTER ROLE command that copies a role from
 * the pg_authid table.
 */
static const char *
GenerateAlterRoleIfExistsCommand(HeapTuple tuple, TupleDesc pgAuthIdDescription)
{
	char *rolPassword = "";
	char *rolValidUntil = "infinity";
	Datum rolValidUntilDatum;
	Datum rolPasswordDatum;
	bool isNull = true;
	Form_pg_authid role = ((Form_pg_authid) GETSTRUCT(tuple));
	AlterRoleStmt *stmt = makeNode(AlterRoleStmt);
	const char *rolename = NameStr(role->rolname);

	stmt->role = makeNode(RoleSpec);
	stmt->role->roletype = ROLESPEC_CSTRING;
	stmt->role->location = -1;
	stmt->role->rolename = pstrdup(rolename);
	stmt->action = 1;
	stmt->options = NIL;

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("superuser", role->rolsuper));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("createdb", role->rolcreatedb));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("createrole", role->rolcreaterole));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("inherit", role->rolinherit));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("canlogin", role->rolcanlogin));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("isreplication", role->rolreplication));

	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("bypassrls", role->rolbypassrls));


	stmt->options =
		lappend(stmt->options,
				makeDefElemInt("connectionlimit", role->rolconnlimit));


	rolPasswordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
									pgAuthIdDescription, &isNull);
	if (!isNull)
	{
		rolPassword = pstrdup(TextDatumGetCString(rolPasswordDatum));
		stmt->options = lappend(stmt->options, makeDefElem("password",
														   (Node *) makeString(
															   rolPassword),
														   -1));
	}
	else
	{
		stmt->options = lappend(stmt->options, makeDefElem("password", NULL, -1));
	}

	rolValidUntilDatum = heap_getattr(tuple, Anum_pg_authid_rolvaliduntil,
									  pgAuthIdDescription, &isNull);
	if (!isNull)
	{
		rolValidUntil = pstrdup((char *) timestamptz_to_str(rolValidUntilDatum));
	}

	stmt->options = lappend(stmt->options, makeDefElem("validUntil",
													   (Node *) makeString(rolValidUntil),
													   -1));

	return CreateAlterRoleIfExistsCommand(stmt);
}


/*
 * GenerateAlterRoleIfExistsCommandAllRoles creates ALTER ROLE commands
 * that copies all roles from the pg_authid table.
 */
List *
GenerateAlterRoleIfExistsCommandAllRoles()
{
	Relation pgAuthId = heap_open(AuthIdRelationId, AccessShareLock);
	TupleDesc pgAuthIdDescription = RelationGetDescr(pgAuthId);
	HeapTuple tuple = NULL;
	List *commands = NIL;
	const char *alterRoleQuery = NULL;

#if PG_VERSION_NUM >= 120000
	TableScanDesc scan = table_beginscan_catalog(pgAuthId, 0, NULL);
#else
	HeapScanDesc scan = heap_beginscan_catalog(pgAuthId, 0, NULL);
#endif

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		const char *rolename = NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname);

		/*
		 * The default roles are skipped, because reserved roles
		 * cannot be altered.
		 */
		if (IsReservedName(rolename))
		{
			continue;
		}
		alterRoleQuery = GenerateAlterRoleIfExistsCommand(tuple, pgAuthIdDescription);
		commands = lappend(commands, (void *) alterRoleQuery);
	}

	heap_endscan(scan);
	heap_close(pgAuthId, AccessShareLock);

	return commands;
}


/*
 * makeDefElemInt creates a DefElem with integer typed value with -1 as location.
 */
static DefElem *
makeDefElemInt(char *name, int value)
{
	return makeDefElem(name, (Node *) makeInteger(value), -1);
}
