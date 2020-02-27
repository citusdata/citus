/*-------------------------------------------------------------------------
 *
 * worker_create_or_replace.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "parser/parse_type.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata/distobject.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_protocol.h"

static const char * CreateStmtByObjectAddress(const ObjectAddress *address);
static RenameStmt * CreateRenameStatement(const ObjectAddress *address, char *newName);
static char * GenerateBackupNameForCollision(const ObjectAddress *address);

PG_FUNCTION_INFO_V1(worker_create_or_replace_object);


/*
 * WrapCreateOrReplace takes a sql CREATE command and wraps it in a call to citus' udf to
 * create or replace the existing object based on its create command.
 */
char *
WrapCreateOrReplace(const char *sql)
{
	StringInfoData buf = { 0 };
	initStringInfo(&buf);
	appendStringInfo(&buf, CREATE_OR_REPLACE_COMMAND, quote_literal_cstr(sql));
	return buf.data;
}


/*
 * worker_create_or_replace_object(statement text)
 *
 * function is called, by the coordinator, with a CREATE statement for an object. This
 * function implements the CREATE ... IF NOT EXISTS functionality for objects that do not
 * have this functionality or where their implementation is not sufficient.
 *
 * Besides checking if an object of said name exists it tries to compare the object to be
 * created with the one in the local catalog. If there is a difference the on in the local
 * catalog will be renamed after which the statement can be executed on this worker to
 * create the object.
 *
 * Renaming has two purposes
 *  - free the identifier for creation
 *  - non destructive if there is data store that would be destroyed if the object was
 *    used in a table on this node, eg. types. If the type would be dropped with a cascade
 *    it would drop any column holding user data for this type.
 */
Datum
worker_create_or_replace_object(PG_FUNCTION_ARGS)
{
	text *sqlStatementText = PG_GETARG_TEXT_P(0);
	const char *sqlStatement = text_to_cstring(sqlStatementText);
	Node *parseTree = ParseTreeNode(sqlStatement);

	/*
	 * since going to the drop statement might require some resolving we will do a check
	 * if the type actually exists instead of adding the IF EXISTS keyword to the
	 * statement.
	 */
	ObjectAddress address = GetObjectAddressFromParseTree(parseTree, true);
	if (ObjectExists(&address))
	{
		const char *localSqlStatement = CreateStmtByObjectAddress(&address);

		if (strcmp(sqlStatement, localSqlStatement) == 0)
		{
			/*
			 * TODO string compare is a poor man's comparison, but calling equal on the
			 * parsetree's returns false because there is extra information list character
			 * position of some sort
			 */

			/*
			 * parseTree sent by the coordinator is the same as we would create for our
			 * object, therefore we can omit the create statement locally and not create
			 * the object as it already exists.
			 *
			 * We let the coordinator know we didn't create the object.
			 */
			PG_RETURN_BOOL(false);
		}

		char *newName = GenerateBackupNameForCollision(&address);

		RenameStmt *renameStmt = CreateRenameStatement(&address, newName);
		const char *sqlRenameStmt = DeparseTreeNode((Node *) renameStmt);
		CitusProcessUtility((Node *) renameStmt, sqlRenameStmt,
							PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}

	/* apply create statement locally */
	CitusProcessUtility(parseTree, sqlStatement, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	/* type has been created */
	PG_RETURN_BOOL(true);
}


/*
 * CreateStmtByObjectAddress returns a parsetree that will recreate the object addressed
 * by the ObjectAddress provided.
 *
 * Note: this tree does not contain position information that is normally in a parsetree,
 * therefore you cannot equal this tree against parsed statement. Instead it can be
 * deparsed to do a string comparison.
 */
static const char *
CreateStmtByObjectAddress(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_COLLATION:
		{
			return CreateCollationDDL(address->objectId);
		}

		case OCLASS_PROC:
		{
			return GetFunctionDDLCommand(address->objectId, false);
		}

		case OCLASS_TYPE:
		{
			return DeparseTreeNode(CreateTypeStmtByObjectAddress(address));
		}

		default:
		{
			ereport(ERROR, (errmsg(
								"unsupported object to construct a create statement")));
		}
	}
}


/*
 * GenerateBackupNameForCollision calculate a backup name for a given object by its
 * address. This name should be used when renaming an existing object before creating the
 * new object locally on the worker.
 */
static char *
GenerateBackupNameForCollision(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_COLLATION:
		{
			return GenerateBackupNameForCollationCollision(address);
		}

		case OCLASS_PROC:
		{
			return GenerateBackupNameForProcCollision(address);
		}

		case OCLASS_TYPE:
		{
			return GenerateBackupNameForTypeCollision(address);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported object to construct a rename statement"),
							errdetail(
								"unable to generate a backup name for the old type")));
		}
	}
}


/*
 * CreateRenameTypeStmt creates a rename statement for a type based on its ObjectAddress.
 * The rename statement will rename the existing object on its address to the value
 * provided in newName.
 */
static RenameStmt *
CreateRenameCollationStmt(const ObjectAddress *address, char *newName)
{
	RenameStmt *stmt = makeNode(RenameStmt);
	Oid collid = address->objectId;

	HeapTuple colltup = SearchSysCache1(COLLOID, collid);
	if (!HeapTupleIsValid(colltup))
	{
		ereport(ERROR, (errmsg("citus cache lookup error")));
	}
	Form_pg_collation collationForm =
		(Form_pg_collation) GETSTRUCT(colltup);

	char *schemaName = get_namespace_name(collationForm->collnamespace);
	char *collationName = NameStr(collationForm->collname);
	List *name = list_make2(makeString(schemaName), makeString(collationName));
	ReleaseSysCache(colltup);

	stmt->renameType = OBJECT_COLLATION;
	stmt->object = (Node *) name;
	stmt->newname = newName;

	return stmt;
}


/*
 * CreateRenameTypeStmt creates a rename statement for a type based on its ObjectAddress.
 * The rename statement will rename the existing object on its address to the value
 * provided in newName.
 */
static RenameStmt *
CreateRenameTypeStmt(const ObjectAddress *address, char *newName)
{
	RenameStmt *stmt = makeNode(RenameStmt);

	stmt->renameType = OBJECT_TYPE;
	stmt->object = (Node *) stringToQualifiedNameList(format_type_be_qualified(
														  address->objectId));
	stmt->newname = newName;

	return stmt;
}


/*
 * CreateRenameTypeStmt creates a rename statement for a type based on its ObjectAddress.
 * The rename statement will rename the existing object on its address to the value
 * provided in newName.
 */
static RenameStmt *
CreateRenameProcStmt(const ObjectAddress *address, char *newName)
{
	RenameStmt *stmt = makeNode(RenameStmt);

	stmt->renameType = OBJECT_ROUTINE;
	stmt->object = (Node *) ObjectWithArgsFromOid(address->objectId);
	stmt->newname = newName;

	return stmt;
}


/*
 * CreateRenameStatement creates a rename statement for an existing object to rename the
 * object to newName.
 */
static RenameStmt *
CreateRenameStatement(const ObjectAddress *address, char *newName)
{
	switch (getObjectClass(address))
	{
		case OCLASS_COLLATION:
		{
			return CreateRenameCollationStmt(address, newName);
		}

		case OCLASS_PROC:
		{
			return CreateRenameProcStmt(address, newName);
		}

		case OCLASS_TYPE:
		{
			return CreateRenameTypeStmt(address, newName);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported object to construct a rename statement"),
							errdetail("unable to generate a parsetree for the rename")));
		}
	}
}
