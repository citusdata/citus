/*-------------------------------------------------------------------------
 *
 * worker_create_or_replace.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "parser/parse_type.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_protocol.h"


/*
 * OnCollisionAction describes what to do when the created object
 * and existing object do not match.
 */
typedef enum OnCollisionAction
{
	ON_COLLISION_RENAME,
	ON_COLLISION_DROP
} OnCollisionAction;


static List * CreateStmtListByObjectAddress(const ObjectAddress *address);
static bool CompareStringList(List *list1, List *list2);
static OnCollisionAction GetOnCollisionAction(const ObjectAddress *address);


PG_FUNCTION_INFO_V1(worker_create_or_replace_object);
PG_FUNCTION_INFO_V1(worker_create_or_replace_object_array);
static bool WorkerCreateOrReplaceObject(List *sqlStatements);


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
 * WrapCreateOrReplaceList takes a list of sql commands and wraps it in a call to citus'
 * udf to create or replace the existing object based on its create commands.
 */
char *
WrapCreateOrReplaceList(List *sqls)
{
	StringInfoData textArrayLitteral = { 0 };
	initStringInfo(&textArrayLitteral);

	appendStringInfoString(&textArrayLitteral, "ARRAY[");
	const char *sql = NULL;
	bool first = true;
	foreach_declared_ptr(sql, sqls)
	{
		if (!first)
		{
			appendStringInfoString(&textArrayLitteral, ", ");
		}
		appendStringInfoString(&textArrayLitteral, quote_literal_cstr(sql));
		first = false;
	}
	appendStringInfoString(&textArrayLitteral, "]::text[]");

	StringInfoData buf = { 0 };
	initStringInfo(&buf);
	appendStringInfo(&buf, CREATE_OR_REPLACE_COMMAND, textArrayLitteral.data);
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
 * created with the one in the local catalog. If there is a difference the one in the local
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
	char *sqlStatement = text_to_cstring(sqlStatementText);
	List *sqlStatements = list_make1(sqlStatement);

	PG_RETURN_BOOL(WorkerCreateOrReplaceObject(sqlStatements));
}


/*
 * worker_create_or_replace_object(statements text[])
 *
 * function is called, by the coordinator, with a CREATE statement for an object. This
 * function implements the CREATE ... IF NOT EXISTS functionality for objects that do not
 * have this functionality or where their implementation is not sufficient.
 *
 * Besides checking if an object of said name exists it tries to compare the object to be
 * created with the one in the local catalog. If there is a difference the one in the local
 * catalog will be renamed after which the statement can be executed on this worker to
 * create the object. If more statements are provided, all are compared in order with the
 * statements generated on the worker. This works assuming a) both citus versions are the
 * same, b) the objects are exactly the same.
 *
 * Renaming has two purposes
 *  - free the identifier for creation
 *  - non destructive if there is data store that would be destroyed if the object was
 *    used in a table on this node, eg. types. If the type would be dropped with a cascade
 *    it would drop any column holding user data for this type.
 */
Datum
worker_create_or_replace_object_array(PG_FUNCTION_ARGS)
{
	List *sqlStatements = NIL;
	Datum *textArray = NULL;
	int length = 0;
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(0), TEXTOID, -1, false, 'i', &textArray,
					  NULL, &length);

	for (int i = 0; i < length; i++)
	{
		sqlStatements = lappend(sqlStatements, TextDatumGetCString(textArray[i]));
	}

	if (list_length(sqlStatements) < 1)
	{
		ereport(ERROR, (errmsg("expected atleast 1 statement to be provided")));
	}

	PG_RETURN_BOOL(WorkerCreateOrReplaceObject(sqlStatements));
}


/*
 * WorkerCreateOrReplaceObject implements the logic used by both variants of
 * worker_create_or_replace_object to either create the object or coming to the conclusion
 * the object already exists in the correct state.
 *
 * Returns true if the object has been created, false if it was already in the exact state
 * it was asked for.
 */
static bool
WorkerCreateOrReplaceObject(List *sqlStatements)
{
	/*
	 * To check which object we are changing we find the object address from the first
	 * statement passed into the UDF. Later we will check if all object addresses are the
	 * same.
	 *
	 * Although many of the objects will only have one statement in this call, more
	 * complex objects might come with a list of statements. We assume they all are on the
	 * same subject.
	 */
	Node *parseTree = ParseTreeNode(linitial(sqlStatements));
	List *addresses = GetObjectAddressListFromParseTree(parseTree, true, false);
	Assert(list_length(addresses) == 1);

	/* We have already asserted that we have exactly 1 address in the addresses. */
	ObjectAddress *address = linitial(addresses);

	if (ObjectExists(address))
	{
		/*
		 * Object with name from statement is already found locally, check if states are
		 * identical. If objects differ we will rename the old object (non- destructively)
		 * or drop it (if safe) as to make room to create the new object according to the
		 * spec sent.
		 */

		/*
		 * Based on the local catalog we generate the list of commands we would send to
		 * recreate our version of the object. This we can compare to what the coordinator
		 * sent us. If they match we don't do anything.
		 */
		List *localSqlStatements = CreateStmtListByObjectAddress(address);
		if (CompareStringList(sqlStatements, localSqlStatements))
		{
			/*
			 * statements sent by the coordinator are the same as we would create for our
			 * object, therefore we can omit the statements locally and not create the
			 * object as it already exists in the correct shape.
			 *
			 * We let the coordinator know we didn't create the object.
			 */
			return false;
		}

		Node *utilityStmt = NULL;

		if (GetOnCollisionAction(address) == ON_COLLISION_DROP)
		{
			/* drop the existing object */
			utilityStmt = (Node *) CreateDropStmt(address);
		}
		else
		{
			/* rename the existing object */
			char *newName = GenerateBackupNameForCollision(address);
			utilityStmt = (Node *) CreateRenameStatement(address, newName);
		}

		const char *commandString = DeparseTreeNode(utilityStmt);
		ProcessUtilityParseTree(utilityStmt, commandString,
								PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	/* apply all statement locally */
	char *sqlStatement = NULL;
	foreach_declared_ptr(sqlStatement, sqlStatements)
	{
		parseTree = ParseTreeNode(sqlStatement);
		ProcessUtilityParseTree(parseTree, sqlStatement, PROCESS_UTILITY_QUERY, NULL,
								None_Receiver, NULL);

		/*  TODO verify all statements are about exactly 1 subject, mostly a sanity check
		 * to prevent unintentional use of this UDF, needs to come after the local
		 * execution to be able to actually resolve the ObjectAddress of the newly created
		 * object */
	}

	/* type has been created */
	return true;
}


static bool
CompareStringList(List *list1, List *list2)
{
	if (list_length(list1) != list_length(list2))
	{
		return false;
	}

	const char *str1 = NULL;
	const char *str2 = NULL;
	forboth_ptr(str1, list1, str2, list2)
	{
		if (strcmp(str1, str2) != 0)
		{
			return false;
		}
	}

	return true;
}


/*
 * CreateStmtByObjectAddress returns a parsetree that will recreate the object addressed
 * by the ObjectAddress provided.
 *
 * Note: this tree does not contain position information that is normally in a parsetree,
 * therefore you cannot equal this tree against parsed statement. Instead it can be
 * deparsed to do a string comparison.
 */
static List *
CreateStmtListByObjectAddress(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_COLLATION:
		{
			return list_make1(CreateCollationDDL(address->objectId));
		}

		case OCLASS_PROC:
		{
			return list_make1(GetFunctionDDLCommand(address->objectId, false));
		}

		case OCLASS_PUBLICATION:
		{
			return list_make1(CreatePublicationDDLCommand(address->objectId));
		}

		case OCLASS_TSCONFIG:
		{
			List *stmts = GetCreateTextSearchConfigStatements(address);
			return DeparseTreeNodes(stmts);
		}

		case OCLASS_TSDICT:
		{
			List *stmts = GetCreateTextSearchDictionaryStatements(address);
			return DeparseTreeNodes(stmts);
		}

		case OCLASS_TYPE:
		{
			return list_make1(DeparseTreeNode(CreateTypeStmtByObjectAddress(address)));
		}

		default:
		{
			ereport(ERROR, (errmsg(
								"unsupported object to construct a create statement")));
		}
	}
}


/*
 * GetOnCollisionAction decides what to do when the object already exists.
 */
static OnCollisionAction
GetOnCollisionAction(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_PUBLICATION:
		{
			/*
			 * We prefer to drop publications because they can be
			 * harmful (cause update/delete failures) and are relatively
			 * safe to drop.
			 */
			return ON_COLLISION_DROP;
		}

		case OCLASS_COLLATION:
		case OCLASS_PROC:
		case OCLASS_TSCONFIG:
		case OCLASS_TSDICT:
		case OCLASS_TYPE:
		default:
		{
			return ON_COLLISION_RENAME;
		}
	}
}


/*
 * GenerateBackupNameForCollision calculate a backup name for a given object by its
 * address. This name should be used when renaming an existing object before creating the
 * new object locally on the worker.
 */
char *
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

		case OCLASS_TSCONFIG:
		{
			return GenerateBackupNameForTextSearchConfiguration(address);
		}

		case OCLASS_TYPE:
		{
			return GenerateBackupNameForTypeCollision(address);
		}

		case OCLASS_CLASS:
		{
			char relKind = get_rel_relkind(address->objectId);
			if (relKind == RELKIND_SEQUENCE)
			{
				return GenerateBackupNameForSequenceCollision(address);
			}
		}

		default:
		{
			break;
		}
	}

	ereport(ERROR, (errmsg("unsupported object to construct a rename statement"),
					errdetail("unable to generate a backup name for the old type")));
}


/*
 * CreateDropPublicationStmt creates a DROP PUBLICATION statement for the
 * publication at the given address.
 */
static DropStmt *
CreateDropPublicationStmt(const ObjectAddress *address)
{
	Assert(address->classId == PublicationRelationId);

	DropStmt *dropStmt = makeNode(DropStmt);
	dropStmt->removeType = OBJECT_PUBLICATION;
	dropStmt->behavior = DROP_RESTRICT;

	HeapTuple publicationTuple =
		SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(address->objectId));

	if (!HeapTupleIsValid(publicationTuple))
	{
		ereport(ERROR, (errmsg("cannot find publication with oid: %d",
							   address->objectId)));
	}

	Form_pg_publication publicationForm =
		(Form_pg_publication) GETSTRUCT(publicationTuple);

	char *publicationName = NameStr(publicationForm->pubname);
	dropStmt->objects = list_make1(makeString(publicationName));

	ReleaseSysCache(publicationTuple);

	return dropStmt;
}


/*
 * CreateDropStmt returns a DROP statement for the given object.
 */
DropStmt *
CreateDropStmt(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_PUBLICATION:
		{
			return CreateDropPublicationStmt(address);
		}

		default:
		{
			break;
		}
	}

	ereport(ERROR, (errmsg("unsupported object to construct a drop statement"),
					errdetail("unable to generate a parsetree for the drop")));
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
	stmt->object = (Node *) stringToQualifiedNameList_compat(format_type_be_qualified(
																 address->objectId));
	stmt->newname = newName;


	return stmt;
}


/*
 * CreateRenameTextSearchStmt creates a rename statement for a text search configuration
 * based on its ObjectAddress. The rename statement will rename the existing object on its
 * address to the value provided in newName.
 */
static RenameStmt *
CreateRenameTextSearchStmt(const ObjectAddress *address, char *newName)
{
	Assert(address->classId == TSConfigRelationId);
	RenameStmt *stmt = makeNode(RenameStmt);

	stmt->renameType = OBJECT_TSCONFIGURATION;
	stmt->object = (Node *) get_ts_config_namelist(address->objectId);
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
 * CreateRenameSequenceStmt creates a rename statement for a sequence based on its
 * ObjectAddress. The rename statement will rename the existing object on its address
 * to the value provided in newName.
 */
static RenameStmt *
CreateRenameSequenceStmt(const ObjectAddress *address, char *newName)
{
	RenameStmt *stmt = makeNode(RenameStmt);
	Oid seqOid = address->objectId;

	HeapTuple seqClassTuple = SearchSysCache1(RELOID, seqOid);
	if (!HeapTupleIsValid(seqClassTuple))
	{
		ereport(ERROR, (errmsg("citus cache lookup error")));
	}
	Form_pg_class seqClassForm = (Form_pg_class) GETSTRUCT(seqClassTuple);

	char *schemaName = get_namespace_name(seqClassForm->relnamespace);
	char *seqName = NameStr(seqClassForm->relname);
	List *name = list_make2(makeString(schemaName), makeString(seqName));
	ReleaseSysCache(seqClassTuple);

	stmt->renameType = OBJECT_SEQUENCE;
	stmt->object = (Node *) name;
	stmt->relation = makeRangeVar(schemaName, seqName, -1);
	stmt->newname = newName;

	return stmt;
}


/*
 * CreateRenameStatement creates a rename statement for an existing object to rename the
 * object to newName.
 */
RenameStmt *
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

		case OCLASS_TSCONFIG:
		{
			return CreateRenameTextSearchStmt(address, newName);
		}

		case OCLASS_TYPE:
		{
			return CreateRenameTypeStmt(address, newName);
		}

		case OCLASS_CLASS:
		{
			char relKind = get_rel_relkind(address->objectId);
			if (relKind == RELKIND_SEQUENCE)
			{
				return CreateRenameSequenceStmt(address, newName);
			}
		}

		default:
		{
			break;
		}
	}

	ereport(ERROR, (errmsg("unsupported object to construct a rename statement"),
					errdetail("unable to generate a parsetree for the rename")));
}
