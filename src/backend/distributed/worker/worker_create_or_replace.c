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
#include "catalog/pg_ts_config.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
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
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_protocol.h"

static List * CreateStmtListByObjectAddress(const ObjectAddress *address);
static bool CompareStringList(List *list1, List *list2);

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


#define PG_GETARG_TYPE(n) pg_getarg_type_impl(fcinfo->flinfo->fn_oid, n);

/*
 * pg_getarg_type_impl returns the oid of the type of the argument to a function by its
 * index.
 *
 * Its an implementation detail of PG_GETARG_TYPE to introspect what kind of argument is
 * passed to the function being called.
 */
static Oid
pg_getarg_type_impl(Oid funcOid, int n)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(proctup))
	{
		return InvalidOid;
	}

	Oid *argtypes = NULL;
	char **argnames = NULL;
	char *argmodes = NULL;
	int numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);
	if (n >= numargs)
	{
		elog(ERROR, "invalid argument index");
	}

	Oid typeOid = argtypes[n];
	pfree(argtypes);
	if (argnames != NULL)
	{
		pfree(argnames);
	}
	if (argmodes != NULL)
	{
		pfree(argmodes);
	}

	ReleaseSysCache(proctup);
	return typeOid;
}


/*
 * Implementation for both
 *  - worker_create_or_replace_object(statement text)
 *  - worker_create_or_replace_object(statements text[])
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
 *
 *  TODO: we can probably remove the existing object if it has no dependents
 */
Datum
worker_create_or_replace_object(PG_FUNCTION_ARGS)
{
	List *sqlStatements = NIL;

	/*
	 * The first argument is either a text or a list of text's, we normalize this to a
	 * list of statements for further use, where the variant with just a single text
	 * argument will be treated as a list of statements with length 1
	 */
	Oid argTypeOid = PG_GETARG_TYPE(0);
	if (argTypeOid == TEXTOID)
	{
		text *sqlStatementText = PG_GETARG_TEXT_P(0);
		char *sqlStatement = text_to_cstring(sqlStatementText);
		sqlStatements = list_make1(sqlStatement);
	}
	else if (argTypeOid == TEXTARRAYOID)
	{
		Datum *textArray = NULL;
		int length = 0;
		deconstruct_array(PG_GETARG_ARRAYTYPE_P(0), TEXTOID, -1, false, 'i', &textArray,
						  NULL, &length);

		for (int i = 0; i < length; i++)
		{
			sqlStatements = lappend(sqlStatements, TextDatumGetCString(textArray[i]));
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unexpected argument type")));
	}

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
	ObjectAddress address = GetObjectAddressFromParseTree(parseTree, true);
	if (ObjectExists(&address))
	{
		/*
		 * Object with name from statement is already found locally, check if states are
		 * identical. If objects differ we will rename the old object (non- destructively)
		 * as to make room to create the new object according to the spec sent.
		 */

		/*
		 * Based on the local catalog we generate the list of commands we would send to
		 * recreate our version of the object. This we can compare to what the coordinator
		 * sent us. If they match we don't do anything.
		 */
		List *localSqlStatements = CreateStmtListByObjectAddress(&address);
		if (CompareStringList(sqlStatements, localSqlStatements))
		{
			/*
			 * statements sent by the coordinator are the same as we would create for our
			 * object, therefore we can omit the statements locally and not create the
			 * object as it already exists in the correct shape.
			 *
			 * We let the coordinator know we didn't create the object.
			 */
			PG_RETURN_BOOL(false);
		}

		char *newName = GenerateBackupNameForCollision(&address);

		RenameStmt *renameStmt = CreateRenameStatement(&address, newName);
		const char *sqlRenameStmt = DeparseTreeNode((Node *) renameStmt);
		ProcessUtilityParseTree((Node *) renameStmt, sqlRenameStmt,
								PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	/* apply all statement locally */
	char *sqlStatement = NULL;
	foreach_ptr(sqlStatement, sqlStatements)
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
	PG_RETURN_BOOL(true);
}


static bool
CompareStringList(List *list1, List *list2)
{
	if (list_length(list1) != list_length(list2))
	{
		return false;
	}

	ListCell *cell1 = NULL;
	ListCell *cell2 = NULL;
	forboth(cell1, list1, cell2, list2)
	{
		const char *str1 = lfirst(cell1);
		const char *str2 = lfirst(cell2);

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

		case OCLASS_TSCONFIG:
		{
			/*
			 * We do support TEXT SEARCH CONFIGURATION, however, we can't recreate the
			 * object in 1 command. Since the returned text is compared to the create
			 * statement sql we always want the sql to be different compared to the
			 * canonical creation sql we return here, hence we return an empty string, as
			 * that should never match the sql we have passed in for the creation.
			 */

			/* TODO: get actual list of statements */
			return NIL;
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
