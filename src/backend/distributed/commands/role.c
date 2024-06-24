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

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "pg_version_compat.h"
#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/comment.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"

static const char * ExtractEncryptedPassword(Oid roleOid);
static const char * CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt);
static const char * CreateAlterRoleSetIfExistsCommand(AlterRoleSetStmt *stmt);
static char * CreateCreateOrAlterRoleCommand(const char *roleName,
											 CreateRoleStmt *createRoleStmt,
											 AlterRoleStmt *alterRoleStmt);
static DefElem * makeDefElemInt(char *name, int value);
static DefElem * makeDefElemBool(char *name, bool value);
static List * GenerateRoleOptionsList(HeapTuple tuple);
static List * GenerateGrantRoleStmtsFromOptions(RoleSpec *roleSpec, List *options);
static List * GenerateGrantRoleStmtsOfRole(Oid roleid);
static List * GenerateSecLabelOnRoleStmts(Oid roleid, char *rolename);
static void EnsureSequentialModeForRoleDDL(void);

static char * GetRoleNameFromDbRoleSetting(HeapTuple tuple,
										   TupleDesc DbRoleSettingDescription);
static char * GetDatabaseNameFromDbRoleSetting(HeapTuple tuple,
											   TupleDesc DbRoleSettingDescription);
static Node * makeStringConst(char *str, int location);
static Node * makeIntConst(int val, int location);
static Node * makeFloatConst(char *str, int location);
static const char * WrapQueryInAlterRoleIfExistsCall(const char *query, RoleSpec *role);
static VariableSetStmt * MakeVariableSetStmt(const char *config);
static int ConfigGenericNameCompare(const void *lhs, const void *rhs);
static List * RoleSpecToObjectAddress(RoleSpec *role, bool missing_ok);

/* controlled via GUC */
bool EnableCreateRolePropagation = true;
bool EnableAlterRolePropagation = true;
bool EnableAlterRoleSetPropagation = true;


/*
 * AlterRoleStmtObjectAddress returns the ObjectAddress of the role in the
 * AlterRoleStmt. If missing_ok is set to false an error will be raised if postgres
 * was unable to find the role that was the target of the statement.
 */
List *
AlterRoleStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterRoleStmt *stmt = castNode(AlterRoleStmt, node);
	return RoleSpecToObjectAddress(stmt->role, missing_ok);
}


/*
 * AlterRoleSetStmtObjectAddress returns the ObjectAddress of the role in the
 * AlterRoleSetStmt. If missing_ok is set to false an error will be raised if postgres
 * was unable to find the role that was the target of the statement.
 */
List *
AlterRoleSetStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterRoleSetStmt *stmt = castNode(AlterRoleSetStmt, node);
	return RoleSpecToObjectAddress(stmt->role, missing_ok);
}


/*
 * RoleSpecToObjectAddress returns the ObjectAddress of a Role associated with a
 * RoleSpec. If missing_ok is set to false an error will be raised by postgres
 * explaining the Role could not be found.
 */
static List *
RoleSpecToObjectAddress(RoleSpec *role, bool missing_ok)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	if (role != NULL)
	{
		/* roles can be NULL for statements on ALL roles eg. ALTER ROLE ALL SET ... */
		Oid roleOid = get_rolespec_oid(role, missing_ok);
		ObjectAddressSet(*address, AuthIdRelationId, roleOid);
	}

	return list_make1(address);
}


/*
 * PostprocessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement. We need to do it this way because we need to use the encrypted
 * password, which is, in some cases, created at standardProcessUtility.
 */
List *
PostprocessAlterRoleStmt(Node *node, const char *queryString)
{
	List *addresses = GetObjectAddressListFromParseTree(node, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	if (!EnableAlterRolePropagation)
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	AlterRoleStmt *stmt = castNode(AlterRoleStmt, node);

	DefElem *option = NULL;
	foreach_ptr(option, stmt->options)
	{
		if (strcasecmp(option->defname, "password") == 0)
		{
			Oid roleOid = get_rolespec_oid(stmt->role, true);
			const char *encryptedPassword = ExtractEncryptedPassword(roleOid);

			if (encryptedPassword != NULL)
			{
				String *encryptedPasswordValue = makeString((char *) encryptedPassword);
				option->arg = (Node *) encryptedPasswordValue;
			}
			else
			{
				option->arg = NULL;
			}

			break;
		}
	}
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) CreateAlterRoleIfExistsCommand(stmt),
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * PreprocessAlterRoleSetStmt actually creates the plan we need to execute for alter
 * role set statement.
 */
List *
PreprocessAlterRoleSetStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	if (!EnableAlterRoleSetPropagation)
	{
		return NIL;
	}

	AlterRoleSetStmt *stmt = castNode(AlterRoleSetStmt, node);

	/* don't propagate if the statement is scoped to another database */
	if (stmt->database != NULL &&
		strcmp(stmt->database, get_database_name(MyDatabaseId)) != 0)
	{
		return NIL;
	}

	List *addresses = GetObjectAddressListFromParseTree(node, false, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	/*
	 * stmt->role could be NULL when the statement is on 'ALL' roles, we do propagate for
	 * ALL roles. If it is not NULL the role is for a specific role. If that role is not
	 * distributed we will not propagate the statement
	 */
	if (stmt->role != NULL && !IsAnyObjectDistributed(addresses))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commandList);
}


/*
 * CreateAlterRoleIfExistsCommand creates ALTER ROLE command, from the alter role node
 *  using the alter_role_if_exists() UDF.
 */
static const char *
CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt)
{
	const char *alterRoleQuery = DeparseTreeNode((Node *) stmt);
	return WrapQueryInAlterRoleIfExistsCall(alterRoleQuery, stmt->role);
}


/*
 * CreateAlterRoleSetIfExistsCommand creates ALTER ROLE .. SET command, from the
 * AlterRoleSetStmt node.
 *
 * If the statement affects a single user, the query is wrapped in a
 * alter_role_if_exists() to make sure that it is run on workers that has a user
 * with the same name. If the query is a ALTER ROLE ALL .. SET query, the query
 * is sent to the workers as is.
 */
static const char *
CreateAlterRoleSetIfExistsCommand(AlterRoleSetStmt *stmt)
{
	char *alterRoleSetQuery = DeparseTreeNode((Node *) stmt);

	/* ALTER ROLE ALL .. SET queries should not be wrapped in a alter_role_if_exists() call */
	if (stmt->role == NULL)
	{
		return alterRoleSetQuery;
	}
	else
	{
		return WrapQueryInAlterRoleIfExistsCall(alterRoleSetQuery, stmt->role);
	}
}


/*
 * WrapQueryInAlterRoleIfExistsCall wraps a given query in a alter_role_if_exists()
 * UDF.
 */
static const char *
WrapQueryInAlterRoleIfExistsCall(const char *query, RoleSpec *role)
{
	StringInfoData buffer = { 0 };

	const char *roleName = RoleSpecString(role, false);
	initStringInfo(&buffer);
	appendStringInfo(&buffer,
					 "SELECT alter_role_if_exists(%s, %s)",
					 quote_literal_cstr(roleName),
					 quote_literal_cstr(query));

	return buffer.data;
}


/*
 * CreateCreateOrAlterRoleCommand creates ALTER ROLE command, from the alter role node
 *  using the alter_role_if_exists() UDF.
 */
static char *
CreateCreateOrAlterRoleCommand(const char *roleName,
							   CreateRoleStmt *createRoleStmt,
							   AlterRoleStmt *alterRoleStmt)
{
	StringInfoData createOrAlterRoleQueryBuffer = { 0 };
	const char *createRoleQuery = "null";
	const char *alterRoleQuery = "null";

	if (createRoleStmt != NULL)
	{
		createRoleQuery = quote_literal_cstr(DeparseTreeNode((Node *) createRoleStmt));
	}

	if (alterRoleStmt != NULL)
	{
		alterRoleQuery = quote_literal_cstr(DeparseTreeNode((Node *) alterRoleStmt));
	}

	initStringInfo(&createOrAlterRoleQueryBuffer);
	appendStringInfo(&createOrAlterRoleQueryBuffer,
					 "SELECT worker_create_or_alter_role(%s, %s, %s)",
					 quote_literal_cstr(roleName),
					 createRoleQuery,
					 alterRoleQuery);

	return createOrAlterRoleQueryBuffer.data;
}


/*
 * ExtractEncryptedPassword extracts the encrypted password of a role. The function
 * gets the password from the pg_authid table.
 */
static const char *
ExtractEncryptedPassword(Oid roleOid)
{
	Relation pgAuthId = table_open(AuthIdRelationId, AccessShareLock);
	TupleDesc pgAuthIdDescription = RelationGetDescr(pgAuthId);
	HeapTuple tuple = SearchSysCache1(AUTHOID, roleOid);
	bool isNull = true;

	if (!HeapTupleIsValid(tuple))
	{
		return NULL;
	}

	Datum passwordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
									   pgAuthIdDescription, &isNull);

	/*
	 * In PG, an empty password is treated the same as NULL.
	 * So we propagate NULL password to the other nodes, even if
	 * the user supplied an empty password
	 */

	char *passwordCstring = NULL;
	if (!isNull)
	{
		passwordCstring = pstrdup(TextDatumGetCString(passwordDatum));
	}

	table_close(pgAuthId, AccessShareLock);
	ReleaseSysCache(tuple);

	return passwordCstring;
}


/*
 * GenerateAlterRoleSetIfExistsCommandList generate a list of ALTER ROLE .. SET commands that
 * copies a role session defaults from the pg_db_role_settings table.
 */
static List *
GenerateAlterRoleSetIfExistsCommandList(HeapTuple tuple,
										TupleDesc DbRoleSettingDescription)
{
	AlterRoleSetStmt *stmt = makeNode(AlterRoleSetStmt);
	List *commandList = NIL;
	bool isnull = false;

	char *databaseName =
		GetDatabaseNameFromDbRoleSetting(tuple, DbRoleSettingDescription);

	if (databaseName != NULL)
	{
		stmt->database = databaseName;
	}

	char *roleName = GetRoleNameFromDbRoleSetting(tuple, DbRoleSettingDescription);

	if (roleName != NULL)
	{
		stmt->role = makeNode(RoleSpec);
		stmt->role->location = -1;
		stmt->role->roletype = ROLESPEC_CSTRING;
		stmt->role->rolename = roleName;
	}

	Datum setconfig = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
								   DbRoleSettingDescription, &isnull);

	Datum *configs;
	int nconfigs;
	int i;

	deconstruct_array(DatumGetArrayTypeP(setconfig),
					  TEXTOID, -1, false, 'i',
					  &configs, NULL, &nconfigs);

	/*
	 * A tuple might contain one or more settings that apply to the user-database combination.
	 * ALTER ROLE ... SET ... only allows to set one at a time. We will create a statement for every
	 * configuration contained in the tuple.
	 */
	for (i = 0; i < nconfigs; i++)
	{
		char *config = TextDatumGetCString(configs[i]);
		stmt->setstmt = MakeVariableSetStmt(config);
		commandList = lappend(commandList,
							  (void *) CreateAlterRoleSetIfExistsCommand(stmt));
	}
	return commandList;
}


/*
 * MakeVariableSetStmt takes a "some-option=some value" string and creates a
 * VariableSetStmt Node.
 */
static VariableSetStmt *
MakeVariableSetStmt(const char *config)
{
	char *name = NULL;
	char *value = NULL;

	ParseLongOption(config, &name, &value);

	VariableSetStmt *variableSetStmt = makeNode(VariableSetStmt);
	variableSetStmt->kind = VAR_SET_VALUE;
	variableSetStmt->name = name;
	variableSetStmt->args = MakeSetStatementArguments(name, value);

	return variableSetStmt;
}


/*
 * GenerateRoleOptionsList returns the list of options set on a user based on the record
 * in pg_authid. It requires the HeapTuple for a user entry to access both its fixed
 * length and variable length fields.
 */
static List *
GenerateRoleOptionsList(HeapTuple tuple)
{
	Form_pg_authid role = ((Form_pg_authid) GETSTRUCT(tuple));

	List *options = NIL;
	options = lappend(options, makeDefElemBool("superuser", role->rolsuper));
	options = lappend(options, makeDefElemBool("createdb", role->rolcreatedb));
	options = lappend(options, makeDefElemBool("createrole", role->rolcreaterole));
	options = lappend(options, makeDefElemBool("inherit", role->rolinherit));
	options = lappend(options, makeDefElemBool("canlogin", role->rolcanlogin));
	options = lappend(options, makeDefElemBool("isreplication", role->rolreplication));
	options = lappend(options, makeDefElemBool("bypassrls", role->rolbypassrls));
	options = lappend(options, makeDefElemInt("connectionlimit", role->rolconnlimit));

	/* load password from heap tuple, use NULL if not set */
	bool isNull = true;
	Datum rolPasswordDatum = SysCacheGetAttr(AUTHNAME, tuple, Anum_pg_authid_rolpassword,
											 &isNull);
	if (!isNull)
	{
		char *rolPassword = pstrdup(TextDatumGetCString(rolPasswordDatum));
		Node *passwordStringNode = (Node *) makeString(rolPassword);
		DefElem *passwordOption = makeDefElem("password", passwordStringNode, -1);
		options = lappend(options, passwordOption);
	}
	else
	{
		options = lappend(options, makeDefElem("password", NULL, -1));
	}

	/* load valid until data from the heap tuple */
	Datum rolValidUntilDatum = SysCacheGetAttr(AUTHNAME, tuple,
											   Anum_pg_authid_rolvaliduntil, &isNull);
	if (!isNull)
	{
		char *rolValidUntil = pstrdup((char *) timestamptz_to_str(rolValidUntilDatum));

		Node *validUntilStringNode = (Node *) makeString(rolValidUntil);
		DefElem *validUntilOption = makeDefElem("validUntil", validUntilStringNode, -1);
		options = lappend(options, validUntilOption);
	}

	return options;
}


/*
 * GenerateCreateOrAlterRoleCommand generates ALTER ROLE command that copies a role from
 * the pg_authid table.
 */
List *
GenerateCreateOrAlterRoleCommand(Oid roleOid)
{
	HeapTuple roleTuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleOid));
	Form_pg_authid role = ((Form_pg_authid) GETSTRUCT(roleTuple));
	char *rolename = pstrdup(NameStr(role->rolname));

	CreateRoleStmt *createRoleStmt = NULL;
	if (EnableCreateRolePropagation)
	{
		createRoleStmt = makeNode(CreateRoleStmt);
		createRoleStmt->stmt_type = ROLESTMT_ROLE;
		createRoleStmt->role = rolename;
		createRoleStmt->options = GenerateRoleOptionsList(roleTuple);
	}

	AlterRoleStmt *alterRoleStmt = NULL;
	if (EnableAlterRolePropagation)
	{
		alterRoleStmt = makeNode(AlterRoleStmt);
		alterRoleStmt->role = makeNode(RoleSpec);
		alterRoleStmt->role->roletype = ROLESPEC_CSTRING;
		alterRoleStmt->role->location = -1;
		alterRoleStmt->role->rolename = rolename;
		alterRoleStmt->action = 1;
		alterRoleStmt->options = GenerateRoleOptionsList(roleTuple);
	}

	ReleaseSysCache(roleTuple);

	List *completeRoleList = NIL;
	if (createRoleStmt != NULL || alterRoleStmt != NULL)
	{
		/* add a worker_create_or_alter_role command if any of them are set */
		char *createOrAlterRoleQuery = CreateCreateOrAlterRoleCommand(
			rolename,
			createRoleStmt,
			alterRoleStmt);

		completeRoleList = lappend(completeRoleList, createOrAlterRoleQuery);
	}

	if (EnableAlterRoleSetPropagation)
	{
		/* append ALTER ROLE ... SET commands fot this specific user */
		List *alterRoleSetCommands = GenerateAlterRoleSetCommandForRole(roleOid);
		completeRoleList = list_concat(completeRoleList, alterRoleSetCommands);
	}

	if (EnableCreateRolePropagation)
	{
		List *grantRoleStmts = GenerateGrantRoleStmtsOfRole(roleOid);
		Node *stmt = NULL;
		foreach_ptr(stmt, grantRoleStmts)
		{
			completeRoleList = lappend(completeRoleList, DeparseTreeNode(stmt));
		}

		/*
		 * append SECURITY LABEL ON ROLE commands for this specific user
		 * When we propagate user creation, we also want to make sure that we propagate
		 * all the security labels it has been given. For this, we check pg_shseclabel
		 * for the ROLE entry corresponding to roleOid, and generate the relevant
		 * SecLabel stmts to be run in the new node.
		 */
		List *secLabelOnRoleStmts = GenerateSecLabelOnRoleStmts(roleOid, rolename);
		stmt = NULL;
		foreach_ptr(stmt, secLabelOnRoleStmts)
		{
			completeRoleList = lappend(completeRoleList, DeparseTreeNode(stmt));
		}

		/*
		 * append COMMENT ON ROLE commands for this specific user
		 * When we propagate user creation, we also want to make sure that we propagate
		 * all the comments it has been given. For this, we check pg_shdescription
		 * for the ROLE entry corresponding to roleOid, and generate the relevant
		 * Comment stmts to be run in the new node.
		 */
		List *commentStmts = GetCommentPropagationCommands(AuthIdRelationId, roleOid,
														   rolename, OBJECT_ROLE);
		completeRoleList = list_concat(completeRoleList, commentStmts);
	}

	return completeRoleList;
}


/*
 * GenerateAlterRoleSetCommandForRole returns the list of database wide settings for a
 * specifc role. If the roleid is InvalidOid it returns the commands that apply to all
 * users for the database or postgres wide.
 */
List *
GenerateAlterRoleSetCommandForRole(Oid roleid)
{
	Relation DbRoleSetting = table_open(DbRoleSettingRelationId, AccessShareLock);
	TupleDesc DbRoleSettingDescription = RelationGetDescr(DbRoleSetting);
	HeapTuple tuple = NULL;
	List *commands = NIL;


	TableScanDesc scan = table_beginscan_catalog(DbRoleSetting, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_db_role_setting roleSetting = (Form_pg_db_role_setting) GETSTRUCT(tuple);
		if (roleSetting->setrole != roleid)
		{
			/* not the user we are looking for */
			continue;
		}

		if (OidIsValid(roleSetting->setdatabase) &&
			roleSetting->setdatabase != MyDatabaseId)
		{
			/* setting is database specific for a different database */
			continue;
		}

		List *alterRoleSetQueries = GenerateAlterRoleSetIfExistsCommandList(tuple,
																			DbRoleSettingDescription);
		commands = list_concat(commands, alterRoleSetQueries);
	}

	heap_endscan(scan);
	table_close(DbRoleSetting, AccessShareLock);

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


/*
 * makeDefElemBool creates a DefElem with boolean typed value with -1 as location.
 */
static DefElem *
makeDefElemBool(char *name, bool value)
{
	return makeDefElem(name, (Node *) makeBoolean(value), -1);
}


/*
 * GetDatabaseNameFromDbRoleSetting performs a lookup, and finds the database name
 * associated DbRoleSetting Tuple
 */
static char *
GetDatabaseNameFromDbRoleSetting(HeapTuple tuple, TupleDesc DbRoleSettingDescription)
{
	bool isnull;

	Datum setdatabase = heap_getattr(tuple, Anum_pg_db_role_setting_setdatabase,
									 DbRoleSettingDescription, &isnull);

	if (isnull)
	{
		return NULL;
	}

	Oid databaseId = DatumGetObjectId(setdatabase);
	char *databaseName = get_database_name(databaseId);

	return databaseName;
}


/*
 * GetRoleNameFromDbRoleSetting performs a lookup, and finds the role name
 * associated DbRoleSetting Tuple
 */
static char *
GetRoleNameFromDbRoleSetting(HeapTuple tuple, TupleDesc DbRoleSettingDescription)
{
	bool isnull;

	Datum setrole = heap_getattr(tuple, Anum_pg_db_role_setting_setrole,
								 DbRoleSettingDescription, &isnull);

	if (isnull)
	{
		return NULL;
	}

	Oid roleId = DatumGetObjectId(setrole);
	char *roleName = GetUserNameFromId(roleId, true);

	return roleName;
}


/*
 * MakeSetStatementArgs parses a configuration value and creates an List of A_Const
 * Nodes with appropriate types.
 *
 * The allowed A_Const types are Integer, Float, and String.
 */
List *
MakeSetStatementArguments(char *configurationName, char *configurationValue)
{
	List *args = NIL;
	char **key = &configurationName;

	/* Perform a lookup on GUC variables to find the config type and units.
	 * All user-defined GUCs have string values, but we need to perform a search
	 * on all the GUCs to understand if it is a user-defined one or not.
	 *
	 * Note: get_guc_variables() is intended for internal use only, but there
	 * is no other way to determine allowed units, and value types other than
	 * using this function
	 */
	int gucCount = 0;
	struct config_generic **gucVariables = get_guc_variables_compat(&gucCount);

	struct config_generic **matchingConfig =
		(struct config_generic **) SafeBsearch((void *) &key,
											   (void *) gucVariables,
											   gucCount,
											   sizeof(struct config_generic *),
											   ConfigGenericNameCompare);

	/* If the config is not user-defined, lookup the variable type to contruct the arguments */
	if (matchingConfig != NULL)
	{
		switch ((*matchingConfig)->vartype)
		{
			/* We use postgresql parser so that we will parse the units only if
			 * the configuration paramater allows it.
			 *
			 * e.g. `SET statement_timeout = '1min'` will be parsed as 60000 since
			 * the value is stored in units of ms internally.
			 */
			case PGC_INT:
			{
				int intValue;
				parse_int(configurationValue, &intValue,
						  (*matchingConfig)->flags, NULL);
				Node *arg = makeIntConst(intValue, -1);
				args = lappend(args, arg);
				break;
			}

			case PGC_REAL:
			{
				Node *arg = makeFloatConst(configurationValue, -1);
				args = lappend(args, arg);
				break;
			}

			case PGC_BOOL:
			case PGC_STRING:
			case PGC_ENUM:
			{
				List *configurationList = NIL;

				if ((*matchingConfig)->flags & GUC_LIST_INPUT)
				{
					char *configurationValueCopy = pstrdup(configurationValue);
					SplitIdentifierString(configurationValueCopy, ',',
										  &configurationList);
				}
				else
				{
					configurationList = list_make1(configurationValue);
				}

				char *configuration = NULL;
				foreach_ptr(configuration, configurationList)
				{
					Node *arg = makeStringConst(configuration, -1);
					args = lappend(args, arg);
				}
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("Unrecognized run-time parameter type for %s",
									   configurationName)));
				break;
			}
		}
	}
	else
	{
		Node *arg = makeStringConst(configurationValue, -1);
		args = lappend(args, arg);
	}
	return args;
}


/*
 * GenerateGrantRoleStmtsFromOptions gets a RoleSpec of a role that is being
 * created and a list of options of CreateRoleStmt to generate GrantRoleStmts
 * for the role's memberships.
 */
static List *
GenerateGrantRoleStmtsFromOptions(RoleSpec *roleSpec, List *options)
{
	List *stmts = NIL;

	DefElem *option = NULL;
	foreach_ptr(option, options)
	{
		if (strcmp(option->defname, "adminmembers") != 0 &&
			strcmp(option->defname, "rolemembers") != 0 &&
			strcmp(option->defname, "addroleto") != 0)
		{
			continue;
		}

		GrantRoleStmt *grantRoleStmt = makeNode(GrantRoleStmt);
		grantRoleStmt->is_grant = true;

		if (strcmp(option->defname, "adminmembers") == 0 || strcmp(option->defname,
																   "rolemembers") == 0)
		{
			grantRoleStmt->granted_roles = list_make1(roleSpec);
			grantRoleStmt->grantee_roles = (List *) option->arg;
		}
		else
		{
			grantRoleStmt->granted_roles = (List *) option->arg;
			grantRoleStmt->grantee_roles = list_make1(roleSpec);
		}

		if (strcmp(option->defname, "adminmembers") == 0)
		{
#if PG_VERSION_NUM >= PG_VERSION_16
			DefElem *opt = makeDefElem("admin", (Node *) makeBoolean(true), -1);
			grantRoleStmt->opt = list_make1(opt);
#else
			grantRoleStmt->admin_opt = true;
#endif
		}

		stmts = lappend(stmts, grantRoleStmt);
	}
	return stmts;
}


/*
 * GenerateGrantRoleStmtsOfRole generates the GrantRoleStmts for the memberships
 * of the role whose oid is roleid.
 */
static List *
GenerateGrantRoleStmtsOfRole(Oid roleid)
{
	Relation pgAuthMembers = table_open(AuthMemRelationId, AccessShareLock);
	HeapTuple tuple = NULL;
	List *stmts = NIL;

	ScanKeyData skey[1];

	ScanKeyInit(&skey[0], Anum_pg_auth_members_member, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roleid));
	SysScanDesc scan = systable_beginscan(pgAuthMembers, AuthMemMemRoleIndexId, true,
										  NULL, 1, &skey[0]);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_auth_members membership = (Form_pg_auth_members) GETSTRUCT(tuple);

		ObjectAddress *roleAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*roleAddress, AuthIdRelationId, membership->grantor);
		if (!IsAnyObjectDistributed(list_make1(roleAddress)))
		{
			/* we only need to propagate the grant if the grantor is distributed */
			continue;
		}

		GrantRoleStmt *grantRoleStmt = makeNode(GrantRoleStmt);
		grantRoleStmt->is_grant = true;

		RoleSpec *grantedRole = makeNode(RoleSpec);
		grantedRole->roletype = ROLESPEC_CSTRING;
		grantedRole->location = -1;
		grantedRole->rolename = GetUserNameFromId(membership->roleid, true);
		grantRoleStmt->granted_roles = list_make1(grantedRole);

		RoleSpec *granteeRole = makeNode(RoleSpec);
		granteeRole->roletype = ROLESPEC_CSTRING;
		granteeRole->location = -1;
		granteeRole->rolename = GetUserNameFromId(membership->member, true);
		grantRoleStmt->grantee_roles = list_make1(granteeRole);

		RoleSpec *grantorRole = makeNode(RoleSpec);
		grantorRole->roletype = ROLESPEC_CSTRING;
		grantorRole->location = -1;
		grantorRole->rolename = GetUserNameFromId(membership->grantor, false);
		grantRoleStmt->grantor = grantorRole;

#if PG_VERSION_NUM >= PG_VERSION_16

		/* inherit option is always included */
		DefElem *inherit_opt;
		if (membership->inherit_option)
		{
			inherit_opt = makeDefElem("inherit", (Node *) makeBoolean(true), -1);
		}
		else
		{
			inherit_opt = makeDefElem("inherit", (Node *) makeBoolean(false), -1);
		}
		grantRoleStmt->opt = list_make1(inherit_opt);

		/* admin option is false by default, only include true case */
		if (membership->admin_option)
		{
			DefElem *admin_opt = makeDefElem("admin", (Node *) makeBoolean(true), -1);
			grantRoleStmt->opt = lappend(grantRoleStmt->opt, admin_opt);
		}

		/* set option is true by default, only include false case */
		if (!membership->set_option)
		{
			DefElem *set_opt = makeDefElem("set", (Node *) makeBoolean(false), -1);
			grantRoleStmt->opt = lappend(grantRoleStmt->opt, set_opt);
		}
#else
		grantRoleStmt->admin_opt = membership->admin_option;
#endif

		stmts = lappend(stmts, grantRoleStmt);
	}

	systable_endscan(scan);
	table_close(pgAuthMembers, AccessShareLock);

	return stmts;
}


/*
 * GenerateSecLabelOnRoleStmts generates the SecLabelStmts for the role
 * whose oid is roleid.
 */
static List *
GenerateSecLabelOnRoleStmts(Oid roleid, char *rolename)
{
	List *secLabelStmts = NIL;

	/*
	 * Note that roles are shared database objects, therefore their
	 * security labels are stored in pg_shseclabel instead of pg_seclabel.
	 */
	Relation pg_shseclabel = table_open(SharedSecLabelRelationId, AccessShareLock);
	ScanKeyData skey[1];
	ScanKeyInit(&skey[0], Anum_pg_shseclabel_objoid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roleid));
	SysScanDesc scan = systable_beginscan(pg_shseclabel, SharedSecLabelObjectIndexId,
										  true, NULL, 1, &skey[0]);

	HeapTuple tuple = NULL;
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		SecLabelStmt *secLabelStmt = makeNode(SecLabelStmt);
		secLabelStmt->objtype = OBJECT_ROLE;
		secLabelStmt->object = (Node *) makeString(pstrdup(rolename));

		Datum datumArray[Natts_pg_shseclabel];
		bool isNullArray[Natts_pg_shseclabel];

		heap_deform_tuple(tuple, RelationGetDescr(pg_shseclabel), datumArray,
						  isNullArray);

		secLabelStmt->provider = TextDatumGetCString(
			datumArray[Anum_pg_shseclabel_provider - 1]);
		secLabelStmt->label = TextDatumGetCString(
			datumArray[Anum_pg_shseclabel_label - 1]);

		secLabelStmts = lappend(secLabelStmts, secLabelStmt);
	}

	systable_endscan(scan);
	table_close(pg_shseclabel, AccessShareLock);

	return secLabelStmts;
}


/*
 * PreprocessCreateRoleStmt creates a worker_create_or_alter_role query for the
 * role that is being created. With that query we can create the role in the
 * workers or if they exist we alter them to the way they are being created
 * right now.
 */
List *
PreprocessCreateRoleStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateRolePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	EnsureSequentialModeForRoleDDL();

	LockRelationOid(DistNodeRelationId(), RowShareLock);

	CreateRoleStmt *createRoleStmt = castNode(CreateRoleStmt, node);

	AlterRoleStmt *alterRoleStmt = makeNode(AlterRoleStmt);
	alterRoleStmt->role = makeNode(RoleSpec);
	alterRoleStmt->role->roletype = ROLESPEC_CSTRING;
	alterRoleStmt->role->location = -1;
	alterRoleStmt->role->rolename = pstrdup(createRoleStmt->role);
	alterRoleStmt->action = 1;
	alterRoleStmt->options = createRoleStmt->options;

	List *grantRoleStmts = GenerateGrantRoleStmtsFromOptions(alterRoleStmt->role,
															 createRoleStmt->options);

	char *createOrAlterRoleQuery = CreateCreateOrAlterRoleCommand(createRoleStmt->role,
																  createRoleStmt,
																  alterRoleStmt);

	List *commands = NIL;
	commands = lappend(commands, DISABLE_DDL_PROPAGATION);
	commands = lappend(commands, createOrAlterRoleQuery);

	/* deparse all grant statements and add them to the commands list */
	Node *stmt = NULL;
	foreach_ptr(stmt, grantRoleStmts)
	{
		commands = lappend(commands, DeparseTreeNode(stmt));
	}

	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * makeStringConst creates a Const Node that stores a given string
 *
 * copied from backend/parser/gram.c
 */
static Node *
makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

#if PG_VERSION_NUM >= PG_VERSION_15
	n->val.sval.type = T_String;
	n->val.sval.sval = str;
#else
	n->val.type = T_String;
	n->val.val.str = str;
#endif
	n->location = location;

	return (Node *) n;
}


/*
 * makeIntConst creates a Const Node that stores a given integer
 *
 * copied from backend/parser/gram.c
 */
static Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

#if PG_VERSION_NUM >= PG_VERSION_15
	n->val.ival.type = T_Integer;
	n->val.ival.ival = val;
#else
	n->val.type = T_Integer;
	n->val.val.ival = val;
#endif
	n->location = location;

	return (Node *) n;
}


/*
 * makeIntConst creates a Const Node that stores a given Float
 *
 * copied from backend/parser/gram.c
 */
static Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

#if PG_VERSION_NUM >= PG_VERSION_15
	n->val.fval.type = T_Float;
	n->val.fval.fval = str;
#else
	n->val.type = T_Float;
	n->val.val.str = str;
#endif
	n->location = location;

	return (Node *) n;
}


/*
 * PreprocessDropRoleStmt finds the distributed role out of the ones
 * being dropped and unmarks them distributed and creates the drop statements
 * for the workers.
 */
List *
PreprocessDropRoleStmt(Node *node, const char *queryString,
					   ProcessUtilityContext processUtilityContext)
{
	DropRoleStmt *stmt = castNode(DropRoleStmt, node);
	List *allDropRoles = stmt->roles;

	List *distributedDropRoles = FilterDistributedRoles(allDropRoles);
	if (list_length(distributedDropRoles) <= 0)
	{
		return NIL;
	}

	if (!EnableCreateRolePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	EnsureSequentialModeForRoleDDL();


	stmt->roles = distributedDropRoles;
	char *sql = DeparseTreeNode((Node *) stmt);
	stmt->roles = allDropRoles;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * UnmarkRolesDistributed unmarks the roles in the RoleSpec list distributed.
 */
void
UnmarkRolesDistributed(List *roles)
{
	Node *roleNode = NULL;
	foreach_ptr(roleNode, roles)
	{
		RoleSpec *role = castNode(RoleSpec, roleNode);
		ObjectAddress roleAddress = { 0 };
		Oid roleOid = get_rolespec_oid(role, true);

		if (roleOid == InvalidOid)
		{
			/*
			 * If the role is dropped (concurrently), we might get an inactive oid for the
			 * role. If it is invalid oid, skip.
			 */
			continue;
		}

		ObjectAddressSet(roleAddress, AuthIdRelationId, roleOid);
		UnmarkObjectDistributed(&roleAddress);
	}
}


/*
 * FilterDistributedRoles filters the list of RoleSpecs and returns the ones
 * that are distributed.
 */
List *
FilterDistributedRoles(List *roles)
{
	List *distributedRoles = NIL;
	Node *roleNode = NULL;
	foreach_ptr(roleNode, roles)
	{
		RoleSpec *role = castNode(RoleSpec, roleNode);
		Oid roleOid = get_rolespec_oid(role, true);
		if (roleOid == InvalidOid)
		{
			/*
			 * Non-existing roles are ignored silently here. Postgres will
			 * handle to give an error or not for these roles.
			 */
			continue;
		}
		ObjectAddress *roleAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*roleAddress, AuthIdRelationId, roleOid);
		if (IsAnyObjectDistributed(list_make1(roleAddress)))
		{
			distributedRoles = lappend(distributedRoles, role);
		}
	}
	return distributedRoles;
}


/*
 * PreprocessGrantRoleStmt finds the distributed grantee roles and creates the
 * query to run on the workers.
 */
List *
PreprocessGrantRoleStmt(Node *node, const char *queryString,
						ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateRolePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	GrantRoleStmt *stmt = castNode(GrantRoleStmt, node);
	List *allGranteeRoles = stmt->grantee_roles;
	RoleSpec *grantor = stmt->grantor;

	List *distributedGranteeRoles = FilterDistributedRoles(allGranteeRoles);
	if (list_length(distributedGranteeRoles) <= 0)
	{
		return NIL;
	}

	stmt->grantee_roles = distributedGranteeRoles;
	char *sql = DeparseTreeNode((Node *) stmt);
	stmt->grantee_roles = allGranteeRoles;
	stmt->grantor = grantor;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * PostprocessGrantRoleStmt actually creates the plan we need to execute for grant
 * role statement.
 */
List *
PostprocessGrantRoleStmt(Node *node, const char *queryString)
{
	if (!EnableCreateRolePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	GrantRoleStmt *stmt = castNode(GrantRoleStmt, node);

	RoleSpec *role = NULL;
	foreach_ptr(role, stmt->grantee_roles)
	{
		Oid roleOid = get_rolespec_oid(role, false);
		ObjectAddress *roleAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*roleAddress, AuthIdRelationId, roleOid);

		if (IsAnyObjectDistributed(list_make1(roleAddress)))
		{
			EnsureAllObjectDependenciesExistOnAllNodes(list_make1(roleAddress));
		}
	}
	return NIL;
}


/*
 * ConfigGenericNameCompare compares two config_generic structs based on their
 * name fields. If the name fields contain the same strings two structs are
 * considered to be equal.
 *
 * copied from guc_var_compare in utils/misc/guc.c
 */
static int
ConfigGenericNameCompare(const void *a, const void *b)
{
	const struct config_generic *confa = *(struct config_generic *const *) a;
	const struct config_generic *confb = *(struct config_generic *const *) b;

	/*
	 * guc_var_compare used a custom comparison function here to allow stable
	 * ordering, but we do not need it here as we only perform a lookup, and do
	 * not use this function to order the guc list.
	 */
	return pg_strcasecmp(confa->name, confb->name);
}


/*
 * CreateRoleStmtObjectAddress finds the ObjectAddress for the role described
 * by the CreateRoleStmt. If missing_ok is false this function throws an error if the
 * role does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
List *
CreateRoleStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CreateRoleStmt *stmt = castNode(CreateRoleStmt, node);
	Oid roleOid = get_role_oid(stmt->role, missing_ok);
	ObjectAddress *roleAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*roleAddress, AuthIdRelationId, roleOid);

	return list_make1(roleAddress);
}


/*
 * EnsureSequentialModeForRoleDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the begining.
 *
 * As roles are node scoped objects there exists only 1 instance of the role used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the role the role needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForRoleDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify role because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a role, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Role is created or altered. To make sure subsequent "
							   "commands see the role correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * PreprocessAlterDatabaseSetStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 */
List *
PreprocessAlterRoleRenameStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	if (!EnableAlterRolePropagation)
	{
		return NIL;
	}

	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ROLE);


	EnsurePropagationToCoordinator();

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


List *
RenameRoleStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ROLE);

	Oid roleOid = get_role_oid(stmt->subname, missing_ok);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, AuthIdRelationId, roleOid);

	return list_make1(address);
}
