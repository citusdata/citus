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

#include "distributed/pg_version_constants.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_type.h"
#include "catalog/objectaddress.h"
#include "commands/dbcommands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "utils/syscache.h"

static const char * ExtractEncryptedPassword(Oid roleOid);
static const char * CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt);
static const char * CreateAlterRoleSetIfExistsCommand(AlterRoleSetStmt *stmt);
static char * CreateCreateOrAlterRoleCommand(const char *roleName,
											 CreateRoleStmt *createRoleStmt,
											 AlterRoleStmt *alterRoleStmt);
static DefElem * makeDefElemInt(char *name, int value);
static List * GenerateRoleOptionsList(HeapTuple tuple);

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
static ObjectAddress RoleSpecToObjectAddress(RoleSpec *role, bool missing_ok);

/* controlled via GUC */
bool EnableAlterRolePropagation = true;
bool EnableAlterRoleSetPropagation = true;


/*
 * AlterRoleStmtObjectAddress returns the ObjectAddress of the role in the
 * AlterRoleStmt. If missing_ok is set to false an error will be raised if postgres
 * was unable to find the role that was the target of the statement.
 */
ObjectAddress
AlterRoleStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterRoleStmt *stmt = castNode(AlterRoleStmt, node);
	return RoleSpecToObjectAddress(stmt->role, missing_ok);
}


/*
 * AlterRoleSetStmtObjectAddress returns the ObjectAddress of the role in the
 * AlterRoleSetStmt. If missing_ok is set to false an error will be raised if postgres
 * was unable to find the role that was the target of the statement.
 */
ObjectAddress
AlterRoleSetStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterRoleSetStmt *stmt = castNode(AlterRoleSetStmt, node);
	return RoleSpecToObjectAddress(stmt->role, missing_ok);
}


/*
 * RoleSpecToObjectAddress returns the ObjectAddress of a Role associated with a
 * RoleSpec. If missing_ok is set to false an error will be raised by postgres
 * explaining the Role could not be found.
 */
static ObjectAddress
RoleSpecToObjectAddress(RoleSpec *role, bool missing_ok)
{
	ObjectAddress address = { 0 };

	if (role != NULL)
	{
		/* roles can be NULL for statements on ALL roles eg. ALTER ROLE ALL SET ... */
		Oid roleOid = get_rolespec_oid(role, missing_ok);
		ObjectAddressSet(address, AuthIdRelationId, roleOid);
	}

	return address;
}


/*
 * PostprocessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement. We need to do it this way because we need to use the encrypted
 * password, which is, in some cases, created at standardProcessUtility.
 */
List *
PostprocessAlterRoleStmt(Node *node, const char *queryString)
{
	ObjectAddress address = GetObjectAddressFromParseTree(node, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	if (!EnableAlterRolePropagation || !IsCoordinator())
	{
		return NIL;
	}

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
	List *commands = list_make1((void *) CreateAlterRoleIfExistsCommand(stmt));

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
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

	ObjectAddress address = GetObjectAddressFromParseTree(node, false);

	/*
	 * stmt->role could be NULL when the statement is on 'ALL' roles, we do propagate for
	 * ALL roles. If it is not NULL the role is for a specific role. If that role is not
	 * distributed we will not propagate the statement
	 */
	if (stmt->role != NULL && !IsObjectDistributed(&address))
	{
		return NIL;
	}

	/*
	 * Since roles need to be handled manually on community, we need to support such queries
	 * by handling them locally on worker nodes
	 */
	if (!IsCoordinator())
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commandList);
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

	table_close(pgAuthId, AccessShareLock);
	ReleaseSysCache(tuple);

	if (isNull)
	{
		return NULL;
	}

	return pstrdup(TextDatumGetCString(passwordDatum));
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
	options = lappend(options, makeDefElemInt("superuser", role->rolsuper));
	options = lappend(options, makeDefElemInt("createdb", role->rolcreatedb));
	options = lappend(options, makeDefElemInt("createrole", role->rolcreaterole));
	options = lappend(options, makeDefElemInt("inherit", role->rolinherit));
	options = lappend(options, makeDefElemInt("canlogin", role->rolcanlogin));
	options = lappend(options, makeDefElemInt("isreplication", role->rolreplication));
	options = lappend(options, makeDefElemInt("bypassrls", role->rolbypassrls));
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

	/* load valid unitl data from the heap tuple, use default of infinity if not set */
	Datum rolValidUntilDatum = SysCacheGetAttr(AUTHNAME, tuple,
											   Anum_pg_authid_rolvaliduntil, &isNull);
	char *rolValidUntil = "infinity";
	if (!isNull)
	{
		rolValidUntil = pstrdup((char *) timestamptz_to_str(rolValidUntilDatum));
	}

	Node *validUntilStringNode = (Node *) makeString(rolValidUntil);
	DefElem *validUntilOption = makeDefElem("validUntil", validUntilStringNode, -1);
	options = lappend(options, validUntilOption);

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

	CreateRoleStmt *createRoleStmt = NULL;
	AlterRoleStmt *alterRoleStmt = NULL;
	if (EnableAlterRolePropagation)
	{
		alterRoleStmt = makeNode(AlterRoleStmt);
		alterRoleStmt->role = makeNode(RoleSpec);
		alterRoleStmt->role->roletype = ROLESPEC_CSTRING;
		alterRoleStmt->role->location = -1;
		alterRoleStmt->role->rolename = pstrdup(NameStr(role->rolname));
		alterRoleStmt->action = 1;
		alterRoleStmt->options = GenerateRoleOptionsList(roleTuple);
	}

	ReleaseSysCache(roleTuple);

	List *completeRoleList = NIL;
	if (createRoleStmt != NULL || alterRoleStmt != NULL)
	{
		/* add a worker_create_or_alter_role command if any of them are set */
		char *createOrAlterRoleQuery = CreateCreateOrAlterRoleCommand(
			pstrdup(NameStr(role->rolname)),
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
	struct config_generic **gucVariables = get_guc_variables();
	int numOpts = GetNumConfigOptions();
	struct config_generic **matchingConfig =
		(struct config_generic **) SafeBsearch((void *) &key,
											   (void *) gucVariables,
											   numOpts,
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
