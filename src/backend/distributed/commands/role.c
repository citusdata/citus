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
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static const char * ExtractEncryptedPassword(Oid roleOid);
static const char * CreateAlterRoleIfExistsCommand(AlterRoleStmt *stmt);
static const char * CreateAlterRoleSetIfExistsCommand(AlterRoleSetStmt *stmt);
static bool ShouldPropagateAlterRoleSetQueries(HeapTuple tuple,
											   TupleDesc DbRoleSettingDescription);
static DefElem * makeDefElemInt(char *name, int value);

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

/* controlled via GUC */
bool EnableAlterRolePropagation = false;


/*
 * PostprocessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement. We need to do it this way because we need to use the encrypted
 * password, which is, in some cases, created at standardProcessUtility.
 */
List *
PostprocessAlterRoleStmt(Node *node, const char *queryString)
{
	AlterRoleStmt *stmt = castNode(AlterRoleStmt, node);

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

	DefElem *option = NULL;
	foreach_ptr(option, stmt->options)
	{
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
	List *commands = list_make1((void *) CreateAlterRoleIfExistsCommand(stmt));

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PreprocessAlterRoleSetStmt actually creates the plan we need to execute for alter
 * role set statement.
 */
List *
PreprocessAlterRoleSetStmt(Node *node, const char *queryString)
{
	if (!EnableAlterRolePropagation)
	{
		return NIL;
	}

	EnsureCoordinator();

	AlterRoleSetStmt *stmt = castNode(AlterRoleSetStmt, node);

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commandList);
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

	if (!HeapTupleIsValid(tuple))
	{
		return NULL;
	}

	Datum passwordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
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
	variableSetStmt->args = list_make1(MakeSetStatementArgument(name, value));

	return variableSetStmt;
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


	Datum rolPasswordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
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

	Datum rolValidUntilDatum = heap_getattr(tuple, Anum_pg_authid_rolvaliduntil,
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
 * GenerateAlterRoleSetIfExistsCommands creates ALTER ROLE .. SET commands
 * that copies all session defaults for roles from the pg_db_role_setting table.
 */
List *
GenerateAlterRoleSetIfExistsCommands()
{
	Relation DbRoleSetting = heap_open(DbRoleSettingRelationId, AccessShareLock);
	TupleDesc DbRoleSettingDescription = RelationGetDescr(DbRoleSetting);
	HeapTuple tuple = NULL;
	List *commands = NIL;
	List *alterRoleSetQueries = NIL;


#if PG_VERSION_NUM >= 120000
	TableScanDesc scan = table_beginscan_catalog(DbRoleSetting, 0, NULL);
#else
	HeapScanDesc scan = heap_beginscan_catalog(DbRoleSetting, 0, NULL);
#endif

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		if (ShouldPropagateAlterRoleSetQueries(tuple, DbRoleSettingDescription))
		{
			alterRoleSetQueries =
				GenerateAlterRoleSetIfExistsCommandList(tuple, DbRoleSettingDescription);
			commands = list_concat(commands, alterRoleSetQueries);
		}
	}

	heap_endscan(scan);
	heap_close(DbRoleSetting, AccessShareLock);

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
 * MakeSetStatementArgs parses a configuraton value and creates an A_Const
 * with an appropriate type.
 *
 * The allowed A_Const types are Integer, Float, and String.
 */
Node *
MakeSetStatementArgument(char *configurationName, char *configurationValue)
{
	Node *arg = NULL;
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
				arg = makeIntConst(intValue, -1);
				break;
			}

			case PGC_REAL:
			{
				arg = makeFloatConst(configurationValue, -1);
				break;
			}

			case PGC_BOOL:
			case PGC_STRING:
			case PGC_ENUM:
			{
				arg = makeStringConst(configurationValue, -1);
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
		arg = makeStringConst(configurationValue, -1);
	}
	return (Node *) arg;
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

	n->val.type = T_String;
	n->val.val.str = str;
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

	n->val.type = T_Integer;
	n->val.val.ival = val;
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

	n->val.type = T_Float;
	n->val.val.str = str;
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


/*
 * ShouldPropagateAlterRoleSetQueries decides if the set of AlterRoleSetStmt
 * queries should be propagated to worker nodes
 *
 * A single DbRoleSetting tuple can be used to create multiple AlterRoleSetStmt
 * queries as all of the configs are stored in a text[] column and each entry
 * creates a seperate statement
 */
static bool
ShouldPropagateAlterRoleSetQueries(HeapTuple tuple,
								   TupleDesc DbRoleSettingDescription)
{
	if (!ShouldPropagate())
	{
		return false;
	}

	const char *currentDatabaseName = CurrentDatabaseName();
	const char *databaseName =
		GetDatabaseNameFromDbRoleSetting(tuple, DbRoleSettingDescription);
	const char *roleName = GetRoleNameFromDbRoleSetting(tuple, DbRoleSettingDescription);

	/*
	 * session defaults for databases other than the current one are not propagated
	 */
	if (databaseName != NULL &&
		pg_strcasecmp(databaseName, currentDatabaseName) != 0)
	{
		return false;
	}

	/*
	 * default roles are skipped, because reserved roles
	 * cannot be altered.
	 */
	if (roleName != NULL && IsReservedName(roleName))
	{
		return false;
	}

	return true;
}
