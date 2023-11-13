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
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/*
 * DatabaseCollationInfo is used to store collation related information of a database
 */
typedef struct DatabaseCollationInfo
{
	char *collation;
	char *ctype;
	#if PG_VERSION_NUM >= PG_VERSION_15
	char *icu_locale;
	char *collversion;
	#endif
} DatabaseCollationInfo;

static AlterOwnerStmt * RecreateAlterDatabaseOwnerStmt(Oid databaseOid);
static Oid get_database_owner(Oid db_oid);
List * PreprocessGrantOnDatabaseStmt(Node *node, const char *queryString,
									 ProcessUtilityContext processUtilityContext);

/* controlled via GUC */
bool EnableCreateDatabasePropagation = false;
bool EnableAlterDatabaseOwner = true;

/*
 * AlterDatabaseOwnerObjectAddress returns the ObjectAddress of the database that is the
 * object of the AlterOwnerStmt. Errors if missing_ok is false.
 */
List *
AlterDatabaseOwnerObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	Oid databaseOid = get_database_oid(strVal((String *) stmt->object), missing_ok);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, DatabaseRelationId, databaseOid);

	return list_make1(address);
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


/*
 * PreprocessGrantOnDatabaseStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 */
List *
PreprocessGrantOnDatabaseStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_DATABASE);

	List *databaseList = stmt->objects;

	if (list_length(databaseList) == 0)
	{
		return NIL;
	}

	EnsureCoordinator();

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


static bool
isSetTablespaceStatement(AlterDatabaseStmt *stmt)
{
	ListCell *lc = NULL;
	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "tablespace") == 0)
		{
			return true;
		}
	}
	return false;
}


/*
 * PreprocessAlterDatabaseStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 */
List *
PreprocessAlterDatabaseStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	if (isSetTablespaceStatement(stmt))
	{
		/* Set tablespace does not work inside a transaction.Therefore, we need to use
		 * NontransactionalNodeDDLTask to run the command on the workers outside
		 * the transaction block.
		 */

		return NontransactionalNodeDDLTaskList(NON_COORDINATOR_NODES, commands);
	}
	else
	{
		return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
	}
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * PreprocessAlterDatabaseSetStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 */
List *
PreprocessAlterDatabaseRefreshCollStmt(Node *node, const char *queryString,
									   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	AlterDatabaseRefreshCollStmt *stmt = castNode(AlterDatabaseRefreshCollStmt, node);

	EnsureCoordinator();

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


#endif

/*
 * PreprocessAlterDatabaseRenameStmt is executed before the statement is applied to the local
 * postgres instance. In this stage we prepare ALTER DATABASE RENAME statement to be run on
 * all workers.
 */
List *
PreprocessAlterDatabaseRenameStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	RenameStmt *stmt = castNode(RenameStmt, node);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterDatabaseSetStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 */
List *
PreprocessAlterDatabaseSetStmt(Node *node, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	AlterDatabaseSetStmt *stmt = castNode(AlterDatabaseSetStmt, node);

	EnsureCoordinator();

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDatabaseStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage, we can perform validations and prepare the commands that need to
 * be run on all workers to grant.
 */
List *
PreprocessCreateDatabaseStmt(Node *node, const char *queryString,
							 ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	/*Validate the statement */
	DeparseTreeNode(node);

	return NIL;
}


/*
 * PostprocessCreatedbStmt is executed after the statement is applied to the local
 * postgres instance. In this stage we can prepare the commands that need to be run on
 * all workers to create the database.
 *
 */
List *
PostprocessCreateDatabaseStmt(Node *node, const char *queryString)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	char *createDatabaseCommand = DeparseTreeNode(node);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) createDatabaseCommand,
								ENABLE_DDL_PROPAGATION);

	return NontransactionalNodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDatabaseStmt is executed after the statement is applied to the local
 * postgres instance. In this stage we can prepare the commands that need to be run on
 * all workers to drop the database. Since the DROP DATABASE statement gives error in
 * transaction context, we need to use NontransactionalNodeDDLTaskList to send the
 * DROP DATABASE statement to the workers.
 */
List *
PreprocessDropDatabaseStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	DropdbStmt *stmt = (DropdbStmt *) node;

	bool isPostProcess = false;
	List *addresses = GetObjectAddressListFromParseTree(node, stmt->missing_ok,
														isPostProcess);

	if (list_length(addresses) != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of objects found when "
							   "executing DROP DATABASE command")));
	}

	ObjectAddress *address = (ObjectAddress *) linitial(addresses);
	if (address->objectId == InvalidOid || !IsAnyObjectDistributed(list_make1(address)))
	{
		return NIL;
	}

	char *dropDatabaseCommand = DeparseTreeNode(node);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropDatabaseCommand,
								ENABLE_DDL_PROPAGATION);

	return NontransactionalNodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * GetDatabaseAddressFromDatabaseName gets the database name and returns the ObjectAddress
 * of the database.
 */
static ObjectAddress *
GetDatabaseAddressFromDatabaseName(char *databaseName, bool missingOk)
{
	Oid databaseOid = get_database_oid(databaseName, missingOk);
	ObjectAddress *dbObjectAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*dbObjectAddress, DatabaseRelationId, databaseOid);
	return dbObjectAddress;
}


/*
 * DropDatabaseStmtObjectAddress gets the ObjectAddress of the database that is the
 * object of the DropdbStmt.
 */
List *
DropDatabaseStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missing_ok);
	return list_make1(dbAddress);
}


/*
 * CreateDatabaseStmtObjectAddress gets the ObjectAddress of the database that is the
 * object of the CreatedbStmt.
 */
List *
CreateDatabaseStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missing_ok);
	return list_make1(dbAddress);
}


/*
 * GetTablespaceName gets the tablespace oid and returns the tablespace name.
 */
static char *
GetTablespaceName(Oid tablespaceOid)
{
	HeapTuple tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(tablespaceOid));
	if (!HeapTupleIsValid(tuple))
	{
		return NULL;
	}

	Form_pg_tablespace tablespaceForm = (Form_pg_tablespace) GETSTRUCT(tuple);
	char *tablespaceName = NameStr(tablespaceForm->spcname);

	ReleaseSysCache(tuple);

	return tablespaceName;
}


/*
 * GetDatabaseCollation gets oid of a database and returns all the collation related information
 * We need this method since collation related info in Form_pg_database is not accessible
 */
static DatabaseCollationInfo
GetDatabaseCollation(Oid db_oid)
{
	DatabaseCollationInfo info;
	bool isNull;

	Snapshot snapshot = RegisterSnapshot(GetLatestSnapshot());
	Relation rel = table_open(DatabaseRelationId, AccessShareLock);
	HeapTuple tup = get_catalog_object_by_oid(rel, Anum_pg_database_oid, db_oid);
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for database %u", db_oid);
	}

	TupleDesc tupdesc = RelationGetDescr(rel);
	Datum collationDatum = heap_getattr(tup, Anum_pg_database_datcollate, tupdesc,
										&isNull);
	if (isNull)
	{
		info.collation = NULL;
	}
	else
	{
		info.collation = TextDatumGetCString(collationDatum);
	}

	Datum ctypeDatum = heap_getattr(tup, Anum_pg_database_datctype, tupdesc, &isNull);
	if (isNull)
	{
		info.ctype = NULL;
	}
	else
	{
		info.ctype = TextDatumGetCString(ctypeDatum);
	}

	#if PG_VERSION_NUM >= PG_VERSION_15

	Datum icuLocaleDatum = heap_getattr(tup, Anum_pg_database_daticulocale, tupdesc,
										&isNull);
	if (isNull)
	{
		info.icu_locale = NULL;
	}
	else
	{
		info.icu_locale = TextDatumGetCString(icuLocaleDatum);
	}

	Datum collverDatum = heap_getattr(tup, Anum_pg_database_datcollversion, tupdesc,
									  &isNull);
	if (isNull)
	{
		info.collversion = NULL;
	}
	else
	{
		info.collversion = TextDatumGetCString(collverDatum);
	}
	#endif

	table_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);
	heap_freetuple(tup);

	return info;
}


/*
 * FreeDatabaseCollationInfo frees the memory allocated for DatabaseCollationInfo
 */
static void
FreeDatabaseCollationInfo(DatabaseCollationInfo collInfo)
{
	if (collInfo.collation != NULL)
	{
		pfree(collInfo.collation);
	}
	if (collInfo.ctype != NULL)
	{
		pfree(collInfo.ctype);
	}
	#if PG_VERSION_NUM >= PG_VERSION_15
	if (collInfo.icu_locale != NULL)
	{
		pfree(collInfo.icu_locale);
	}
	#endif
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * GetLocaleProviderString gets the datlocprovider stored in pg_database
 * and returns the string representation of the datlocprovider
 */
static char *
GetLocaleProviderString(char datlocprovider)
{
	switch (datlocprovider)
	{
		case 'c':
		{
			return "libc";
		}

		case 'i':
		{
			return "icu";
		}

		case 'l':
		{
			return "locale";
		}

		default:
			return "";
	}
}


#endif


/*
 * GenerateCreateDatabaseStatementFromPgDatabase gets the pg_database tuple and returns the
 * CREATE DATABASE statement that can be used to create given database.
 */
static char *
GenerateCreateDatabaseStatementFromPgDatabase(Form_pg_database databaseForm)
{
	DatabaseCollationInfo collInfo = GetDatabaseCollation(databaseForm->oid);

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "CREATE DATABASE %s",
					 quote_identifier(NameStr(databaseForm->datname)));

	if (databaseForm->datdba != InvalidOid)
	{
		appendStringInfo(&str, " OWNER = %s",
						 quote_literal_cstr(GetUserNameFromId(databaseForm->datdba,
															  false)));
	}

	if (databaseForm->encoding != -1)
	{
		appendStringInfo(&str, " ENCODING = %s",
						 quote_literal_cstr(pg_encoding_to_char(databaseForm->encoding)));
	}

	if (collInfo.collation != NULL)
	{
		appendStringInfo(&str, " LC_COLLATE = %s", quote_literal_cstr(
							 collInfo.collation));
	}
	if (collInfo.ctype != NULL)
	{
		appendStringInfo(&str, " LC_CTYPE = %s", quote_literal_cstr(collInfo.ctype));
	}

	#if PG_VERSION_NUM >= PG_VERSION_15
	if (collInfo.icu_locale != NULL)
	{
		appendStringInfo(&str, " ICU_LOCALE = %s", quote_literal_cstr(
							 collInfo.icu_locale));
	}

	if (databaseForm->datlocprovider != 0)
	{
		appendStringInfo(&str, " LOCALE_PROVIDER = %s",
						 quote_literal_cstr(GetLocaleProviderString(
												databaseForm->datlocprovider)));
	}

	if (collInfo.collversion != NULL)
	{
		appendStringInfo(&str, " COLLATION_VERSION = %s", quote_literal_cstr(
							 collInfo.collversion));
	}
	#endif

	if (databaseForm->dattablespace != InvalidOid)
	{
		appendStringInfo(&str, " TABLESPACE = %s",
						 quote_identifier(GetTablespaceName(
											  databaseForm->dattablespace)));
	}

	appendStringInfo(&str, " ALLOW_CONNECTIONS = %s",
					 quote_literal_cstr(databaseForm->datallowconn ? "true" : "false"));

	if (databaseForm->datconnlimit >= 0)
	{
		appendStringInfo(&str, " CONNECTION LIMIT %d", databaseForm->datconnlimit);
	}

	appendStringInfo(&str, " IS_TEMPLATE = %s",
					 quote_literal_cstr(databaseForm->datistemplate ? "true" : "false"));

	FreeDatabaseCollationInfo(collInfo);


	return str.data;
}


/*
 * GenerateCreateDatabaseCommandList gets a list of pg_database tuples and returns
 * a list of CREATE DATABASE statements for all the databases.
 *
 * Commands in the list are wrapped by citus_internal_database_command() UDF
 * to avoid from transaction block restrictions that apply to database commands
 */
List *
GenerateCreateDatabaseCommandList(void)
{
	List *commands = NIL;

	Relation pgDatabaseRel = table_open(DatabaseRelationId, AccessShareLock);
	TableScanDesc scan = table_beginscan_catalog(pgDatabaseRel, 0, NULL);

	HeapTuple tuple = NULL;
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_database databaseForm = (Form_pg_database) GETSTRUCT(tuple);

		char *createStmt = GenerateCreateDatabaseStatementFromPgDatabase(databaseForm);


		StringInfo outerDbStmt = makeStringInfo();

		/* Generate the CREATE DATABASE statement */
		appendStringInfo(outerDbStmt,
						 "SELECT pg_catalog.citus_internal_database_command(%s)",
						 quote_literal_cstr(
							 createStmt));

		elog(LOG, "outerDbStmt: %s", outerDbStmt->data);

		/* Add the statement to the list of commands */
		commands = lappend(commands, outerDbStmt->data);
	}

	heap_endscan(scan);
	table_close(pgDatabaseRel, AccessShareLock);

	return commands;
}
