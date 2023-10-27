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
#include "utils/builtins.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_transaction.h"
#include "distributed/deparser.h"
#include "distributed/worker_protocol.h"
#include "distributed/metadata/distobject.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/adaptive_executor.h"
#include "access/htup_details.h"
#include "catalog/pg_tablespace.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "catalog/pg_collation.h"
#include "utils/relcache.h"
#include "catalog/pg_database_d.h"


static AlterOwnerStmt * RecreateAlterDatabaseOwnerStmt(Oid databaseOid);


PG_FUNCTION_INFO_V1(citus_internal_database_command);
static Oid get_database_owner(Oid db_oid);
List * PreprocessGrantOnDatabaseStmt(Node *node, const char *queryString,
									 ProcessUtilityContext processUtilityContext);

/* controlled via GUC */
bool EnableCreateDatabasePropagation = true;
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

	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);

	EnsureCoordinator();

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
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

	return NontransactionalNodeDDLTask(NON_COORDINATOR_NODES, commands);
}


/*
 * citus_internal_database_command is an internal UDF to
 * create/drop a database in an idempotent maner without
 * transaction block restrictions.
 */
Datum
citus_internal_database_command(PG_FUNCTION_ARGS)
{
	int saveNestLevel = NewGUCNestLevel();
	text *commandText = PG_GETARG_TEXT_P(0);
	char *command = text_to_cstring(commandText);
	Node *parseTree = ParseTreeNode(command);

	set_config_option("citus.enable_ddl_propagation", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	set_config_option("citus.enable_create_database_propagation", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	/*
	 * createdb() / DropDatabase() uses ParseState to report the error position for the
	 * input command and the position is reported to be 0 when it's provided as NULL.
	 * We're okay with that because we don't expect this UDF to be called with an incorrect
	 * DDL command.
	 *
	 */
	ParseState *pstate = NULL;

	if (IsA(parseTree, CreatedbStmt))
	{
		CreatedbStmt *stmt = castNode(CreatedbStmt, parseTree);

		bool missingOk = true;
		Oid databaseOid = get_database_oid(stmt->dbname, missingOk);

		if (!OidIsValid(databaseOid))
		{
			createdb(pstate, (CreatedbStmt *) parseTree);
		}
	}
	else if (IsA(parseTree, DropdbStmt))
	{
		DropdbStmt *stmt = castNode(DropdbStmt, parseTree);

		bool missingOk = false;
		Oid databaseOid = get_database_oid(stmt->dbname, missingOk);


		if (OidIsValid(databaseOid))
		{
			DropDatabase(pstate, (DropdbStmt *) parseTree);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported command type %d", nodeTag(parseTree))));
	}

	/* Below command rollbacks flags to the state before this session*/
	AtEOXact_GUC(true, saveNestLevel);

	PG_RETURN_VOID();
}


List *
PreprocessDropDatabaseStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	bool isPostProcess = false;
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	DropdbStmt *stmt = (DropdbStmt *) node;

	List *addresses = GetObjectAddressListFromParseTree(node, stmt->missing_ok,
														isPostProcess);

	if (list_length(addresses) == 0)
	{
		return NIL;
	}

	ObjectAddress *address = (ObjectAddress *) linitial(addresses);
	if (address->objectId == InvalidOid || !IsObjectDistributed(address))
	{
		return NIL;
	}

	char *dropDatabaseCommand = DeparseTreeNode(node);


	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropDatabaseCommand,
								ENABLE_DDL_PROPAGATION);

	return NontransactionalNodeDDLTask(NON_COORDINATOR_NODES, commands);
}


static ObjectAddress *
GetDatabaseAddressFromDatabaseName(char *databaseName, bool missingOk)
{
	Oid databaseOid = get_database_oid(databaseName, missingOk);
	ObjectAddress *dbAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*dbAddress, DatabaseRelationId, databaseOid);
	return dbAddress;
}


List *
DropDatabaseStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missing_ok);
	return list_make1(dbAddress);
}


List *
CreateDatabaseStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missing_ok);
	return list_make1(dbAddress);
}


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
 * DatabaseCollationInfo is used to store collation related information of a database
 */
typedef struct DatabaseCollationInfo
{
	char *collation;
	char *ctype;
	char *icu_locale;
	char *collversion;
} DatabaseCollationInfo;

/*
 * GetDatabaseCollation gets oid of a database and returns all the collation related information
 * We need this method since collation related info in Form_pg_database is not accessible
 */
static DatabaseCollationInfo
GetDatabaseCollation(Oid db_oid)
{
	HeapTuple tup;
	DatabaseCollationInfo info;
	Datum collationDatum, ctypeDatum, icuLocaleDatum, collverDatum;
	bool isNull;
	Relation rel;
	TupleDesc tupdesc;
	Snapshot snapshot;

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	rel = table_open(DatabaseRelationId, AccessShareLock);
	tup = get_catalog_object_by_oid(rel, Anum_pg_database_oid, db_oid);
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for database %u", db_oid);
	}

	tupdesc = RelationGetDescr(rel);
	collationDatum = heap_getattr(tup, Anum_pg_database_datcollate, tupdesc, &isNull);
	if (isNull)
	{
		info.collation = NULL;
	}
	else
	{
		info.collation = TextDatumGetCString(collationDatum);
	}

	ctypeDatum = heap_getattr(tup, Anum_pg_database_datctype, tupdesc, &isNull);
	if (isNull)
	{
		info.ctype = NULL;
	}
	else
	{
		info.ctype = TextDatumGetCString(ctypeDatum);
	}

	icuLocaleDatum = heap_getattr(tup, Anum_pg_database_daticulocale, tupdesc, &isNull);
	if (isNull)
	{
		info.icu_locale = NULL;
	}
	else
	{
		info.icu_locale = TextDatumGetCString(icuLocaleDatum);
	}

	collverDatum = heap_getattr(tup, Anum_pg_database_datcollversion, tupdesc, &isNull);
	if (isNull)
	{
		info.collversion = NULL;
	}
	else
	{
		info.collversion = TextDatumGetCString(collverDatum);
	}

	table_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);
	heap_freetuple(tup);

	return info;
}


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
	if (collInfo.icu_locale != NULL)
	{
		pfree(collInfo.icu_locale);
	}
}



static char *get_locale_provider_string(char datlocprovider)
{
    switch (datlocprovider)
    {
        case 'c':
            return "libc";
        case 'i':
            return "icu";
        case 'l':
            return "locale";
        default:
            return "";
    }
}


/*
 * GenerateCreateDatabaseStatementFromPgDatabase is gets the pg_database tuple and returns the CREATE DATABASE statement
 */
static char *
GenerateCreateDatabaseStatementFromPgDatabase(Form_pg_database databaseForm)
{
	DatabaseCollationInfo collInfo = GetDatabaseCollation(databaseForm->oid);

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "CREATE DATABASE %s", quote_identifier(NameStr(
																	  databaseForm->
																	  datname)));

	if (databaseForm->datdba != InvalidOid)
	{
		appendStringInfo(&str, " OWNER = %s", GetUserNameFromId(databaseForm->datdba,
																false));
	}

	if (databaseForm->encoding != -1)
	{
		appendStringInfo(&str, " ENCODING = '%s'", pg_encoding_to_char(
							 databaseForm->encoding));
	}

	if (collInfo.collation != NULL)
	{
		appendStringInfo(&str, " LC_COLLATE = '%s'", collInfo.collation);
	}
	if (collInfo.ctype != NULL)
	{
		appendStringInfo(&str, " LC_CTYPE = '%s'", collInfo.ctype);
	}

	if (collInfo.icu_locale != NULL)
	{
		appendStringInfo(&str, " ICU_LOCALE = '%s'", collInfo.icu_locale);
	}

	if (databaseForm->datlocprovider != 0)
	{
		appendStringInfo(&str, " LOCALE_PROVIDER = '%s'", get_locale_provider_string(databaseForm->datlocprovider));
	}

	if (collInfo.collversion != NULL)
	{
		appendStringInfo(&str, " COLLATION_VERSION = '%s'", collInfo.collversion);
	}

	if (databaseForm->dattablespace != InvalidOid)
	{
		appendStringInfo(&str, " TABLESPACE = %s", quote_identifier(GetTablespaceName(
																		databaseForm->
																		dattablespace)));
	}

	appendStringInfo(&str, " ALLOW_CONNECTIONS = '%s'", databaseForm->datallowconn ?
					 "true" : "false");

	if (databaseForm->datconnlimit >= 0)
	{
		appendStringInfo(&str, " CONNECTION LIMIT %d", databaseForm->datconnlimit);
	}

	appendStringInfo(&str, " IS_TEMPLATE = '%s'", databaseForm->datistemplate ? "true" :
					 "false");

	FreeDatabaseCollationInfo(collInfo);


	return str.data;
}


/*
 * GenerateCreateDatabaseCommandList is gets the pg_database tuples and returns the CREATE DATABASE statement list
 * for all the databases in the cluster.citus_internal_database_command UDF is used to send the CREATE DATABASE
 * statement to the workers since the CREATE DATABASE statement gives error in transaction context.
 */
List *
GenerateCreateDatabaseCommandList(void)
{
	List *commands = NIL;
	HeapTuple tuple;
	Relation pgDatabaseRel;
	TableScanDesc scan;

	pgDatabaseRel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(pgDatabaseRel, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_database databaseForm = (Form_pg_database) GETSTRUCT(tuple);

		char *createStmt = GenerateCreateDatabaseStatementFromPgDatabase(databaseForm);


		StringInfo outerDbStmt;
		outerDbStmt = makeStringInfo();

		/* Generate the CREATE DATABASE statement */
		appendStringInfo(outerDbStmt,
						 "select pg_catalog.citus_internal_database_command( %s)",
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
