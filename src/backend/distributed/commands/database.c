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
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/database/database_sharding.h"
#include "distributed/deparser.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/* macros to add DefElems to a list  */
#define DEFELEM_ADD_STRING(options, key, value) { \
		DefElem *elem = makeDefElem(key, (Node *) makeString(value), -1); \
		options = lappend(options, elem); \
}

#define DEFELEM_ADD_BOOL(options, key, value) { \
		DefElem *elem = makeDefElem(key, (Node *) makeBoolean(value), -1); \
		options = lappend(options, elem); \
}

#define DEFELEM_ADD_INT(options, key, value) { \
		DefElem *elem = makeDefElem(key, (Node *) makeInteger(value), -1); \
		options = lappend(options, elem); \
}


static AlterOwnerStmt * RecreateAlterDatabaseOwnerStmt(Oid databaseOid);
static Oid get_database_owner(Oid db_oid);
static List * CreateDDLTaskList(char *command, List *workerNodeList,
								bool outsideTransaction);

PG_FUNCTION_INFO_V1(citus_internal_database_command);


/* controlled via GUC */
bool EnableCreateDatabasePropagation = true;
bool EnableAlterDatabaseOwner = true;


/*
 * PostprocessCreatedbStmt creates the plan to synchronize CREATE DATABASE
 * across nodes. We use the cannotBeExecutedInTransction option to avoid
 * u* sending transaction blocks.
 */
List *
PostprocessCreatedbStmt(Node *node, const char *queryString)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	char *databaseName = stmt->dbname;
	bool missingOk = false;
	Oid databaseOid = get_database_oid(databaseName, missingOk);

	/*
	 * TODO: try to reuse regular DDL infrastructure
	 *
	 * We do not do this right now because of the AssignDatabaseToShard at the end.
	 */
	List *workerNodes = TargetWorkerSetNodeList(OTHER_METADATA_NODES, RowShareLock);
	if (list_length(workerNodes) > 0)
	{
		char *createDatabaseCommand = DeparseTreeNode(node);

		StringInfo internalCreateCommand = makeStringInfo();
		appendStringInfo(internalCreateCommand,
						 "SELECT pg_catalog.citus_internal_database_command(%s)",
						 quote_literal_cstr(createDatabaseCommand));

		/*
		 * For the moment, we run CREATE DATABASE in 2PC, though that prevents
		 * us from immediately doing a pg_dump | pg_restore when dealing with
		 * a remote template database.
		 */
		bool outsideTransaction = false;

		List *taskList = CreateDDLTaskList(internalCreateCommand->data, workerNodes,
										   outsideTransaction);

		bool localExecutionSupported = false;
		ExecuteUtilityTaskList(taskList, localExecutionSupported);
	}

	/* synchronize pg_dist_object records */
	ObjectAddress dbAddress = { 0 };
	ObjectAddressSet(dbAddress, DatabaseRelationId, databaseOid);
	MarkObjectDistributed(&dbAddress);

	if (EnableDatabaseSharding)
	{
		AssignDatabaseToShard(databaseOid);
	}

	return NIL;
}


/*
 * CreatedbStmtObjectAddress returns the ObjectAddress of the database that is the
 * object of the CreatedbStmt. Errors if missing_ok is false.
 */
List *
CreatedbStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);

	Oid databaseOid = get_database_oid(stmt->dbname, missing_ok);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, DatabaseRelationId, databaseOid);

	return list_make1(address);
}


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
 * CreateDatabaseDDLCommands returns a list of sql statements to idempotently
 * create a database.
 */
List *
CreateDatabaseDDLCommands(const ObjectAddress *address)
{
	Node *stmt = (Node *) RecreateCreatedbStmt(address->objectId);
	char *createDatabaseCommand = DeparseTreeNode(stmt);

	StringInfo internalCreateCommand = makeStringInfo();
	appendStringInfo(internalCreateCommand,
					 "SELECT pg_catalog.citus_internal_database_command(%s)",
					 quote_literal_cstr(createDatabaseCommand));

	return list_make1(internalCreateCommand->data);
}


/*
 * RecreateCreatedbStmt creates a CreatedbStmt for recreating the given
 * database.
 */
CreatedbStmt *
RecreateCreatedbStmt(Oid databaseOid)
{
	HeapTuple dbTuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(databaseOid));
	if (!HeapTupleIsValid(dbTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("database with OID %u does not exist", databaseOid)));
	}

	Form_pg_database dbRecord = (Form_pg_database) GETSTRUCT(dbTuple);

	List *options = NIL;

	/* OWNER */
	char *ownerName = GetUserNameFromId(dbRecord->datdba, false);
	DEFELEM_ADD_STRING(options, "owner", ownerName);

	/* options below are following pg_database.h order */

	/* ENCODING */
	char *encodingName = (char *) pg_encoding_to_char(dbRecord->encoding);
	DEFELEM_ADD_STRING(options, "encoding", encodingName);

	/* LOCALE_PROVIDER */
	char *localeProvider = dbRecord->datlocprovider == COLLPROVIDER_ICU ? "icu" : "libc";
	DEFELEM_ADD_STRING(options, "locale_provider", localeProvider);

	/* LOCALE_PROVIDER */
	bool isTemplate = dbRecord->datistemplate;
	DEFELEM_ADD_BOOL(options, "is_template", isTemplate);

	/* ALLOW_CONNECTIONS */
	bool allowConns = dbRecord->datallowconn;
	DEFELEM_ADD_BOOL(options, "allow_connections", allowConns);

	/* CONNECTION_LIMIT */
	int connectionLimit = dbRecord->datconnlimit;
	DEFELEM_ADD_INT(options, "connection_limit", connectionLimit);

	/* TABLESPACE */
	char *tablespaceName = get_tablespace_name(dbRecord->dattablespace);
	DEFELEM_ADD_STRING(options, "tablespace", tablespaceName);

	/* LC_COLLATE */
	bool isNull = false;
	Datum collateDatum = SysCacheGetAttr(DATABASEOID, dbTuple,
										 Anum_pg_database_datcollate, &isNull);
	if (!isNull)
	{
		char *collate = TextDatumGetCString(collateDatum);
		DEFELEM_ADD_STRING(options, "lc_collate", collate);
	}

	/* LC_CTYPE */
	Datum ctypeDatum = SysCacheGetAttr(DATABASEOID, dbTuple,
									   Anum_pg_database_datctype, &isNull);
	if (!isNull)
	{
		char *ctype = TextDatumGetCString(ctypeDatum);
		DEFELEM_ADD_STRING(options, "lc_ctype", ctype);
	}

	/* ICU_LOCALE */
	Datum icuLocaleDatum = SysCacheGetAttr(DATABASEOID, dbTuple,
										   Anum_pg_database_daticulocale, &isNull);
	if (!isNull)
	{
		char *icuLocale = TextDatumGetCString(icuLocaleDatum);
		DEFELEM_ADD_STRING(options, "icu_locale", icuLocale);
	}

#if PG_VERSION_NUM >= PG_VERSION_15

	/* COLLATION_VERSION */
	Datum collationVersionDatum = SysCacheGetAttr(DATABASEOID, dbTuple,
												  Anum_pg_database_datcollversion,
												  &isNull);
	if (!isNull)
	{
		char *collationVersion = TextDatumGetCString(collationVersionDatum);
		DEFELEM_ADD_STRING(options, "collation_version", collationVersion);
	}

	/* TODO: decide whether we want OID */

	/* as a general policy, we use WAL strategy on PG15+ */
	DEFELEM_ADD_STRING(options, "strategy", "wal_log");
#endif

	CreatedbStmt *stmt = makeNode(CreatedbStmt);
	stmt->dbname = NameStr(dbRecord->datname);
	stmt->options = options;

	ReleaseSysCache(dbTuple);

	return stmt;
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
 * PreprocessDropdbStmt creates the plan to synchronize DROP DATABASE
 * across nodes. We use the cannotBeExecutedInTransction option to avoid
 * sending transaction blocks.
 */
List *
PreprocessDropdbStmt(Node *node, const char *queryString,
					 ProcessUtilityContext processUtilityContext)
{
	DropdbStmt *stmt = (DropdbStmt *) node;
	char *databaseName = stmt->dbname;
	bool missingOk = false;
	Oid databaseOid = get_database_oid(databaseName, missingOk);

	ObjectAddress dbAddress = { 0 };
	ObjectAddressSet(dbAddress, DatabaseRelationId, databaseOid);
	if (!IsObjectDistributed(&dbAddress))
	{
		return NIL;
	}

	List *workerNodes = TargetWorkerSetNodeList(OTHER_METADATA_NODES, RowShareLock);
	if (list_length(workerNodes) == 0)
	{
		return NIL;
	}

	char *dropDatabaseCommand = DeparseTreeNode(node);

	StringInfo internalDropCommand = makeStringInfo();
	appendStringInfo(internalDropCommand,
					 "SELECT pg_catalog.citus_internal_database_command(%s)",
					 quote_literal_cstr(dropDatabaseCommand));


	/* we execute here to avoid EnsureCoordinator check in ExecuteDistributedDDLJob */
	bool outsideTransaction = false;
	List *taskList = CreateDDLTaskList(internalDropCommand->data, workerNodes,
									   outsideTransaction);

	bool localExecutionSupported = false;
	ExecuteUtilityTaskList(taskList, localExecutionSupported);

	return NIL;
}


/*
 * CreateDDLTaskList creates a task list for running a single DDL command.
 */
static List *
CreateDDLTaskList(char *command, List *workerNodeList, bool outsideTransaction)
{
	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   command,
								   ENABLE_DDL_PROPAGATION);

	Task *task = CitusMakeNode(Task);
	task->taskType = DDL_TASK;
	SetTaskQueryStringList(task, commandList);
	task->cannotBeExecutedInTransction = outsideTransaction;

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
		targetPlacement->nodeName = workerNode->workerName;
		targetPlacement->nodePort = workerNode->workerPort;
		targetPlacement->groupId = workerNode->groupId;
		task->taskPlacementList = lappend(task->taskPlacementList,
										  targetPlacement);
	}

	return list_make1(task);
}


/*
 * citus_internal_database_command is an internal UDF to
 * create/drop a database in an idempotent maner without
 * transaction block restrictions.
 */
Datum
citus_internal_database_command(PG_FUNCTION_ARGS)
{
	text *commandText = PG_GETARG_TEXT_P(0);
	char *command = text_to_cstring(commandText);
	Node *parseTree = ParseTreeNode(command);

	if (IsA(parseTree, CreatedbStmt))
	{
		CreatedbStmt *stmt = castNode(CreatedbStmt, parseTree);

		bool missingOk = true;
		Oid databaseOid = get_database_oid(stmt->dbname, missingOk);

		if (!OidIsValid(databaseOid))
		{
			createdb(NULL, (CreatedbStmt *) parseTree);
		}
		else
		{
			/* TODO: check database properties */
		}
	}
	else if (IsA(parseTree, DropdbStmt))
	{
		DropdbStmt *stmt = castNode(DropdbStmt, parseTree);

		bool missingOk = true;
		Oid databaseOid = get_database_oid(stmt->dbname, missingOk);

		if (!OidIsValid(databaseOid))
		{
			/* already dropped? */
		}
		else
		{
			DropDatabase(NULL, (DropdbStmt *) parseTree);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported command type %d", nodeTag(parseTree))));
	}

	PG_RETURN_VOID();
}
