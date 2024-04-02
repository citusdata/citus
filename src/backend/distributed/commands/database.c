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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/comment.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/serialize_distributed_ddls.h"
#include "distributed/shard_cleaner.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/*
 * Used to save original name of the database before it is replaced with a
 * temporary name for failure handling purposes in PreprocessCreateDatabaseStmt().
 */
static char *CreateDatabaseCommandOriginalDbName = NULL;


/*
 * The format string used when creating a temporary databases for failure
 * handling purposes.
 *
 * The fields are as follows to ensure using a unique name for each temporary
 * database:
 * - operationId: The operation id returned by RegisterOperationNeedingCleanup().
 * - groupId:     The group id of the worker node where CREATE DATABASE command
 *                is issued from.
 */
#define TEMP_DATABASE_NAME_FMT "citus_temp_database_%lu_%d"


/*
 * DatabaseCollationInfo is used to store collation related information of a database.
 */
typedef struct DatabaseCollationInfo
{
	char *datcollate;
	char *datctype;

#if PG_VERSION_NUM >= PG_VERSION_15
	char *daticulocale;
	char *datcollversion;
#endif

#if PG_VERSION_NUM >= PG_VERSION_16
	char *daticurules;
#endif
} DatabaseCollationInfo;

static char * GenerateCreateDatabaseStatementFromPgDatabase(Form_pg_database
															databaseForm);
static DatabaseCollationInfo GetDatabaseCollation(Oid dbOid);
static AlterOwnerStmt * RecreateAlterDatabaseOwnerStmt(Oid databaseOid);
#if PG_VERSION_NUM >= PG_VERSION_15
static char * GetLocaleProviderString(char datlocprovider);
#endif
static char * GetTablespaceName(Oid tablespaceOid);
static ObjectAddress * GetDatabaseAddressFromDatabaseName(char *databaseName,
														  bool missingOk);

static List * FilterDistributedDatabases(List *databases);
static Oid get_database_owner(Oid dbId);


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
get_database_owner(Oid dbId)
{
	HeapTuple tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbId));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
						errmsg("database with OID %u does not exist", dbId)));
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

	List *distributedDatabases = FilterDistributedDatabases(stmt->objects);

	if (list_length(distributedDatabases) == 0)
	{
		return NIL;
	}

	EnsureCoordinator();

	List *originalObjects = stmt->objects;

	stmt->objects = distributedDatabases;

	char *sql = DeparseTreeNode((Node *) stmt);

	stmt->objects = originalObjects;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * FilterDistributedDatabases filters the database list and returns the distributed ones,
 * as a list.
 */
static List *
FilterDistributedDatabases(List *databases)
{
	List *distributedDatabases = NIL;
	String *databaseName = NULL;
	foreach_ptr(databaseName, databases)
	{
		bool missingOk = true;
		ObjectAddress *dbAddress =
			GetDatabaseAddressFromDatabaseName(strVal(databaseName), missingOk);
		if (IsAnyObjectDistributed(list_make1(dbAddress)))
		{
			distributedDatabases = lappend(distributedDatabases, databaseName);
		}
	}

	return distributedDatabases;
}


/*
 * IsSetTablespaceStatement returns true if the statement is a SET TABLESPACE statement,
 * false otherwise.
 */
bool
IsSetTablespaceStatement(AlterDatabaseStmt *stmt)
{
	DefElem *def = NULL;
	foreach_ptr(def, stmt->options)
	{
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
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 */
List *
PreprocessAlterDatabaseStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	bool missingOk = false;
	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missingOk);

	if (!ShouldPropagate() || !IsAnyObjectDistributed(list_make1(dbAddress)))
	{
		return NIL;
	}


	EnsurePropagationToCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(dbAddress));
	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, stmt->dbname);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	if (IsSetTablespaceStatement(stmt))
	{
		/*
		 * Set tablespace does not work inside a transaction.Therefore, we need to use
		 * NontransactionalNodeDDLTask to run the command on the workers outside
		 * the transaction block.
		 */
		bool warnForPartialFailure = true;
		return NontransactionalNodeDDLTaskList(REMOTE_NODES, commands,
											   warnForPartialFailure);
	}
	else
	{
		return NodeDDLTaskList(REMOTE_NODES, commands);
	}
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * PreprocessAlterDatabaseRefreshCollStmt is executed before the statement is applied to
 * the local postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 */
List *
PreprocessAlterDatabaseRefreshCollStmt(Node *node, const char *queryString,
									   ProcessUtilityContext processUtilityContext)
{
	bool missingOk = true;
	AlterDatabaseRefreshCollStmt *stmt = castNode(AlterDatabaseRefreshCollStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missingOk);

	if (!ShouldPropagate() || !IsAnyObjectDistributed(list_make1(dbAddress)))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(dbAddress));

	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, stmt->dbname);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


#endif


/*
 * PreprocessAlterDatabaseRenameStmt is executed before the statement is applied to
 * the local postgres instance.
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 *
 * We acquire this lock here instead of PostprocessAlterDatabaseRenameStmt because the
 * command renames the database and SerializeDistributedDDLsOnObjectClass resolves the
 * object on workers based on database name. For this reason, we need to acquire the lock
 * before the command is applied to the local postgres instance.
 */
List *
PreprocessAlterDatabaseRenameStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	bool missingOk = true;
	RenameStmt *stmt = castNode(RenameStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->subname,
																  missingOk);

	if (!ShouldPropagate() || !IsAnyObjectDistributed(list_make1(dbAddress)))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(dbAddress));

	/*
	 * Different than other ALTER DATABASE commands, we first acquire a lock
	 * by providing InvalidOid because we want ALTER TABLE .. RENAME TO ..
	 * commands to block not only with ALTER DATABASE operations but also
	 * with CREATE DATABASE operations because they might cause name conflicts
	 * and that could also cause deadlocks too.
	 */
	SerializeDistributedDDLsOnObjectClass(OCLASS_DATABASE);
	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, stmt->subname);

	return NIL;
}


/*
 * PostprocessAlterDatabaseRenameStmt is executed after the statement is applied to the local
 * postgres instance. In this stage we prepare ALTER DATABASE RENAME statement to be run on
 * all workers.
 */
List *
PostprocessAlterDatabaseRenameStmt(Node *node, const char *queryString)
{
	bool missingOk = false;
	RenameStmt *stmt = castNode(RenameStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->newname,
																  missingOk);

	if (!ShouldPropagate() || !IsAnyObjectDistributed(list_make1(dbAddress)))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(dbAddress));

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * PreprocessAlterDatabaseSetStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on databases.
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 */
List *
PreprocessAlterDatabaseSetStmt(Node *node, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	AlterDatabaseSetStmt *stmt = castNode(AlterDatabaseSetStmt, node);

	bool missingOk = true;
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missingOk);
	if (!ShouldPropagate() || !IsAnyObjectDistributed(list_make1(dbAddress)))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(dbAddress));
	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, stmt->dbname);

	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * PreprocessCreateDatabaseStmt is executed before the statement is applied to the local
 * Postgres instance.
 *
 * In this stage, we perform validations that we want to ensure before delegating to
 * previous utility hooks because it might not be convenient to throw an error in an
 * implicit transaction that creates a database. Also in this stage, we save the original
 * database name and replace dbname field with a temporary name for failure handling
 * purposes. We let Postgres create the database with the temporary name, insert a cleanup
 * record for the temporary database name on all nodes and let PostprocessCreateDatabaseStmt()
 * to return the distributed DDL job that both creates the database with the temporary name
 * and then renames it back to its original name.
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 */
List *
PreprocessCreateDatabaseStmt(Node *node, const char *queryString,
							 ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinatorIsInMetadata();

	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	EnsureSupportedCreateDatabaseCommand(stmt);

	SerializeDistributedDDLsOnObjectClass(OCLASS_DATABASE);

	OperationId operationId = RegisterOperationNeedingCleanup();

	char *tempDatabaseName = psprintf(TEMP_DATABASE_NAME_FMT,
									  operationId, GetLocalGroupId());

	List *remoteNodes = TargetWorkerSetNodeList(ALL_SHARD_NODES, RowShareLock);
	WorkerNode *remoteNode = NULL;
	foreach_ptr(remoteNode, remoteNodes)
	{
		InsertCleanupRecordOutsideTransaction(
			CLEANUP_OBJECT_DATABASE,
			pstrdup(quote_identifier(tempDatabaseName)),
			remoteNode->groupId,
			CLEANUP_ON_FAILURE
			);
	}

	CreateDatabaseCommandOriginalDbName = stmt->dbname;
	stmt->dbname = tempDatabaseName;

	/*
	 * Delete cleanup records in the same transaction so that if the current
	 * transactions fails for some reason, then the cleanup records won't be
	 * deleted. In the happy path, we will delete the cleanup records without
	 * deferring them to the background worker.
	 */
	FinalizeOperationNeedingCleanupOnSuccess("create database");

	return NIL;
}


/*
 * PostprocessCreateDatabaseStmt is executed after the statement is applied to the local
 * postgres instance.
 *
 * In this stage, we first rename the temporary database back to its original name for
 * local node and then return a list of distributed DDL jobs to create the database with
 * the temporary name and then to rename it back to its original name. That way, if CREATE
 * DATABASE fails on any of the nodes, the temporary database will be cleaned up by the
 * cleanup records that we inserted in PreprocessCreateDatabaseStmt() and in case of a
 * failure, we won't leak any databases called as the name that user intended to use for
 * the database.
 */
List *
PostprocessCreateDatabaseStmt(Node *node, const char *queryString)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	/*
	 * Given that CREATE DATABASE doesn't support "IF NOT EXISTS" and we're
	 * in the post-process, database must exist, hence missingOk = false.
	 */
	bool missingOk = false;
	bool isPostProcess = true;
	List *addresses = GetObjectAddressListFromParseTree(node, missingOk,
														isPostProcess);
	EnsureAllObjectDependenciesExistOnAllNodes(addresses);

	char *createDatabaseCommand = DeparseTreeNode(node);

	List *createDatabaseCommands = list_make3(DISABLE_DDL_PROPAGATION,
											  (void *) createDatabaseCommand,
											  ENABLE_DDL_PROPAGATION);

	/*
	 * Since the CREATE DATABASE statements cannot be executed in a transaction
	 * block, we need to use NontransactionalNodeDDLTaskList() to send the CREATE
	 * DATABASE statement to the workers.
	 */
	bool warnForPartialFailure = false;
	List *createDatabaseDDLJobList =
		NontransactionalNodeDDLTaskList(REMOTE_NODES, createDatabaseCommands,
										warnForPartialFailure);

	CreatedbStmt *stmt = castNode(CreatedbStmt, node);

	char *renameDatabaseCommand =
		psprintf("ALTER DATABASE %s RENAME TO %s",
				 quote_identifier(stmt->dbname),
				 quote_identifier(CreateDatabaseCommandOriginalDbName));

	List *renameDatabaseCommands = list_make3(DISABLE_DDL_PROPAGATION,
											  renameDatabaseCommand,
											  ENABLE_DDL_PROPAGATION);

	/*
	 * We use NodeDDLTaskList() to send the RENAME DATABASE statement to the
	 * workers because we want to execute it in a coordinated transaction.
	 */
	List *renameDatabaseDDLJobList =
		NodeDDLTaskList(REMOTE_NODES, renameDatabaseCommands);

	/*
	 * Temporarily disable citus.enable_ddl_propagation before issuing
	 * rename command locally because we don't want to execute it on remote
	 * nodes yet. We will execute it on remote nodes by returning it as a
	 * distributed DDL job.
	 *
	 * The reason why we don't want to execute it on remote nodes yet is that
	 * the database is not created on remote nodes yet.
	 */
	int saveNestLevel = NewGUCNestLevel();
	set_config_option("citus.enable_ddl_propagation", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	ExecuteUtilityCommand(renameDatabaseCommand);

	AtEOXact_GUC(true, saveNestLevel);

	/*
	 * Restore the original database name because MarkObjectDistributed()
	 * resolves oid of the object based on the database name and is called
	 * after executing the distributed DDL job that renames temporary database.
	 */
	stmt->dbname = CreateDatabaseCommandOriginalDbName;

	return list_concat(createDatabaseDDLJobList, renameDatabaseDDLJobList);
}


/*
 * PreprocessDropDatabaseStmt is executed before the statement is applied to the local
 * postgres instance. In this stage we can prepare the commands that need to be run on
 * all workers to drop the database.
 *
 * We also serialize database commands globally by acquiring a Citus specific advisory
 * lock based on OCLASS_DATABASE on the first primary worker node.
 */
List *
PreprocessDropDatabaseStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	if (!EnableCreateDatabasePropagation || !ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

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

	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, stmt->dbname);

	char *dropDatabaseCommand = DeparseTreeNode(node);

	List *dropDatabaseCommands = list_make3(DISABLE_DDL_PROPAGATION,
											(void *) dropDatabaseCommand,
											ENABLE_DDL_PROPAGATION);

	/*
	 * Due to same reason stated in PostprocessCreateDatabaseStmt(), we need to
	 * use NontransactionalNodeDDLTaskList() to send the DROP DATABASE statement
	 * to the workers.
	 */
	bool warnForPartialFailure = true;
	List *dropDatabaseDDLJobList =
		NontransactionalNodeDDLTaskList(REMOTE_NODES, dropDatabaseCommands,
										warnForPartialFailure);
	return dropDatabaseDDLJobList;
}


List *
PreprocessAlterDatabaseOwnerStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	if (!EnableAlterDatabaseOwner)
	{
		return NIL;
	}

	List *addresses = GetObjectAddressListFromParseTree(node, false, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	AlterOwnerStmt *stmt = (AlterOwnerStmt *) node;
	char *databaseName = strVal((String *) stmt->object);
	SerializeDistributedDDLsOnObjectClassObject(OCLASS_DATABASE, databaseName);

	EnsureSequentialMode(stmt->objectType);
	QualifyTreeNode(node);

	const char *sql = DeparseTreeNode((Node *) node);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * DropDatabaseStmtObjectAddress gets the ObjectAddress of the database that is the
 * object of the DropdbStmt.
 */
List *
DropDatabaseStmtObjectAddress(Node *node, bool missingOk, bool isPostprocess)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missingOk);
	return list_make1(dbAddress);
}


/*
 * CreateDatabaseStmtObjectAddress gets the ObjectAddress of the database that is the
 * object of the CreatedbStmt.
 */
List *
CreateDatabaseStmtObjectAddress(Node *node, bool missingOk, bool isPostprocess)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	ObjectAddress *dbAddress = GetDatabaseAddressFromDatabaseName(stmt->dbname,
																  missingOk);
	return list_make1(dbAddress);
}


/*
 * EnsureSupportedCreateDatabaseCommand validates the options provided for the CREATE
 * DATABASE command.
 *
 * Parameters:
 * stmt: A CreatedbStmt struct representing a CREATE DATABASE command.
 *       The options field is a list of DefElem structs, each representing an option.
 *
 * Currently, this function checks for the following:
 * - The "oid" option is not supported.
 * - The "template" option is only supported with the value "template1".
 * - The "strategy" option is only supported with the value "wal_log".
 */
void
EnsureSupportedCreateDatabaseCommand(CreatedbStmt *stmt)
{
	DefElem *option = NULL;
	foreach_ptr(option, stmt->options)
	{
		if (strcmp(option->defname, "oid") == 0)
		{
			ereport(ERROR,
					errmsg("CREATE DATABASE option \"%s\" is not supported",
						   option->defname));
		}

		char *optionValue = defGetString(option);

		if (strcmp(option->defname, "template") == 0 &&
			strcmp(optionValue, "template1") != 0)
		{
			ereport(ERROR, errmsg("Only template1 is supported as template "
								  "parameter for CREATE DATABASE"));
		}

		if (strcmp(option->defname, "strategy") == 0 &&
			strcmp(optionValue, "wal_log") != 0)
		{
			ereport(ERROR, errmsg("Only wal_log is supported as strategy "
								  "parameter for CREATE DATABASE"));
		}
	}
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
	char *tablespaceName = pstrdup(NameStr(tablespaceForm->spcname));

	ReleaseSysCache(tuple);

	return tablespaceName;
}


/*
 * GetDatabaseMetadataSyncCommands returns a list of sql statements
 * for the given database id. The list contains the database ddl command,
 * grant commands and comment propagation commands.
 */
List *
GetDatabaseMetadataSyncCommands(Oid dbOid)
{
	char *databaseName = get_database_name(dbOid);
	char *databaseDDLCommand = CreateDatabaseDDLCommand(dbOid);

	List *ddlCommands = list_make1(databaseDDLCommand);

	List *grantDDLCommands = GrantOnDatabaseDDLCommands(dbOid);
	List *commentDDLCommands = GetCommentPropagationCommands(DatabaseRelationId, dbOid,
															 databaseName,
															 OBJECT_DATABASE);

	ddlCommands = list_concat(ddlCommands, grantDDLCommands);
	ddlCommands = list_concat(ddlCommands, commentDDLCommands);

	return ddlCommands;
}


/*
 * GetDatabaseCollation gets oid of a database and returns all the collation related information
 * We need this method since collation related info in Form_pg_database is not accessible.
 */
static DatabaseCollationInfo
GetDatabaseCollation(Oid dbOid)
{
	DatabaseCollationInfo info;
	memset(&info, 0, sizeof(DatabaseCollationInfo));

	Relation rel = table_open(DatabaseRelationId, AccessShareLock);
	HeapTuple tup = get_catalog_object_by_oid(rel, Anum_pg_database_oid, dbOid);
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for database %u", dbOid);
	}

	bool isNull = false;

	TupleDesc tupdesc = RelationGetDescr(rel);

	Datum collationDatum = heap_getattr(tup, Anum_pg_database_datcollate, tupdesc,
										&isNull);
	info.datcollate = TextDatumGetCString(collationDatum);

	Datum ctypeDatum = heap_getattr(tup, Anum_pg_database_datctype, tupdesc, &isNull);
	info.datctype = TextDatumGetCString(ctypeDatum);

#if PG_VERSION_NUM >= PG_VERSION_15

	Datum icuLocaleDatum = heap_getattr(tup, Anum_pg_database_daticulocale, tupdesc,
										&isNull);
	if (!isNull)
	{
		info.daticulocale = TextDatumGetCString(icuLocaleDatum);
	}

	Datum collverDatum = heap_getattr(tup, Anum_pg_database_datcollversion, tupdesc,
									  &isNull);
	if (!isNull)
	{
		info.datcollversion = TextDatumGetCString(collverDatum);
	}
#endif

#if PG_VERSION_NUM >= PG_VERSION_16
	Datum icurulesDatum = heap_getattr(tup, Anum_pg_database_daticurules, tupdesc,
									   &isNull);
	if (!isNull)
	{
		info.daticurules = TextDatumGetCString(icurulesDatum);
	}
#endif

	table_close(rel, AccessShareLock);
	heap_freetuple(tup);

	return info;
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

		default:
		{
			ereport(ERROR, (errmsg("unexpected datlocprovider value: %c",
								   datlocprovider)));
		}
	}
}


#endif


/*
 * GenerateCreateDatabaseStatementFromPgDatabase gets the pg_database tuple and returns the
 * CREATE DATABASE statement that can be used to create given database.
 *
 * Note that this doesn't deparse OID of the database and this is not a
 * problem as we anyway don't allow specifying custom OIDs for databases
 * when creating them.
 */
static char *
GenerateCreateDatabaseStatementFromPgDatabase(Form_pg_database databaseForm)
{
	DatabaseCollationInfo collInfo = GetDatabaseCollation(databaseForm->oid);

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "CREATE DATABASE %s",
					 quote_identifier(NameStr(databaseForm->datname)));

	appendStringInfo(&str, " CONNECTION LIMIT %d", databaseForm->datconnlimit);

	appendStringInfo(&str, " ALLOW_CONNECTIONS = %s",
					 quote_literal_cstr(databaseForm->datallowconn ? "true" : "false"));

	appendStringInfo(&str, " IS_TEMPLATE = %s",
					 quote_literal_cstr(databaseForm->datistemplate ? "true" : "false"));

	appendStringInfo(&str, " LC_COLLATE = %s",
					 quote_literal_cstr(collInfo.datcollate));

	appendStringInfo(&str, " LC_CTYPE = %s", quote_literal_cstr(collInfo.datctype));

	appendStringInfo(&str, " OWNER = %s",
					 quote_identifier(GetUserNameFromId(databaseForm->datdba, false)));

	appendStringInfo(&str, " TABLESPACE = %s",
					 quote_identifier(GetTablespaceName(databaseForm->dattablespace)));

	appendStringInfo(&str, " ENCODING = %s",
					 quote_literal_cstr(pg_encoding_to_char(databaseForm->encoding)));

#if PG_VERSION_NUM >= PG_VERSION_15
	if (collInfo.datcollversion != NULL)
	{
		appendStringInfo(&str, " COLLATION_VERSION = %s",
						 quote_identifier(collInfo.datcollversion));
	}

	if (collInfo.daticulocale != NULL)
	{
		appendStringInfo(&str, " ICU_LOCALE = %s", quote_identifier(
							 collInfo.daticulocale));
	}

	appendStringInfo(&str, " LOCALE_PROVIDER = %s",
					 quote_identifier(GetLocaleProviderString(
										  databaseForm->datlocprovider)));
#endif

#if PG_VERSION_NUM >= PG_VERSION_16
	if (collInfo.daticurules != NULL)
	{
		appendStringInfo(&str, " ICU_RULES = %s", quote_identifier(
							 collInfo.daticurules));
	}
#endif

	return str.data;
}


/*
 * CreateDatabaseDDLCommand returns a CREATE DATABASE command to create given
 * database
 *
 * Command is wrapped by citus_internal_database_command() UDF
 * to avoid from transaction block restrictions that apply to database commands.
 */
char *
CreateDatabaseDDLCommand(Oid dbId)
{
	HeapTuple tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbId));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
						errmsg("database with OID %u does not exist", dbId)));
	}

	Form_pg_database databaseForm = (Form_pg_database) GETSTRUCT(tuple);

	char *createStmt = GenerateCreateDatabaseStatementFromPgDatabase(databaseForm);

	StringInfo outerDbStmt = makeStringInfo();

	/* Generate the CREATE DATABASE statement */
	appendStringInfo(outerDbStmt,
					 "SELECT citus_internal.database_command(%s)",
					 quote_literal_cstr(createStmt));

	ReleaseSysCache(tuple);

	return outerDbStmt->data;
}
