/*
 * ddl_replication.c
 *     Proof-of-concept DDL replication
 */
#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"
#include "pg_version_compat.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "commands/publicationcmds.h"
#include "commands/subscriptioncmds.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "distributed/database/ddl_replication.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "postmaster/postmaster.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/elog.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/* use pg_catalog to prevent non-superusers from creating the table */
#define DDL_REPLICATION_SCHEMA "pg_catalog"
#define DDL_REPLICATION_TABLE_NAME "ddl_statements"


static bool ShouldReplicateAlterTableStmt(AlterTableStmt *alterTableStmt);
static bool ShouldReplicateCreateStmt(CreateStmt *createStmt);
static bool ShouldReplicateDropStmt(DropStmt *dropStmt);
static bool ShouldReplicateIndexStmt(IndexStmt *indexStmt);
static bool ShouldReplicateViewStmt(ViewStmt *viewStmt);
static bool ShouldReplicateCommandType(Node *parsetree);
static List * PreProcessDDLReplicationParseTrees(List *parseTreeList);
static void PostProcessDDLReplicationParseTrees(List *parseTreeList);
static void ExecuteParseTreeList(List *parseTreeList, char *sourceText);
static void ExecuteRawStmt(RawStmt *parsetree, char *queryString);

PG_FUNCTION_INFO_V1(database_shard_move_trigger);

/* citus.enable_database_shard_move_ddl_replication setting */
bool EnableDDLReplicationInDatabaseShardMove = false;


/*
 * ShouldReplicateDDLCommand returns whether a DDL command should be replicated
 * based on current settings and the type of command.
 *
 * The switch statement largely follows the structure in ClassifyUtilityCommandAsReadOnly
 * to easily detect differences across PG versions.
 */
bool
ShouldReplicateDDLCommand(Node *parsetree)
{
	if (!EnableDDLReplicationInDatabaseShardMove)
	{
		return false;
	}

	if (creating_extension)
	{
		/* never replicate extension objects */
		return false;
	}

	if (!ShouldReplicateCommandType(parsetree))
	{
		return false;
	}

	/*
	 * Check whether the statements table exists. If only gets created when the
	 * database is migrated.
	 *
	 * Non-existence is not cached, but we only get here for DDL, which is relatively
	 * infrequent.
	 */
	bool missingOk = true;
	RangeVar *rangeVar = makeRangeVar(DDL_REPLICATION_SCHEMA,
									  DDL_REPLICATION_TABLE_NAME, -1);

	Oid statementTableId = RangeVarGetRelid(rangeVar,
											RowExclusiveLock, missingOk);
	if (!OidIsValid(statementTableId))
	{
		/* ddl_statements table does not exist, no move has started */
		return false;
	}

	Publication *publication = GetPublicationByName("database_shard_move_0_pub",
													missingOk);
	if (publication == NULL)
	{
		/* not the publishing side */
		return false;
	}

	return true;
}


/*
 * ShouldReplicateCommandType returns whether parsetree is of a type that
 * should be replicated.
 */
static bool
ShouldReplicateCommandType(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_AlterCollationStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_AlterDomainStmt:
		case T_AlterEnumStmt:
		case T_AlterEventTrigStmt:
		case T_AlterExtensionContentsStmt:
		case T_AlterExtensionStmt:
		case T_AlterFdwStmt:
		case T_AlterForeignServerStmt:
		case T_AlterFunctionStmt:
		case T_AlterObjectDependsStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOpFamilyStmt:
		case T_AlterOperatorStmt:
		case T_AlterOwnerStmt:
		case T_AlterPolicyStmt:
		case T_AlterRoleSetStmt:
		case T_AlterSeqStmt:
#if PG_VERSION_NUM >= 130000
		case T_AlterStatsStmt:
#endif
		case T_AlterTSConfigurationStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTableMoveAllStmt:
		case T_AlterTableSpaceOptionsStmt:
#if PG_VERSION_NUM >= 130000
		case T_AlterTypeStmt:
#endif
		case T_AlterUserMappingStmt:
		case T_CommentStmt:
		case T_CompositeTypeStmt:
		case T_CreateAmStmt:
		case T_CreateCastStmt:
		case T_CreateConversionStmt:
		case T_CreateDomainStmt:
		case T_CreateEnumStmt:
		case T_CreateEventTrigStmt:
		case T_CreateExtensionStmt:
		case T_CreateFdwStmt:
		case T_CreateForeignServerStmt:
		case T_CreateForeignTableStmt:
		case T_CreateFunctionStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_CreatePLangStmt:
		case T_CreatePolicyStmt:
		case T_CreateRangeStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
		case T_CreateStatsStmt:
		case T_CreateTableSpaceStmt:
		case T_CreateTransformStmt:
		case T_CreateTrigStmt:
		case T_CreateUserMappingStmt:
		case T_DefineStmt:
		case T_DropOwnedStmt:
		case T_DropTableSpaceStmt:
		case T_DropUserMappingStmt:
		case T_GrantRoleStmt:
		case T_GrantStmt:
		case T_ImportForeignSchemaStmt:
		case T_ReassignOwnedStmt:
		case T_RefreshMatViewStmt:
		case T_RenameStmt:
		case T_RuleStmt:
		case T_SecLabelStmt:
		{
			return true;
		}

		case T_CreateTableAsStmt:
		{
			/*
			 * TODO: we need to be careful about CREATE TABLE .. AS because
			 * we would need to ensure the result of the SELECT is the same
			 * on the other end. The preferable way to achieve that is to not
			 * replicate the statement directly, but instead replicate a
			 * regular CREATE TABLE statement and replicate the contents.
			 */
			return true;
		}

		case T_AlterTableStmt:
		{
			return ShouldReplicateAlterTableStmt((AlterTableStmt *) parsetree);
		}

		case T_CreateStmt:
		{
			return ShouldReplicateCreateStmt((CreateStmt *) parsetree);
		}

		case T_DropStmt:
		{
			return ShouldReplicateDropStmt((DropStmt *) parsetree);
		}

		case T_IndexStmt:
		{
			return ShouldReplicateIndexStmt((IndexStmt *) parsetree);
		}

		case T_ViewStmt:
		{
			return ShouldReplicateViewStmt((ViewStmt *) parsetree);
		}

		case T_AlterPublicationStmt:
		case T_AlterSubscriptionStmt:
		case T_DropSubscriptionStmt:
		case T_CreateSubscriptionStmt:
		case T_CreatePublicationStmt:
		{
			/* logical replication management is server-specific */
			return false;
		}

		case T_AlterRoleStmt:
		case T_CreateRoleStmt:
		case T_DropRoleStmt:
		{
			/* we consider roles as server-specific */
			return false;
		}

		case T_AlterDatabaseStmt:
#if PG_VERSION_NUM >= PG_VERSION_15
		case T_AlterDatabaseRefreshCollStmt:
#endif
		case T_AlterDatabaseSetStmt:
		case T_CreatedbStmt:
		case T_DropdbStmt:
		{
			/* we consider databases as server-specific */
			return false;
		}

		case T_AlterSystemStmt:
		case T_CallStmt:
		case T_ClusterStmt:
		case T_CopyStmt:
		case T_CheckPointStmt:
		case T_ClosePortalStmt:
		case T_ConstraintsSetStmt:
		case T_DeallocateStmt:
		case T_DeclareCursorStmt:
		case T_DeleteStmt:
		case T_DiscardStmt:
		case T_DoStmt:
		case T_ExecuteStmt:
		case T_ExplainStmt:
		case T_FetchStmt:
		case T_InsertStmt:
		case T_ListenStmt:
		case T_LoadStmt:
		case T_LockStmt:
#if PG_VERSION_NUM >= PG_VERSION_15
		case T_MergeStmt:
#endif
		case T_NotifyStmt:
		case T_PLAssignStmt:
		case T_PrepareStmt:
		case T_ReindexStmt:
		case T_SelectStmt:
		case T_SetOperationStmt:
		case T_TransactionStmt:
		case T_TruncateStmt:
		case T_UnlistenStmt:
		case T_UpdateStmt:
		case T_VacuumStmt:
		case T_VariableSetStmt:
		case T_VariableShowStmt:
		{
			/* these commands do not affect pg_catalog */
			return false;
		}

		default:
		{
			/* error to detect issues early */
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(parsetree));
		}
	}
}


/*
 * ShouldReplicateAlterTableStmt returns whether a ALTER TABLE statement should be
 * replicated.
 */
static bool
ShouldReplicateAlterTableStmt(AlterTableStmt *alterTableStmt)
{
	if (alterTableStmt->relation->relpersistence == RELPERSISTENCE_TEMP)
	{
		return false;
	}

	return true;
}


/*
 * ShouldReplicateCreateStmt returns whether a CREATE TABLE statement should be
 * replicated.
 */
static bool
ShouldReplicateCreateStmt(CreateStmt *createStmt)
{
	if (createStmt->relation->relpersistence == RELPERSISTENCE_TEMP)
	{
		return false;
	}

	return true;
}


/*
 * ShouldReplicateDropStmt returns whether a DROP .. statement should be
 * replicated.
 */
static bool
ShouldReplicateDropStmt(DropStmt *dropStmt)
{
	bool hasObjectsRequiringReplication = false;
	ListCell *cell1 = NULL;

	if (dropStmt->removeType == OBJECT_DATABASE)
	{
		/* DROP DATABASE is not replicated */
		return false;
	}

	foreach(cell1, dropStmt->objects)
	{
		Node *object = lfirst(cell1);
		Relation relation = NULL;
		bool missingOK = true;

		/* Get an ObjectAddress for the object. */
		ObjectAddress address = get_object_address(dropStmt->removeType, object,
												   &relation, AccessExclusiveLock,
												   missingOK);
		if (!OidIsValid(address.objectId))
		{
			/* non-existent objects do not need replication */
			continue;
		}

		Oid namespaceId = get_object_namespace(&address);
		if (OidIsValid(namespaceId) && isTempNamespace(namespaceId))
		{
			/* temporary objects do not need replication */
			continue;
		}

		hasObjectsRequiringReplication = true;

		if (relation != NULL)
		{
			table_close(relation, NoLock);
		}
	}

	return hasObjectsRequiringReplication;
}


/*
 * ShouldReplicateIndexStmt returns whether a CREATE INDEX statement should be
 * replicated.
 */
static bool
ShouldReplicateIndexStmt(IndexStmt *indexStmt)
{
	if (indexStmt->relation->relpersistence == RELPERSISTENCE_TEMP)
	{
		return false;
	}

	return true;
}


/*
 * ShouldReplicateViewStmt returns whether a CREATE VIEW statement should be
 * replicated.
 */
static bool
ShouldReplicateViewStmt(ViewStmt *viewStmt)
{
	if (viewStmt->view->relpersistence == RELPERSISTENCE_TEMP)
	{
		return false;
	}

	return true;
}


/*
 * InsertDDLCommand inserts a DDL command into ddl_replication.statements
 * to trigger logical replication.
 */
void
ReplicateDDLCommand(Node *parsetree, const char *ddlCommand, char *searchPath)
{
	StringInfo insertCommand = makeStringInfo();
	const int argCount = 3;
	Oid argTypes[3];
	Datum argValues[3];
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	if (searchPath == NULL)
	{
		searchPath = "";
	}

	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	RangeVar *ddlStatementsTableName = makeRangeVar(DDL_REPLICATION_SCHEMA,
													DDL_REPLICATION_TABLE_NAME, -1);
	Relation relation = relation_openrv(ddlStatementsTableName, RowExclusiveLock);

	appendStringInfo(insertCommand,
					 "insert into %s (ddl_command, search_path, user_name) values ($1, $2, $3)",
					 quote_qualified_identifier(DDL_REPLICATION_SCHEMA,
												DDL_REPLICATION_TABLE_NAME));

	argTypes[0] = TEXTOID;
	argValues[0] = CStringGetTextDatum(ddlCommand);

	argTypes[1] = TEXTOID;
	argValues[1] = CStringGetTextDatum(searchPath);

	argTypes[2] = NAMEOID;
	argValues[2] = CStringGetDatum(userName);

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(relation->rd_rel->relowner, SECURITY_LOCAL_USERID_CHANGE);

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	if (SPI_execute_with_args(insertCommand->data, argCount, argTypes, argValues, NULL,
							  false, 1) != SPI_OK_INSERT)
	{
		elog(ERROR, "SPI_exec failed: %s", insertCommand->data);
	}

	if (SPI_processed <= 0)
	{
		elog(ERROR, "query did not return any rows: %s", insertCommand->data);
	}

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	table_close(relation, NoLock);
}


/*
 * database_shard_move_ddl_trigger is a trigger on the ddl_statements table
 * that runs the DDL command.
 */
Datum
database_shard_move_trigger(PG_FUNCTION_ARGS)
{
	if (!EnableDDLReplicationInDatabaseShardMove)
	{
		return 0;
	}

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	TupleDesc tupleDescriptor = RelationGetDescr(triggerData->tg_relation);
	HeapTuple insertedTuple = triggerData->tg_trigtuple;

	bool isNull = false;
	Datum ddlDatum = heap_getattr(insertedTuple, 2, tupleDescriptor, &isNull);
	Datum searchPathDatum = heap_getattr(insertedTuple, 3, tupleDescriptor, &isNull);
	Datum userNameDatum = heap_getattr(insertedTuple, 4, tupleDescriptor, &isNull);

	char *ddlCommand = TextDatumGetCString(ddlDatum);
	char *searchPath = TextDatumGetCString(searchPathDatum);
	char *userName = NameStr(*(DatumGetName(userNameDatum)));

	/* TODO: decide what to do in case of a missing role (alert?) */
	bool missingOk = false;
	Oid userId = get_role_oid(userName, missingOk);

	char *savedSearchPath = namespace_search_path;

	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(userId, SECURITY_LOCAL_USERID_CHANGE);

	set_config_option("search_path", searchPath, PGC_USERSET, PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	/* run the DDL command through the parser */
	List *ddlParseTrees = pg_parse_query(ddlCommand);

	ddlParseTrees = PreProcessDDLReplicationParseTrees(ddlParseTrees);

	/* execute the DDL command */
	ExecuteParseTreeList(ddlParseTrees, ddlCommand);

	PostProcessDDLReplicationParseTrees(ddlParseTrees);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	set_config_option("search_path", savedSearchPath, PGC_USERSET, PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	return 0;
}


/*
 * PreProcessDDLReplicationParseTrees cleans parse trees arriving via DDL
 * propagation.
 */
static List *
PreProcessDDLReplicationParseTrees(List *parseTreeList)
{
	List *finalParseTrees = NIL;
	ListCell *commandCell;

	foreach(commandCell, parseTreeList)
	{
		RawStmt *parseTree = (RawStmt *) lfirst(commandCell);
		Node *statement = parseTree->stmt;

		if (IsA(statement, DropStmt))
		{
			DropStmt *dropStmt = (DropStmt *) statement;

			/* make DROP command idempotent to avoid failure */
			dropStmt->missing_ok = true;
		}
		else if (IsA(statement, CreateTableAsStmt))
		{
			CreateTableAsStmt *createTableAsStmt = (CreateTableAsStmt *) statement;
			IntoClause *into = createTableAsStmt->into;

#if PG_VERSION_NUM >= PG_VERSION_14
			if (createTableAsStmt->objtype != OBJECT_MATVIEW)
#else
			if (createTableAsStmt->relkind != OBJECT_MATVIEW)
#endif
			{
				/* logical replication will copy data for us */
				into->skipData = true;
			}
		}

		finalParseTrees = lappend(finalParseTrees, parseTree);
	}

	return finalParseTrees;
}


/*
 * PostProcessDDLReplicationParseTrees takes additional steps parse trees arriving
 * via DDL propagation.
 */
static void
PostProcessDDLReplicationParseTrees(List *parseTreeList)
{
	ListCell *commandCell;

	foreach(commandCell, parseTreeList)
	{
		RawStmt *parseTree = (RawStmt *) lfirst(commandCell);
		Node *statement = parseTree->stmt;
		bool newTableAdded = false;

		if (IsA(statement, CreateStmt))
		{
			newTableAdded = true;
		}
		else if (IsA(statement, CreateTableAsStmt))
		{
			CreateTableAsStmt *createTableAsStmt = (CreateTableAsStmt *) statement;
			IntoClause *into = createTableAsStmt->into;
			bool isMaterializedView = into->viewQuery != NULL;

			if (!isMaterializedView)
			{
				newTableAdded = true;
			}
		}

		if (newTableAdded)
		{
			/*
			 * A new table was added. Trigger an ALTER SUBSCRIPTION .. REFRESH
			 * to start replicating its contents. We cannot do this from the
			 * trigger because it opens and closes a replication connection,
			 * which would break logical replication in spectacular fashion.
			 */
			/*TriggerSubscriptionRefresh(); */
		}
	}
}


/*
 * Execute all commands in a given SQL string.
 */
static void
ExecuteParseTreeList(List *parseTreeList, char *sourceText)
{
	ListCell *commandCell;

	foreach(commandCell, parseTreeList)
	{
		RawStmt *parsetree = (RawStmt *) lfirst(commandCell);

		char *queryString = sourceText;
		bool freeQueryString = false;

		if (parsetree->stmt_len > 0)
		{
			queryString = (char *) palloc(parsetree->stmt_len + 1);
			strlcpy(queryString, sourceText + parsetree->stmt_location,
					parsetree->stmt_len + 1);
			freeQueryString = true;
		}

		ExecuteRawStmt(parsetree, queryString);

		if (freeQueryString)
		{
			pfree(queryString);
		}

		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * ExecuteRawStmt plans and executes a parse tree.
 */
static void
ExecuteRawStmt(RawStmt *parsetree, char *queryString)
{
	bool isTopLevel = false;

#if PG_VERSION_NUM < 130000
	const char *commandTag;
	char completionTag[COMPLETION_TAG_BUFSIZE];
#else
	CommandTag commandTag;
	QueryCompletion qc;
#endif

	List *querytree_list;
	List *plantree_list;
	Portal portal;
	DestReceiver *receiver;

	if (IsA(parsetree, TransactionStmt))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unexpected transaction control statement")));
	}

	ereport(LOG, (errmsg("replaying DDL from source: %s", queryString)));

	if (LogLocalCommands)
	{
		ereport(NOTICE, (errmsg("running locally: %s", queryString)));
	}

	commandTag = CreateCommandTag(parsetree->stmt);

	set_ps_display(GetCommandTagName(commandTag));

	querytree_list = pg_analyze_and_rewrite_fixedparams(parsetree, queryString,
														NULL, 0, NULL);
	plantree_list = pg_plan_queries(querytree_list, queryString, 0, NULL);

	CHECK_FOR_INTERRUPTS();

	portal = CreatePortal("restore", true, true);
	portal->visible = false;

	PortalDefineQuery(portal, NULL, queryString, commandTag, plantree_list, NULL);
	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	receiver = CreateDestReceiver(DestNone);

	/* Here's where we actually execute the command. */
	(void) PortalRun(portal, FETCH_ALL, isTopLevel, true, receiver, receiver, &qc);

	/* Clean up the receiver. */
	(*receiver->rDestroy)(receiver);

	/* Clean up the portal. */
	PortalDrop(portal, false);

	CommandCounterIncrement();
}
