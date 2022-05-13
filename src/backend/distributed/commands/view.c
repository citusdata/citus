/*-------------------------------------------------------------------------
 *
 * view.c
 *    Commands for distributing CREATE OR REPLACE VIEW statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "catalog/objectaddress.h"
#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/errormessage.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/worker_transaction.h"
#include "executor/spi.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static List * FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok);
static void AppendQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid);
static void AppendViewDefinitionToCreateViewCommand(StringInfo buf, Oid viewOid);
static void AppendAliasesToCreateViewCommand(StringInfo createViewCommand, Oid viewOid);
static void AppendOptionsToCreateViewCommand(StringInfo createViewCommand, Oid viewOid);

/*
 * PreprocessViewStmt is called during the planning phase for CREATE OR REPLACE VIEW
 * before it is created on the local node internally.
 */
List *
PreprocessViewStmt(Node *node, const char *queryString,
				   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	EnsureCoordinator();

	return NIL;
}


/*
 * PostprocessViewStmt actually creates the commmands we need to run on workers to
 * propagate views.
 *
 * If view depends on any undistributable object, Citus can not distribute it. In order to
 * not to prevent users from creating local views on the coordinator WARNING message will
 * be sent to the customer about the case instead of erroring out. If no worker nodes exist
 * at all, view will be created locally without any WARNING message.
 *
 * Besides creating the plan we also make sure all (new) dependencies of the view are
 * created on all nodes.
 */
List *
PostprocessViewStmt(Node *node, const char *queryString)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	if (IsObjectAddressOwnedByExtension(&viewAddress, NULL))
	{
		return NIL;
	}

	/* If the view has any unsupported dependency, create it locally */
	if (ErrorOrWarnIfObjectHasUnsupportedDependency(&viewAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&viewAddress);

	char *command = CreateViewDDLCommand(viewAddress.objectId);

	/*
	 * We'd typically use NodeDDLTaskList() for generating node-level DDL commands,
	 * such as when creating a type. However, views are different in a sense that
	 * views do not depend on citus tables. Instead, they are `depending` on citus tables.
	 *
	 * When NodeDDLTaskList() used, it should be accompanied with sequential execution.
	 * Here, we do something equivalent to NodeDDLTaskList(), but using metadataSyncCommand
	 * field. This hack allows us to use the metadata connection
	 * (see `REQUIRE_METADATA_CONNECTION` flag). Meaning that, view creation is treated as
	 * a metadata operation.
	 *
	 * We do this mostly for performance reasons, because we cannot	afford to switch to
	 * sequential execution, for instance when we are altering or creating distributed
	 * tables -- which may require significant resources.
	 *
	 * The downside of using this hack is that if a view is re-used in the same transaction
	 * that creates the view on the workers, we might get errors such as the below which
	 * we consider a decent trade-off currently:
	 *
	 * BEGIN;
	 *      CREATE VIEW dist_view ..
	 *      CRETAE TABLE t2(id int, val dist_view);
	 *
	 *      -- shard creation fails on one of the connections
	 *      SELECT create_distributed_table('t2', 'id');
	 * ERROR: type "public.dist_view" does not exist
	 *
	 */
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetObjectAddress = viewAddress;
	ddlJob->metadataSyncCommand = command;
	ddlJob->taskList = NIL;

	return list_make1(ddlJob);
}


/*
 * ViewStmtObjectAddress returns the ObjectAddress for the subject of the
 * CREATE [OR REPLACE] VIEW statement.
 */
ObjectAddress
ViewStmtObjectAddress(Node *node, bool missing_ok)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->view, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessDropViewStmt gets called during the planning phase of a DROP VIEW statement
 * and returns a list of DDLJob's that will drop any distributed view from the
 * workers.
 *
 * The DropStmt could have multiple objects to drop, the list of objects will be filtered
 * to only keep the distributed views for deletion on the workers. Non-distributed
 * views will still be dropped locally but not on the workers.
 */
List *
PreprocessDropViewStmt(Node *node, const char *queryString, ProcessUtilityContext
					   processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	List *distributedViewNames = FilterNameListForDistributedViews(stmt->objects,
																   stmt->missing_ok);

	if (list_length(distributedViewNames) < 1)
	{
		/* no distributed view to drop */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_VIEW);

	/*
	 * Swap the list of objects before deparsing and restore the old list after. This
	 * ensures we only have distributed views in the deparsed drop statement.
	 */
	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedViewNames;

	QualifyTreeNode((Node *) stmtCopy);
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * FilterNameListForDistributedViews takes a list of view names and filters against the
 * views that are distributed.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok)
{
	List *distributedViewNames = NIL;

	List *possiblyQualifiedViewName = NULL;
	foreach_ptr(possiblyQualifiedViewName, viewNamesList)
	{
		char *viewName = NULL;
		char *schemaName = NULL;
		DeconstructQualifiedName(possiblyQualifiedViewName, &schemaName, &viewName);

		if (schemaName == NULL)
		{
			char *objName = NULL;
			Oid schemaOid = QualifiedNameGetCreationNamespace(possiblyQualifiedViewName,
															  &objName);
			schemaName = get_namespace_name(schemaOid);
		}

		Oid schemaId = get_namespace_oid(schemaName, missing_ok);
		Oid viewOid = get_relname_relid(viewName, schemaId);

		if (!OidIsValid(viewOid))
		{
			continue;
		}

		ObjectAddress viewAddress = { 0 };
		ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

		if (IsObjectDistributed(&viewAddress))
		{
			distributedViewNames = lappend(distributedViewNames,
										   possiblyQualifiedViewName);
		}
	}

	return distributedViewNames;
}


/*
 * CreateViewDDLCommand returns the DDL command to create the view addressed by
 * the viewAddress.
 */
char *
CreateViewDDLCommand(Oid viewOid)
{
	StringInfo createViewCommand = makeStringInfo();

	appendStringInfoString(createViewCommand, "CREATE OR REPLACE VIEW ");

	AppendQualifiedViewNameToCreateViewCommand(createViewCommand, viewOid);
	AppendAliasesToCreateViewCommand(createViewCommand, viewOid);
	AppendOptionsToCreateViewCommand(createViewCommand, viewOid);
	AppendViewDefinitionToCreateViewCommand(createViewCommand, viewOid);

	return createViewCommand->data;
}


/*
 * AppendQualifiedViewNameToCreateViewCommand adds the qualified view of the given view
 * oid to the given create view command.
 */
static void
AppendQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	char *viewName = get_rel_name(viewOid);
	char *schemaName = get_namespace_name(get_rel_namespace(viewOid));
	char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);

	appendStringInfo(buf, "%s ", qualifiedViewName);
}


/*
 * AppendAliasesToCreateViewCommand appends aliases to the create view
 * command for the existing view.
 */
static void
AppendAliasesToCreateViewCommand(StringInfo createViewCommand, Oid viewOid)
{
	/* Get column name aliases from pg_attribute */
	ScanKeyData key[1];
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(viewOid));

	Relation maprel = table_open(AttributeRelationId, AccessShareLock);
	Relation mapidx = index_open(AttributeRelidNumIndexId, AccessShareLock);
	SysScanDesc pgAttributeScan = systable_beginscan_ordered(maprel, mapidx, NULL, 1,
															 key);

	bool isInitialAlias = true;
	bool hasAlias = false;
	HeapTuple attributeTuple;
	while (HeapTupleIsValid(attributeTuple = systable_getnext_ordered(pgAttributeScan,
																	  ForwardScanDirection)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		const char *aliasName = quote_identifier(NameStr(att->attname));

		if (isInitialAlias)
		{
			appendStringInfoString(createViewCommand, "(");
		}
		else
		{
			appendStringInfoString(createViewCommand, ",");
		}

		appendStringInfoString(createViewCommand, aliasName);

		hasAlias = true;
		isInitialAlias = false;
	}

	if (hasAlias)
	{
		appendStringInfoString(createViewCommand, ") ");
	}

	systable_endscan_ordered(pgAttributeScan);
	index_close(mapidx, AccessShareLock);
	table_close(maprel, AccessShareLock);
}


/*
 * AppendOptionsToCreateViewCommand add relation options to create view command
 * for an existing view
 */
static void
AppendOptionsToCreateViewCommand(StringInfo createViewCommand, Oid viewOid)
{
	/* Add rel options to create view command */
	char *relOptions = flatten_reloptions(viewOid);
	if (relOptions != NULL)
	{
		appendStringInfo(createViewCommand, "WITH (%s) ", relOptions);
	}
}


/*
 * AppendViewDefinitionToCreateViewCommand adds the definition of the given view to the
 * given create view command.
 */
static void
AppendViewDefinitionToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed.
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/*
	 * Push the transaction snapshot to be able to get vief definition with pg_get_viewdef
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
													ObjectIdGetDatum(viewOid));
	char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);

	PopActiveSnapshot();
	PopOverrideSearchPath();

	appendStringInfo(buf, "AS %s ", viewDefinition);
}


/*
 * AlterViewOwnerCommand returns the command to alter view owner command for the
 * given view oid.
 */
char *
AlterViewOwnerCommand(Oid viewOid)
{
	/* Add alter owner commmand */
	StringInfo alterOwnerCommand = makeStringInfo();

	char *viewName = get_rel_name(viewOid);
	Oid schemaOid = get_rel_namespace(viewOid);
	char *schemaName = get_namespace_name(schemaOid);

	char *viewOwnerName = TableOwner(viewOid);
	char *qualifiedViewName = NameListToQuotedString(list_make2(makeString(schemaName),
																makeString(viewName)));
	appendStringInfo(alterOwnerCommand,
					 "ALTER VIEW %s OWNER TO %s", qualifiedViewName,
					 quote_identifier(viewOwnerName));

	return alterOwnerCommand->data;
}


/*
 * PreprocessAlterViewStmt is invoked for alter view statements.
 */
List *
PreprocessAlterViewStmt(Node *node, const char *queryString, ProcessUtilityContext
						processUtilityContext)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_VIEW);

	/* reconstruct alter statement in a portable fashion */
	const char *alterViewStmtSql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterViewStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterViewStmt is invoked for alter view statements.
 */
List *
PostprocessAlterViewStmt(Node *node, const char *queryString)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_VIEW);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	if (IsObjectAddressOwnedByExtension(&viewAddress, NULL))
	{
		return NIL;
	}

	/* If the view has any unsupported dependency, create it locally */
	if (ErrorOrWarnIfObjectHasUnsupportedDependency(&viewAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&viewAddress);

	return NIL;
}


/*
 * AlterViewStmtObjectAddress returns the ObjectAddress for the subject of the
 * ALTER VIEW statement.
 */
ObjectAddress
AlterViewStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessRenameViewStmt is called when the user is renaming the view or the column of
 * the view.
 */
List *
PreprocessRenameViewStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	ObjectAddress typeAddress = GetObjectAddressFromParseTree(node, true);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	/* fully qualify */
	QualifyTreeNode(node);

	/* deparse sql*/
	const char *renameStmtSql = DeparseTreeNode(node);

	EnsureSequentialMode(OBJECT_VIEW);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) renameStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * RenameViewStmtObjectAddress returns the ObjectAddress of the view that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameViewStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessAlterViewSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 */
List *
PreprocessAlterViewSchemaStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_VIEW);

	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterViewSchemaStmt is executed after the change has been applied locally, we
 * can now use the new dependencies of the view to ensure all its dependencies exist on
 * the workers before we apply the commands remotely.
 */
List *
PostprocessAlterViewSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, true);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&viewAddress);

	return NIL;
}


/*
 * AlterViewSchemaStmtObjectAddress returns the ObjectAddress of the view that is the object
 * of the alter schema statement.
 */
ObjectAddress
AlterViewSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, true);

	/*
	 * Since it can be called both before and after executing the standardProcess utility,
	 * we need to check both old and new schemas
	 */
	if (viewOid == InvalidOid)
	{
		Oid schemaId = get_namespace_oid(stmt->newschema, missing_ok);
		viewOid = get_relname_relid(stmt->relation->relname, schemaId);

		/*
		 * if the view is still invalid we couldn't find the view, error with the same
		 * message postgres would error with it missing_ok is false (not ok to miss)
		 */
		if (!missing_ok && viewOid == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("view \"%s\" does not exist",
								   stmt->relation->relname)));
		}
	}

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * IsViewRenameStmt returns whether the passed-in RenameStmt is the following
 * form:
 *
 *   - ALTER VIEW RENAME
 *   - ALTER VIEW RENAME COLUMN
 */
bool
IsViewRenameStmt(RenameStmt *renameStmt)
{
	bool isViewRenameStmt = false;

	if (renameStmt->renameType == OBJECT_VIEW ||
		(renameStmt->renameType == OBJECT_COLUMN &&
		 renameStmt->relationType == OBJECT_VIEW))
	{
		isViewRenameStmt = true;
	}

	return isViewRenameStmt;
}
