/*-------------------------------------------------------------------------
 *
 * grant.c
 *    Commands for granting access to distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"


/* Local functions forward declarations for helper functions */
static List * CollectGrantTableIdList(GrantStmt *grantStmt);

static List * CollectGrantTableRangeVarList(GrantStmt *grantStmt);
static List * PreprocessGrantStmtOnNodes(Node *node, const char *queryString,
										 ProcessUtilityContext processUtilityContext);
static List * PreprocessGrantStmtOnShards(Node *node, const char *queryString,
										  ProcessUtilityContext processUtilityContext);

/*
 * PreprocessGrantStmt determines whether a given GRANT/REVOKE statement involves
 * a distributed table. If so, it creates DDLJobs to encapsulate information
 * needed during the worker node portion of DDL execution before returning the
 * DDLJobs in a List. If no distributed table is involved, this returns NIL.
 *
 * NB: So far column level privileges are not supported.
 */
List *
PreprocessGrantStmt(Node *node, const char *queryString,
					ProcessUtilityContext processUtilityContext)
{
	GrantStmt *grantStmt = castNode(GrantStmt, node);
	StringInfoData privsString;
	StringInfoData granteesString;
	StringInfoData targetString;
	StringInfoData ddlString;
	ListCell *granteeCell = NULL;
	ListCell *tableListCell = NULL;
	bool isFirst = true;
	List *ddlJobs = NIL;

	initStringInfo(&privsString);
	initStringInfo(&granteesString);
	initStringInfo(&targetString);
	initStringInfo(&ddlString);

	/*
	 * So far only table level grants are supported. Most other types of
	 * grants aren't interesting anyway.
	 */
	if (grantStmt->objtype != OBJECT_TABLE)
	{
		return NIL;
	}

	List *tableIdList = CollectGrantTableIdList(grantStmt);

	/* nothing to do if there is no distributed table in the grant list */
	if (tableIdList == NIL)
	{
		return NIL;
	}

	/* deparse the privileges */
	if (grantStmt->privileges == NIL)
	{
		appendStringInfo(&privsString, "ALL");
	}
	else
	{
		ListCell *privilegeCell = NULL;

		isFirst = true;
		foreach(privilegeCell, grantStmt->privileges)
		{
			AccessPriv *priv = lfirst(privilegeCell);

			if (!isFirst)
			{
				appendStringInfoString(&privsString, ", ");
			}
			isFirst = false;

			if (priv->cols != NIL)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("grant/revoke on column list is currently "
									   "unsupported")));
			}

			Assert(priv->priv_name != NULL);

			appendStringInfo(&privsString, "%s", priv->priv_name);
		}
	}

	/* deparse the grantees */
	isFirst = true;
	foreach(granteeCell, grantStmt->grantees)
	{
		RoleSpec *spec = lfirst(granteeCell);

		if (!isFirst)
		{
			appendStringInfoString(&granteesString, ", ");
		}
		isFirst = false;

		appendStringInfoString(&granteesString, RoleSpecString(spec, true));
	}

	/*
	 * Deparse the target objects, and issue the deparsed statements to
	 * workers, if applicable. That's so we easily can replicate statements
	 * only to distributed relations.
	 */
	isFirst = true;
	foreach(tableListCell, tableIdList)
	{
		Oid relationId = lfirst_oid(tableListCell);
		const char *grantOption = "";

		resetStringInfo(&targetString);
		appendStringInfo(&targetString, "%s", generate_relation_name(relationId, NIL));

		if (grantStmt->is_grant)
		{
			if (grantStmt->grant_option)
			{
				grantOption = " WITH GRANT OPTION";
			}

			appendStringInfo(&ddlString, "GRANT %s ON %s TO %s%s",
							 privsString.data, targetString.data, granteesString.data,
							 grantOption);
		}
		else
		{
			if (grantStmt->grant_option)
			{
				grantOption = "GRANT OPTION FOR ";
			}

			appendStringInfo(&ddlString, "REVOKE %s%s ON %s FROM %s",
							 grantOption, privsString.data, targetString.data,
							 granteesString.data);
		}

		DDLJob *ddlJob = palloc0(sizeof(DDLJob));
		ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
		ddlJob->metadataSyncCommand = pstrdup(ddlString.data);
		ddlJob->taskList = NIL;
		if (IsCitusTable(relationId))
		{
			ddlJob->taskList = DDLTaskList(relationId, ddlString.data);
		}
		ddlJobs = lappend(ddlJobs, ddlJob);

		resetStringInfo(&ddlString);
	}

	return ddlJobs;
}


/*
 *  CollectGrantTableIdList determines and returns a list of distributed table
 *  Oids from grant statement.
 *  Grant statement may appear in two forms
 *  1 - grant on table:
 *      each distributed table oid in grant object list is added to returned list.
 *  2 - grant all tables in schema:
 *     Collect namespace oid list from grant statement
 *     Add each distributed table oid in the target namespace list to the returned list.
 */
static List *
CollectGrantTableIdList(GrantStmt *grantStmt)
{
	List *grantTableList = NIL;

	bool grantOnTableCommand = (grantStmt->targtype == ACL_TARGET_OBJECT &&
								grantStmt->objtype == OBJECT_TABLE);
	bool grantAllTablesOnSchemaCommand = (grantStmt->targtype ==
										  ACL_TARGET_ALL_IN_SCHEMA &&
										  grantStmt->objtype == OBJECT_TABLE);

	/* we are only interested in table level grants */
	if (!grantOnTableCommand && !grantAllTablesOnSchemaCommand)
	{
		return NIL;
	}

	if (grantAllTablesOnSchemaCommand)
	{
		List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
		ListCell *citusTableIdCell = NULL;
		List *namespaceOidList = NIL;

		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			char *nspname = strVal(lfirst(objectCell));
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			Assert(namespaceOid != InvalidOid);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		foreach(citusTableIdCell, citusTableIdList)
		{
			Oid relationId = lfirst_oid(citusTableIdCell);
			Oid namespaceOid = get_rel_namespace(relationId);
			if (list_member_oid(namespaceOidList, namespaceOid))
			{
				grantTableList = lappend_oid(grantTableList, relationId);
			}
		}
	}
	else
	{
		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			RangeVar *relvar = (RangeVar *) lfirst(objectCell);
			Oid relationId = RangeVarGetRelid(relvar, NoLock, false);
			if (IsCitusTable(relationId))
			{
				grantTableList = lappend_oid(grantTableList, relationId);
				continue;
			}

			/* check for distributed sequences included in GRANT ON TABLE statement */
			ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
			ObjectAddressSet(*sequenceAddress, RelationRelationId, relationId);
			if (IsAnyObjectDistributed(list_make1(sequenceAddress)))
			{
				grantTableList = lappend_oid(grantTableList, relationId);
			}
		}
	}

	return grantTableList;
}



/*
 * PreprocessGrantStmt determines whether a given GRANT/REVOKE statement involves
 * a distributed object. If so, it creates DDLJobs to encapsulate information
 * needed during the worker node portion of DDL execution before returning the
 * DDLJobs in a List. If no distributed object is involved, this returns NIL.
 *
 * NB: So far column level privileges are not supported.
 *
 */
List *
NewPreprocessGrantStmt(Node *node, const char *queryString,
					ProcessUtilityContext processUtilityContext)
{
	GrantStmt *grantStmt = castNode(GrantStmt, node);
	switch (grantStmt->objtype)
	{
		case OBJECT_AGGREGATE:
		case OBJECT_DATABASE:
		case OBJECT_FDW:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
		case OBJECT_SCHEMA:
			return PreprocessGrantStmtOnNodes(node, queryString, processUtilityContext);
			break;

		case OBJECT_SEQUENCE:
		case OBJECT_TABLE:
			return PreprocessGrantStmtOnShards(node, queryString, processUtilityContext);
			break;
		default:
			elog(WARNING, "grant TREE: %s", nodeToString(grantStmt));
			// elog(ERROR, "GRANT statement not managed by citus");
	}
	return NIL;
}


/*
 *  CollectGrantTableIdList determines and returns a list of distributed table
 *  RangeVar from grant statement.
 *  Grant statement may appear in two forms
 *  1 - grant on table:
 *      each distributed table oid in grant object list is added to returned list.
 *  2 - grant all tables in schema:
 *     Collect namespace oid list from grant statement
 *     Add each distributed table RangeVar in the target namespace list to the
 *     returned list.
 */
static List *
CollectGrantTableRangeVarList(GrantStmt *grantStmt)
{
	List *grantTableList = NIL;

	bool grantOnTableCommand = (grantStmt->targtype == ACL_TARGET_OBJECT &&
								grantStmt->objtype == OBJECT_TABLE);
	bool grantAllTablesOnSchemaCommand = (grantStmt->targtype ==
										  ACL_TARGET_ALL_IN_SCHEMA &&
										  grantStmt->objtype == OBJECT_TABLE);

	/* we are only interested in table level grants */
	if (!grantOnTableCommand && !grantAllTablesOnSchemaCommand)
	{
		return NIL;
	}

	if (grantAllTablesOnSchemaCommand)
	{
		List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
		ListCell *citusTableIdCell = NULL;
		List *namespaceOidList = NIL;

		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			char *nspname = strVal(lfirst(objectCell));
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			Assert(namespaceOid != InvalidOid);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		foreach(citusTableIdCell, citusTableIdList)
		{
			Oid relationId = lfirst_oid(citusTableIdCell);
			Oid namespaceOid = get_rel_namespace(relationId);
			if (list_member_oid(namespaceOidList, namespaceOid))
			{
				RangeVar *relvar = makeRangeVar(
					get_namespace_name(namespaceOid),
					get_rel_name(relationId), -1
				);
				grantTableList = lappend(grantTableList, relvar);
			}
		}
	}
	else
	{
		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			RangeVar *relvar = (RangeVar *) lfirst(objectCell);
			Oid relationId = RangeVarGetRelid(relvar, NoLock, false);
			if (IsCitusTable(relationId))
			{
				grantTableList = lappend(grantTableList, relvar);
				continue;
			}

			/* check for distributed sequences included in GRANT ON TABLE statement */
			ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
			ObjectAddressSet(*sequenceAddress, RelationRelationId, relationId);
			if (IsAnyObjectDistributed(list_make1(sequenceAddress)))
			{
				Oid namespaceOid = get_rel_namespace(relationId);
				RangeVar *relvarseq = makeRangeVar(
					get_namespace_name(namespaceOid),
					get_rel_name(relationId), -1
				);
				grantTableList = lappend(grantTableList, relvarseq);
			}
		}
	}

	return grantTableList;
}


/*
 * PreprocessGrantStmtOnNodes is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers.
 */
List *
PreprocessGrantStmtOnNodes(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	List *distributedObjects = NIL;
	GrantTargetType distributedGrantTargetType;
	List *distributedFunctions = NIL;
	ObjectAddress *functionAddress = NULL;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	GrantStmt *grantStmt = castNode(GrantStmt, node);
	bool includesDistributedServer;

	switch(grantStmt->objtype)
	{
		case OBJECT_DATABASE:
			distributedObjects = FilterDistributedDatabases(grantStmt->objects);
			break;

		case OBJECT_SCHEMA:
			distributedObjects = FilterDistributedSchemas(grantStmt->objects);
			break;

		case OBJECT_SEQUENCE:
			distributedGrantTargetType = ACL_TARGET_OBJECT;
			distributedObjects = FilterDistributedSequences(grantStmt);
			break;

		/*
		 * The FUNCTION syntax works for plain functions, aggregate functions, and window
		 * functions, but not for procedures; use PROCEDURE for those. Alternatively, use
		 * ROUTINE to refer to a function, aggregate function, window function, or procedure
		 * regardless of its precise type.
		 * https://www.postgresql.org/docs/current/sql-grant.html
		 */
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
			distributedFunctions = FilterDistributedFunctions(grantStmt);

			foreach_ptr(functionAddress, distributedFunctions)
			{
				ObjectWithArgs *distFunction = ObjectWithArgsFromOid(
					functionAddress->objectId);
				distributedObjects = lappend(distributedObjects, distFunction);
			}
			distributedGrantTargetType = ACL_TARGET_OBJECT;
			break;

		case OBJECT_FDW:
			if (!NameListHasFDWOwnedByDistributedExtension(grantStmt->objects))
			{
				/*
				* We propagate granted privileges on a FDW only if it belongs to a distributed
				* extension. For now, we skip for custom FDWs, as most of the users prefer
				* extension FDWs.
				*/
				return NIL;
			}

			if (list_length(grantStmt->objects) > 1)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot grant on distributed server with other servers"),
								errhint("Try granting on each object in separate commands")));
			}

			distributedObjects = grantStmt->objects;
			break;

		case OBJECT_FOREIGN_SERVER:
			includesDistributedServer = NameListHasDistributedServer(
												grantStmt->objects);
			if (!includesDistributedServer)
			{
				return NIL;
			}
			if (list_length(grantStmt->objects) > 1)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot grant on distributed server with other servers"),
								errhint("Try granting on each object in separate commands")));
			}

			distributedObjects = grantStmt->objects;
			break;

		case OBJECT_TABLESPACE:


		case OBJECT_COLUMN:
			/* we want to exclude system columns here */

		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_DOMAIN:
		case OBJECT_LANGUAGE:
		case OBJECT_LARGEOBJECT:
		case OBJECT_PARAMETER_ACL:
		case OBJECT_TYPE:
		case OBJECT_ROLE:

		default:
			elog(WARNING, "grant TREE: %s", nodeToString(grantStmt));
			// elog(ERROR, "GRANT statement on nodes not managed by citus");
			return NIL;
	}

	if (distributedObjects == NIL)
	{
		return NIL;
	}

	EnsureCoordinator();

	List *originalObjects = grantStmt->objects;
	GrantTargetType originalTargetType = grantStmt->targtype;

	grantStmt->objects = distributedObjects;
	if (originalTargetType != distributedGrantTargetType)
		grantStmt->targtype = distributedGrantTargetType;

	char *sql = DeparseTreeNode((Node *) grantStmt);
	// elog(WARNING, "deparsed grant for nodes: %s", sql);

	grantStmt->objects = originalObjects;
	grantStmt->targtype = originalTargetType;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessGrantStmtOnShards is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on object(s).
 */
List *
PreprocessGrantStmtOnShards(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	List *ddlJobs = NIL;
	List *distributedObjects = NIL;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	GrantStmt *grantStmt = castNode(GrantStmt, node);
	// elog(WARNING, "grant TREE: %s", nodeToString(stmt));

	switch(grantStmt->objtype)
	{
		case OBJECT_TABLE:
			distributedObjects = CollectGrantTableRangeVarList(grantStmt);
			break;
		default:
			return PreprocessGrantStmtOnNodes(node, queryString, processUtilityContext);
			break;
	}

	if (distributedObjects == NIL)
	{
		return NIL;
	}

	List *originalObjects = grantStmt->objects;
	ListCell *distributedObject;

	foreach(distributedObject, distributedObjects)
	{
		RangeVar *rangeVar = lfirst(distributedObject);
		List *tempRangeVarList = NIL;
		Oid relationId;

		tempRangeVarList = lappend(tempRangeVarList, rangeVar);
		relationId =  RangeVarGetRelid(rangeVar, NoLock, false);

		grantStmt->objects = tempRangeVarList;

		char *sql = DeparseTreeNode((Node *) grantStmt);

		grantStmt->objects = originalObjects;

		DDLJob *ddlJob = palloc0(sizeof(DDLJob));
		ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId,
						 (RangeVarGetRelid(rangeVar, NoLock, false)));
		ddlJob->metadataSyncCommand = pstrdup(sql);
		ddlJob->taskList = NIL;
		// elog(WARNING, "deparsed grant for shard: %s", sql);
		if (IsCitusTable(relationId))
		{
			ddlJob->taskList = DDLTaskList(relationId, sql);
		}
		ddlJobs = lappend(ddlJobs, ddlJob);

	}
	return ddlJobs;
}


/*
 * PostprocessGrantStmt makes sure dependencies of each
 * distributed object in the statement exist on all nodes
 */
List *
PostprocessGrantStmt(Node *node, const char *queryString)
{
	GrantStmt *grantStmt = castNode(GrantStmt, node);
	List *distributedObjects = NIL;

	switch (grantStmt->objtype)
	{
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
				EnsureCoordinator();
				distributedObjects = FilterDistributedFunctions(grantStmt);
				if (distributedObjects == NIL)
				{
					return NIL;
				}

				ObjectAddress *functionAddress = NULL;
				foreach_ptr(functionAddress, distributedObjects)
				{
					EnsureAllObjectDependenciesExistOnAllNodes(list_make1(functionAddress));
				}
				break;
		case OBJECT_SEQUENCE:
				EnsureCoordinator();
				distributedObjects = FilterDistributedSequences(grantStmt);
				if (distributedObjects == NIL)
				{
					return NIL;
				}

				RangeVar *sequence = NULL;
				foreach_ptr(sequence, distributedObjects)
				{
					ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
					Oid sequenceOid = RangeVarGetRelid(sequence, NoLock, false);
					ObjectAddressSet(*sequenceAddress, RelationRelationId, sequenceOid);
					EnsureAllObjectDependenciesExistOnAllNodes(list_make1(sequenceAddress));
				}
			break;
		default:
			break;
	}
	return NIL;
}
