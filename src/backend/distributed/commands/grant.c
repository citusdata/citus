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
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/version_compat.h"


/* Local functions forward declarations for helper functions */
static List * CollectGrantTableIdList(GrantStmt *grantStmt);


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
