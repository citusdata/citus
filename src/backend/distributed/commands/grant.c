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

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "commands/dbcommands.h"
#include "utils/formatting.h"
#include "distributed/metadata_sync.h"

/* define a constant for pg_class which has value 1259 */
#define PG_CLASS_ID_PG_CLASS 1259
#define PG_CLASS_ID_PG_DATABASE 1262

/* Local functions forward declarations for helper functions */
static void DeparsePrivileges(GrantStmt *grantStmt, StringInfoData privsString);
static void DeparseGrantees(GrantStmt *grantStmt, StringInfoData granteesString);
static void BuildGrantStmt(StringInfoData ddlString, GrantStmt *grantStmt, StringInfoData targetString,
						   StringInfoData privsString, StringInfoData granteesString);
static List *PrepareDDLJobsForTables(List *tableIdList, GrantStmt *grantStmt,
									 StringInfoData ddlString,
									 StringInfoData privsString, StringInfoData granteesString, StringInfoData targetString);
static List *PrepareDDLJobsForDatabases(List *databaseIdLists, GrantStmt *grantStmt,
										StringInfoData ddlString,
										StringInfoData privsString, StringInfoData granteesString, StringInfoData targetString);
static List *CollectGrantDatabaseNameList(GrantStmt *grantStmt);
static List *CollectGrantTableIdList(GrantStmt *grantStmt);

enum SupportedGrantTemplates
{
	GRANT_TABLE,
	REVOKE_TABLE,
	GRANT_DATABASE,
	REVOKE_DATABASE,
	NUM_SUPPORTED_GRANT_TEMPLATES
};

const char *SupportedGrantTemplates[] = {
	"GRANT %1$s ON %2$s TO %3$s %4$s",
	"REVOKE %4$s %1$s ON %2$s FROM %3$s",
	"GRANT %1$s ON DATABASE %2$s TO %3$s %4$s",
	"REVOKE %4$s %1$s ON DATABASE %2$s FROM %3$s"};

static void
DeparsePrivileges(GrantStmt *grantStmt, StringInfoData privsString)
{
	bool isFirst = true;

	/* deparse the privileges */
	if (grantStmt->privileges == NIL)
	{
		appendStringInfo(&privsString, "ALL");
	}
	else
	{
		ListCell *privilegeCell = NULL;

		isFirst = true;
		foreach (privilegeCell, grantStmt->privileges)
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
}

static void
DeparseGrantees(GrantStmt *grantStmt, StringInfoData granteesString)
{
	bool isFirst = true;
	ListCell *granteeCell = NULL;

	foreach (granteeCell, grantStmt->grantees)
	{
		RoleSpec *spec = lfirst(granteeCell);

		if (!isFirst)
		{
			appendStringInfoString(&granteesString, ", ");
		}
		isFirst = false;

		appendStringInfoString(&granteesString, RoleSpecString(spec, true));
	}
}

static void
BuildGrantStmt(StringInfoData ddlString, GrantStmt *grantStmt, StringInfoData targetString, StringInfoData privsString,
			   StringInfoData granteesString)
{
	const char *grantOption = "";

	const char *template = "";

	switch (grantStmt->objtype)
	{
	case OBJECT_DATABASE:
	{
		template = grantStmt->is_grant ? SupportedGrantTemplates[GRANT_DATABASE] : SupportedGrantTemplates[REVOKE_DATABASE];
		break;
	}

	case OBJECT_TABLE:
	{
		template = grantStmt->is_grant ? SupportedGrantTemplates[GRANT_TABLE] : SupportedGrantTemplates[REVOKE_TABLE];
		break;
	}

	default:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"grant/revoke only supported for DATABASE and TABLE list is currently "
							"unsupported")));
	}

	grantOption = grantStmt->grant_option ? (grantStmt->is_grant ? "WITH GRANT OPTION" : "GRANT OPTION FOR ") : "";

	appendStringInfo(&ddlString, template,
					 privsString.data, targetString.data, granteesString.data,
					 grantOption);
}

static List *
PrepareDDLJobsForTables(List *tableIdList, GrantStmt *grantStmt,
						StringInfoData ddlString,
						StringInfoData privsString, StringInfoData granteesString,
						StringInfoData targetString)
{
	ListCell *tableListCell = NULL;
	List *ddlJobs = NIL;

	foreach (tableListCell, tableIdList)
	{
		Oid relationId = lfirst_oid(tableListCell);

		resetStringInfo(&targetString);
		appendStringInfo(&targetString, "%s", generate_relation_name(relationId, NIL));

		BuildGrantStmt(ddlString, grantStmt, targetString, privsString, granteesString);

		DDLJob *ddlJob = palloc0(sizeof(DDLJob));
		do
		{
			(ddlJob->targetObjectAddress).classId = (PG_CLASS_ID_PG_CLASS);
			(ddlJob->targetObjectAddress).objectId = (relationId);
			(ddlJob->targetObjectAddress).objectSubId = (0);
		} while (0);
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

static List *
PrepareDDLJobsForDatabases(List *databaseIdLists, GrantStmt *grantStmt,
						   StringInfoData ddlString,
						   StringInfoData privsString, StringInfoData granteesString,
						   StringInfoData targetString)
{
	ListCell *databaseListCell = NULL;
	List *ddlJobs = NIL;
	foreach (databaseListCell, databaseIdLists)
	{
		Oid relationId = lfirst_oid(databaseListCell);

		resetStringInfo(&targetString);
		appendStringInfo(&targetString, "%s", get_database_name(relationId));

		BuildGrantStmt(ddlString, grantStmt, targetString, privsString, granteesString);

		char *sql = ddlString.data;

		List *commands = list_make3(DISABLE_DDL_PROPAGATION,
									(void *)sql ,
									ENABLE_DDL_PROPAGATION);

		ddlJobs = list_concat(ddlJobs,
							  NodeDDLTaskList(NON_COORDINATOR_NODES, commands));

	}
	return ddlJobs;
}



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
	List *ddlJobs = NIL;

	initStringInfo(&privsString);
	initStringInfo(&granteesString);
	initStringInfo(&targetString);
	initStringInfo(&ddlString);

	List *objectList = NIL;

	switch (grantStmt->objtype)
	{
	case OBJECT_DATABASE:
	{
		objectList = CollectGrantDatabaseNameList(grantStmt);
		break;
	}

	case OBJECT_TABLE:
	{
		objectList = CollectGrantTableIdList(grantStmt);
		break;
	}

	default:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"grant/revoke only supported for DATABASE and TABLE list is currently "
							"unsupported")));
	}

	/* nothing to do if there is no distributed table in the grant list */
	if (objectList == NIL)
	{
		ereport(ERROR, (errmsg("objectList == NIL")));
		return NIL;
	}

	/* deparse the privileges */
	DeparsePrivileges(grantStmt, privsString);

	/* deparse the grantees */
	DeparseGrantees(grantStmt, granteesString);

	/*
	 * Deparse the target objects, and issue the deparsed statements to
	 * workers, if applicable. That's so we easily can replicate statements
	 * only to distributed relations.
	 */
	if (grantStmt->objtype == OBJECT_DATABASE)
	{
		ddlJobs = PrepareDDLJobsForDatabases(objectList, grantStmt, ddlString, privsString,
											 granteesString, targetString);
	}
	else
	{
		ddlJobs = PrepareDDLJobsForTables(objectList, grantStmt, ddlString, privsString,
										  granteesString, targetString);
	}

	return ddlJobs;
}

static List *
CollectGrantDatabaseNameList(GrantStmt *grantStmt)
{
	List *grantDatabaseList = NIL;

	if (grantStmt->objtype != OBJECT_DATABASE)
	{
		return NIL;
	}

	ListCell *objectCell = NULL;
	foreach (objectCell, grantStmt->objects)
	{
		char *databaseName = strVal(lfirst(objectCell));
		bool missing_ok = false;
		Oid databaseOid = get_database_oid(databaseName, missing_ok);
		if (databaseOid == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database \"%s\" does not exist", databaseName)));
		}
		grantDatabaseList = lappend_oid(grantDatabaseList, databaseOid);
	}

	return grantDatabaseList;
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
		foreach (objectCell, grantStmt->objects)
		{
			char *nspname = strVal(lfirst(objectCell));
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			Assert(namespaceOid != InvalidOid);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		foreach (citusTableIdCell, citusTableIdList)
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
		foreach (objectCell, grantStmt->objects)
		{
			RangeVar *relvar = (RangeVar *)lfirst(objectCell);
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
