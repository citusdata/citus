#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_cimv.h"
#include "distributed/security_utils.h"
#include "executor/spi.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/cimv.h"

static void DropCimv(Form_pg_cimv formCimv, DropBehavior behavior);
static void DropDmlTriggers(Form_pg_cimv cimv);
static void DropCronJob(Form_pg_cimv cimv);

extern void
ProcessDropMaterializedViewStmt(DropStmt *stmt)
{
	if (stmt->removeType != OBJECT_MATVIEW)
	{
		return;
	}

	List *viewNameList = NULL;
	List *cimvObjects = NIL;
	List *formCimvs = NIL;
	Form_pg_cimv formCimv;
	bool hasCimv = false;
	bool hasNonCimv = false;
	foreach_ptr(viewNameList, stmt->objects)
	{
		RangeVar *viewRangeVar = makeRangeVarFromNameList(viewNameList);

		Oid relationId = RangeVarGetRelid(viewRangeVar, NoLock, true);

		if (relationId == InvalidOid)
		{
			hasNonCimv = true;
			continue;
		}

		formCimv = LookupCimvFromCatalog(relationId, true);

		if (formCimv == NULL)
		{
			hasNonCimv = true;
			continue;
		}

		hasCimv = true;

		cimvObjects = lappend(cimvObjects, list_make2(makeString(get_namespace_name(
																	 get_rel_namespace(
																		 formCimv->
																		 mattable))),
													  makeString(get_rel_name(
																	 formCimv
																	 ->
																	 mattable))));
		formCimvs = lappend(formCimvs, formCimv);
	}

	if (hasCimv && hasNonCimv)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"cannot drop regular and Citus materialized views with single command")));
	}

	if (hasCimv)
	{
		foreach_ptr(formCimv, formCimvs)
		{
			DropCimv(formCimv, stmt->behavior);
		}

		stmt->removeType = OBJECT_TABLE;
		stmt->objects = cimvObjects;
	}
}


extern void
ProcessDropViewStmt(DropStmt *stmt)
{
	if (stmt->removeType != OBJECT_VIEW)
	{
		return;
	}

	List *viewNameList = NULL;
	foreach_ptr(viewNameList, stmt->objects)
	{
		RangeVar *viewRangeVar = makeRangeVarFromNameList(viewNameList);

		Oid relationId = RangeVarGetRelid(viewRangeVar, NoLock, true);

		if (relationId == InvalidOid)
		{
			continue;
		}

		Form_pg_cimv formCimv = LookupCimvFromCatalog(relationId, true);

		if (formCimv != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("DROP VIEW not supported for %s",
								   viewRangeVar->relname),
							errhint("use DROP MATERIALIZED VIEW")));
		}
	}
}


static void
DropCimv(Form_pg_cimv formCimv, DropBehavior behavior)
{

	ObjectAddress matTableAddress;
	matTableAddress.classId = RelationRelationId;
	matTableAddress.objectId = formCimv->mattable;
	matTableAddress.objectSubId = 0;

	ObjectAddress userViewAddress;
	userViewAddress.classId = RelationRelationId;
	userViewAddress.objectId = formCimv->userview;
	userViewAddress.objectSubId = 0;

	ObjectAddress refreshViewAddress;
	refreshViewAddress.classId = RelationRelationId;
	refreshViewAddress.objectId = formCimv->refreshview;
	refreshViewAddress.objectSubId = 0;

	ObjectAddress landingTableAddress;
	landingTableAddress.classId = RelationRelationId;
	landingTableAddress.objectId = formCimv->landingtable;
	landingTableAddress.objectSubId = 0;


	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	DropCronJob(formCimv);
	DropDmlTriggers(formCimv);

	/* Lock */
	if (OidIsValid(matTableAddress.objectId))
	{
		AcquireDeletionLock(&matTableAddress, 0);
	}

	if (OidIsValid(userViewAddress.objectId))
	{
		AcquireDeletionLock(&userViewAddress, 0);
	}

	if (OidIsValid(refreshViewAddress.objectId))
	{
		AcquireDeletionLock(&refreshViewAddress, 0);
	}

	if (OidIsValid(landingTableAddress.objectId))
	{
		AcquireDeletionLock(&landingTableAddress, 0);
	}

	/* Drop views */
	if (OidIsValid(userViewAddress.objectId))
	{
		performDeletion(&userViewAddress, behavior, 0);
	}

	if (OidIsValid(refreshViewAddress.objectId))
	{
		performDeletion(&refreshViewAddress, behavior, 0);
	}

	/* Drop landing table */
	if (OidIsValid(landingTableAddress.objectId))
	{
		performDeletion(&landingTableAddress, behavior, 0);
	}

	DeletePgCimvRow(userViewAddress.objectId);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed");
	}
}


static void
DropDmlTriggers(Form_pg_cimv cimv)
{
	StringInfoData querybuf;
	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					 "DROP FUNCTION %s.%s CASCADE;",
					 quote_identifier(NameStr(cimv->triggerfnnamespace)),
					 quote_identifier(NameStr(cimv->triggerfnname)));

	if (SPI_execute(querybuf.data, false, 0) != SPI_OK_UTILITY)
	{
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	pfree(querybuf.data);
}


static void
DropCronJob(Form_pg_cimv cimv)
{
	if (cimv->jobid == 0)
	{
		return;
	}

	StringInfoData querybuf;
	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					 "SELECT * FROM cron.unschedule(" INT64_FORMAT ");",
					 cimv->jobid);

	int spiResult = SPI_execute(querybuf.data, false, 0);

	if (spiResult != SPI_OK_SELECT)
	{
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	pfree(querybuf.data);
}
