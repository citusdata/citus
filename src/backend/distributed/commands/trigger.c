/*-------------------------------------------------------------------------
 * trigger.c
 *
 * This file contains functions to create and process trigger objects on
 * citus tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "access/table.h"
#else
#include "access/heapam.h"
#include "access/htup_details.h"
#endif
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_trigger.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

/*
 * GetExplicitTriggerCommandList returns the list of DDL commands to create
 * triggers that are explicitly created for the table with relationId. See
 * comment of GetExplicitTriggerIdList function.
 */
List *
GetExplicitTriggerCommandList(Oid relationId)
{
	List *createTriggerCommandList = NIL;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	List *triggerIdList = GetExplicitTriggerIdList(relationId);

	Oid triggerId = InvalidOid;
	foreach_oid(triggerId, triggerIdList)
	{
		char *createTriggerCommand = pg_get_triggerdef_command(triggerId);

		createTriggerCommandList = lappend(createTriggerCommandList,
										   createTriggerCommand);
	}

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return createTriggerCommandList;
}


/*
 * GetExplicitTriggerIdList returns a list of OIDs corresponding to the triggers
 * that are explicitly created on the relation with relationId. That means,
 * this function discards internal triggers implicitly created by postgres for
 * foreign key constraint validation and the citus_truncate_trigger.
 */
List *
GetExplicitTriggerIdList(Oid relationId)
{
	List *triggerIdList = NIL;

	Relation pgTrigger = heap_open(TriggerRelationId, AccessShareLock);

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgTrigger, TriggerRelidNameIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(heapTuple);

		/*
		 * Note that we mark truncate trigger that we create on citus tables as
		 * internal. Hence, below we discard citus_truncate_trigger as well as
		 * the implicit triggers created by postgres for foreign key validation.
		 */
		if (!triggerForm->tgisinternal)
		{
			Oid triggerId = get_relation_trigger_oid_compat(heapTuple);
			triggerIdList = lappend_oid(triggerIdList, triggerId);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgTrigger, NoLock);

	return triggerIdList;
}


/*
 * get_relation_trigger_oid_compat returns OID of the trigger represented
 * by the constraintForm, which is passed as an heapTuple. OID of the
 * trigger is already stored in the triggerForm struct if major PostgreSQL
 * version is 12. However, in the older versions, we should utilize
 * HeapTupleGetOid to deduce that OID with no cost.
 */
Oid
get_relation_trigger_oid_compat(HeapTuple heapTuple)
{
	Assert(HeapTupleIsValid(heapTuple));

	Oid triggerOid = InvalidOid;

#if PG_VERSION_NUM >= PG_VERSION_12
	Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(heapTuple);
	triggerOid = triggerForm->oid;
#else
	triggerOid = HeapTupleGetOid(heapTuple);
#endif

	return triggerOid;
}
