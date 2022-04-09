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
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_trigger.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/namespace_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* appropriate lock modes for the owner relation according to postgres */
#define CREATE_TRIGGER_LOCK_MODE ShareRowExclusiveLock
#define ALTER_TRIGGER_LOCK_MODE AccessExclusiveLock
#define DROP_TRIGGER_LOCK_MODE AccessExclusiveLock


/* local function forward declarations */
static bool IsCreateCitusTruncateTriggerStmt(CreateTrigStmt *createTriggerStmt);
static String * GetAlterTriggerDependsTriggerNameValue(AlterObjectDependsStmt *
													   alterTriggerDependsStmt);
static void ErrorIfUnsupportedDropTriggerCommand(DropStmt *dropTriggerStmt);
static RangeVar * GetDropTriggerStmtRelation(DropStmt *dropTriggerStmt);
static void ExtractDropStmtTriggerAndRelationName(DropStmt *dropTriggerStmt,
												  char **triggerName,
												  char **relationName);
static void ErrorIfDropStmtDropsMultipleTriggers(DropStmt *dropTriggerStmt);
static int16 GetTriggerTypeById(Oid triggerId);


/* GUC that overrides trigger checks for distributed tables and reference tables */
bool EnableUnsafeTriggers = false;


/*
 * GetExplicitTriggerCommandList returns the list of DDL commands to create
 * triggers that are explicitly created for the table with relationId. See
 * comment of GetExplicitTriggerIdList function.
 */
List *
GetExplicitTriggerCommandList(Oid relationId)
{
	List *createTriggerCommandList = NIL;

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	List *triggerIdList = GetExplicitTriggerIdList(relationId);

	Oid triggerId = InvalidOid;
	foreach_oid(triggerId, triggerIdList)
	{
		bool prettyOutput = false;
		Datum commandText = DirectFunctionCall2(pg_get_triggerdef_ext,
												ObjectIdGetDatum(triggerId),
												BoolGetDatum(prettyOutput));

		/*
		 * pg_get_triggerdef_ext doesn't throw an error if there is no such
		 * trigger, be on the safe side.
		 */
		if (DatumGetPointer(commandText) == NULL)
		{
			ereport(ERROR, (errmsg("trigger with oid %u does not exist",
								   triggerId)));
		}

		char *createTriggerCommand = TextDatumGetCString(commandText);

		createTriggerCommandList = lappend(
			createTriggerCommandList,
			makeTableDDLCommandString(createTriggerCommand));
	}

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return createTriggerCommandList;
}


/*
 * GetTriggerTupleById returns copy of the heap tuple from pg_trigger for
 * the trigger with triggerId. If no such trigger exists, this function returns
 * NULL or errors out depending on missingOk.
 */
HeapTuple
GetTriggerTupleById(Oid triggerId, bool missingOk)
{
	Relation pgTrigger = table_open(TriggerRelationId, AccessShareLock);

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	AttrNumber attrNumber = Anum_pg_trigger_oid;

	ScanKeyInit(&scanKey[0], attrNumber, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(triggerId));

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgTrigger, TriggerOidIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple targetHeapTuple = NULL;

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		targetHeapTuple = heap_copytuple(heapTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgTrigger, NoLock);

	if (targetHeapTuple == NULL && missingOk == false)
	{
		ereport(ERROR, (errmsg("could not find heap tuple for trigger with "
							   "OID %d", triggerId)));
	}

	return targetHeapTuple;
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

	Relation pgTrigger = table_open(TriggerRelationId, AccessShareLock);

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
			triggerIdList = lappend_oid(triggerIdList, triggerForm->oid);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgTrigger, NoLock);

	return triggerIdList;
}


/*
 * PostprocessCreateTriggerStmt is called after a CREATE TRIGGER command has
 * been executed by standard process utility. This function errors out for
 * unsupported commands or creates ddl job for supported CREATE TRIGGER commands.
 */
List *
PostprocessCreateTriggerStmt(Node *node, const char *queryString)
{
	CreateTrigStmt *createTriggerStmt = castNode(CreateTrigStmt, node);
	if (IsCreateCitusTruncateTriggerStmt(createTriggerStmt))
	{
		return NIL;
	}

	RangeVar *relation = createTriggerStmt->relation;
	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relation, CREATE_TRIGGER_LOCK_MODE, missingOk);

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	EnsureCoordinator();
	ErrorOutForTriggerIfNotSupported(relationId);

	ObjectAddress objectAddress = GetObjectAddressFromParseTree(node, missingOk);
	EnsureDependenciesExistOnAllNodes(&objectAddress);

	char *triggerName = createTriggerStmt->trigname;
	return CitusCreateTriggerCommandDDLJob(relationId, triggerName,
										   queryString);
}


/*
 * CreateTriggerStmtObjectAddress finds the ObjectAddress for the trigger that
 * is created by given CreateTriggerStmt. If missingOk is false and if trigger
 * does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
CreateTriggerStmtObjectAddress(Node *node, bool missingOk)
{
	CreateTrigStmt *createTriggerStmt = castNode(CreateTrigStmt, node);

	RangeVar *relation = createTriggerStmt->relation;
	Oid relationId = RangeVarGetRelid(relation, CREATE_TRIGGER_LOCK_MODE, missingOk);

	char *triggerName = createTriggerStmt->trigname;
	Oid triggerId = get_trigger_oid(relationId, triggerName, missingOk);

	if (triggerId == InvalidOid && missingOk == false)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("trigger \"%s\" on relation \"%s\" does not exist",
							   triggerName, relationName)));
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TriggerRelationId, triggerId);
	return address;
}


/*
 * IsCreateCitusTruncateTriggerStmt returns true if given createTriggerStmt
 * creates citus_truncate_trigger.
 */
static bool
IsCreateCitusTruncateTriggerStmt(CreateTrigStmt *createTriggerStmt)
{
	List *functionNameList = createTriggerStmt->funcname;
	RangeVar *functionRangeVar = makeRangeVarFromNameList(functionNameList);
	char *functionName = functionRangeVar->relname;
	if (strncmp(functionName, CITUS_TRUNCATE_TRIGGER_NAME, NAMEDATALEN) == 0)
	{
		return true;
	}

	return false;
}


/*
 * CreateTriggerEventExtendNames extends relation name and trigger name with
 * shardId, and sets schema name in given CreateTrigStmt.
 */
void
CreateTriggerEventExtendNames(CreateTrigStmt *createTriggerStmt, char *schemaName,
							  uint64 shardId)
{
	RangeVar *relation = createTriggerStmt->relation;

	char **relationName = &(relation->relname);
	AppendShardIdToName(relationName, shardId);

	char **triggerName = &(createTriggerStmt->trigname);
	AppendShardIdToName(triggerName, shardId);

	char **relationSchemaName = &(relation->schemaname);
	SetSchemaNameIfNotExist(relationSchemaName, schemaName);
}


/*
 * PostprocessAlterTriggerRenameStmt is called after a ALTER TRIGGER RENAME
 * command has been executed by standard process utility. This function errors
 * out for unsupported commands or creates ddl job for supported ALTER TRIGGER
 * RENAME commands.
 */
List *
PostprocessAlterTriggerRenameStmt(Node *node, const char *queryString)
{
	RenameStmt *renameTriggerStmt = castNode(RenameStmt, node);
	Assert(renameTriggerStmt->renameType == OBJECT_TRIGGER);

	RangeVar *relation = renameTriggerStmt->relation;

	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relation, ALTER_TRIGGER_LOCK_MODE, missingOk);

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	EnsureCoordinator();
	ErrorOutForTriggerIfNotSupported(relationId);

	/* use newname as standard process utility already renamed it */
	char *triggerName = renameTriggerStmt->newname;
	return CitusCreateTriggerCommandDDLJob(relationId, triggerName,
										   queryString);
}


/*
 * AlterTriggerRenameEventExtendNames extends relation name, old and new trigger
 * name with shardId, and sets schema name in given RenameStmt.
 */
void
AlterTriggerRenameEventExtendNames(RenameStmt *renameTriggerStmt, char *schemaName,
								   uint64 shardId)
{
	Assert(renameTriggerStmt->renameType == OBJECT_TRIGGER);

	RangeVar *relation = renameTriggerStmt->relation;

	char **relationName = &(relation->relname);
	AppendShardIdToName(relationName, shardId);

	char **triggerOldName = &(renameTriggerStmt->subname);
	AppendShardIdToName(triggerOldName, shardId);

	char **triggerNewName = &(renameTriggerStmt->newname);
	AppendShardIdToName(triggerNewName, shardId);

	char **relationSchemaName = &(relation->schemaname);
	SetSchemaNameIfNotExist(relationSchemaName, schemaName);
}


/*
 * PreprocessAlterTriggerDependsStmt is called during the planning phase of an
 * ALTER TRIGGER ... DEPENDS ON EXTENSION ... statement. Since triggers depending on
 * extensions are assumed to be Owned by an extension we assume the extension to keep
 * the trigger in sync.
 *
 * If we would allow users to create a dependency between a distributed trigger and an
 * extension, our pruning logic for which objects to distribute as dependencies of other
 * objects will change significantly, which could cause issues adding new workers. Hence
 * we don't allow this dependency to be created.
 */
List *
PreprocessAlterTriggerDependsStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	AlterObjectDependsStmt *alterTriggerDependsStmt =
		castNode(AlterObjectDependsStmt, node);
	Assert(alterTriggerDependsStmt->objectType == OBJECT_TRIGGER);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, triggers cascading
		 * from an extension should therefore not be propagated here.
		 */
		return NIL;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return NIL;
	}

	RangeVar *relation = alterTriggerDependsStmt->relation;

	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relation, ALTER_TRIGGER_LOCK_MODE, missingOk);

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	/*
	 * Distributed objects should not start depending on an extension, this will break
	 * the dependency resolving mechanism we use to replicate distributed objects to new
	 * workers
	 */

	String *triggerNameValue =
		GetAlterTriggerDependsTriggerNameValue(alterTriggerDependsStmt);
	ereport(ERROR, (errmsg(
						"Triggers \"%s\" on distributed tables and local tables added to metadata "
						"are not allowed to depend on an extension", strVal(
							triggerNameValue)),
					errdetail(
						"Triggers from extensions are expected to be created on the workers "
						"by the extension they depend on.")));
}


/*
 * PostprocessAlterTriggerDependsStmt is called after a ALTER TRIGGER DEPENDS ON
 * command has been executed by standard process utility. This function errors out
 * for unsupported commands or creates ddl job for supported ALTER TRIGGER DEPENDS
 * ON commands.
 */
List *
PostprocessAlterTriggerDependsStmt(Node *node, const char *queryString)
{
	AlterObjectDependsStmt *alterTriggerDependsStmt =
		castNode(AlterObjectDependsStmt, node);
	Assert(alterTriggerDependsStmt->objectType == OBJECT_TRIGGER);

	RangeVar *relation = alterTriggerDependsStmt->relation;

	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relation, ALTER_TRIGGER_LOCK_MODE, missingOk);

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	EnsureCoordinator();
	ErrorOutForTriggerIfNotSupported(relationId);

	String *triggerNameValue =
		GetAlterTriggerDependsTriggerNameValue(alterTriggerDependsStmt);
	return CitusCreateTriggerCommandDDLJob(relationId, strVal(triggerNameValue),
										   queryString);
}


/*
 * AlterTriggerDependsEventExtendNames extends relation name and trigger name
 * with shardId, and sets schema name in given AlterObjectDependsStmt.
 */
void
AlterTriggerDependsEventExtendNames(AlterObjectDependsStmt *alterTriggerDependsStmt,
									char *schemaName, uint64 shardId)
{
	Assert(alterTriggerDependsStmt->objectType == OBJECT_TRIGGER);

	RangeVar *relation = alterTriggerDependsStmt->relation;

	char **relationName = &(relation->relname);
	AppendShardIdToName(relationName, shardId);

	String *triggerNameValue =
		GetAlterTriggerDependsTriggerNameValue(alterTriggerDependsStmt);
	AppendShardIdToName(&strVal(triggerNameValue), shardId);

	char **relationSchemaName = &(relation->schemaname);
	SetSchemaNameIfNotExist(relationSchemaName, schemaName);
}


/*
 * GetAlterTriggerDependsTriggerName returns Value object for the trigger
 * name that given AlterObjectDependsStmt is executed for.
 */
static String *
GetAlterTriggerDependsTriggerNameValue(AlterObjectDependsStmt *alterTriggerDependsStmt)
{
	List *triggerObjectNameList = (List *) alterTriggerDependsStmt->object;

	/*
	 * Before standard process utility, we only have trigger name in "object"
	 * list. However, standard process utility prepends that list with the
	 * relationNameList retrieved from AlterObjectDependsStmt->RangeVar and
	 * we call this method after standard process utility. So, for the further
	 * usages, it is certain that the last element in "object" list will always
	 * be the name of the trigger in either before or after standard process
	 * utility.
	 */
	String *triggerNameValue = llast(triggerObjectNameList);
	return triggerNameValue;
}


/*
 * PreprocessDropTriggerStmt is called before a DROP TRIGGER command has been
 * executed by standard process utility. This function errors out for
 * unsupported commands or creates ddl job for supported DROP TRIGGER commands.
 * The reason we process drop trigger commands before standard process utility
 * (unlike the other type of trigger commands) is that we act according to trigger
 * type in CitusCreateTriggerCommandDDLJob but trigger wouldn't exist after
 * standard process utility.
 */
List *
PreprocessDropTriggerStmt(Node *node, const char *queryString,
						  ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropTriggerStmt = castNode(DropStmt, node);
	Assert(dropTriggerStmt->removeType == OBJECT_TRIGGER);

	RangeVar *relation = GetDropTriggerStmtRelation(dropTriggerStmt);

	bool missingOk = true;
	Oid relationId = RangeVarGetRelid(relation, DROP_TRIGGER_LOCK_MODE, missingOk);

	if (!OidIsValid(relationId))
	{
		/* let standard process utility to error out */
		return NIL;
	}

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	ErrorIfUnsupportedDropTriggerCommand(dropTriggerStmt);

	char *triggerName = NULL;
	ExtractDropStmtTriggerAndRelationName(dropTriggerStmt, &triggerName, NULL);
	return CitusCreateTriggerCommandDDLJob(relationId, triggerName,
										   queryString);
}


/*
 * ErrorIfUnsupportedDropTriggerCommand errors out for unsupported
 * "DROP TRIGGER triggerName ON relationName" commands.
 */
static void
ErrorIfUnsupportedDropTriggerCommand(DropStmt *dropTriggerStmt)
{
	RangeVar *relation = GetDropTriggerStmtRelation(dropTriggerStmt);

	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relation, DROP_TRIGGER_LOCK_MODE, missingOk);

	if (!IsCitusTable(relationId))
	{
		return;
	}

	EnsureCoordinator();
	ErrorOutForTriggerIfNotSupported(relationId);
}


/*
 * ErrorOutForTriggerIfNotSupported is a helper function to error
 * out for unsupported trigger commands depending on the citus table type.
 */
void
ErrorOutForTriggerIfNotSupported(Oid relationId)
{
	if (EnableUnsafeTriggers)
	{
		/* user really wants triggers */
		return;
	}
	else if (IsCitusTableType(relationId, REFERENCE_TABLE))
	{
		ereport(ERROR, (errmsg("triggers are not supported on reference tables")));
	}
	else if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
	{
		ereport(ERROR, (errmsg("triggers are not supported on distributed tables "
							   "when \"citus.enable_unsafe_triggers\" is set to "
							   "\"false\"")));
	}

	/* we always support triggers on citus local tables */
}


/*
 * GetDropTriggerStmtRelation takes a DropStmt for a trigger object and returns
 * RangeVar for the relation that owns the trigger.
 */
static RangeVar *
GetDropTriggerStmtRelation(DropStmt *dropTriggerStmt)
{
	Assert(dropTriggerStmt->removeType == OBJECT_TRIGGER);

	ErrorIfDropStmtDropsMultipleTriggers(dropTriggerStmt);

	List *targetObjectList = dropTriggerStmt->objects;
	List *triggerObjectNameList = linitial(targetObjectList);

	/*
	 * The name list that identifies the trigger to be dropped looks like:
	 * [catalogName, schemaName, relationName, triggerName], where, the first
	 * two elements are optional. We should take all elements except the
	 * triggerName to create the range var object that defines the owner
	 * relation.
	 */
	int relationNameListLength = list_length(triggerObjectNameList) - 1;
	List *relationNameList = list_truncate(list_copy(triggerObjectNameList),
										   relationNameListLength);

	return makeRangeVarFromNameList(relationNameList);
}


/*
 * DropTriggerEventExtendNames extends relation name and trigger name with
 * shardId, and sets schema name in given DropStmt by recreating "objects"
 * list.
 */
void
DropTriggerEventExtendNames(DropStmt *dropTriggerStmt, char *schemaName, uint64 shardId)
{
	Assert(dropTriggerStmt->removeType == OBJECT_TRIGGER);

	char *triggerName = NULL;
	char *relationName = NULL;
	ExtractDropStmtTriggerAndRelationName(dropTriggerStmt, &triggerName, &relationName);

	AppendShardIdToName(&triggerName, shardId);
	String *triggerNameValue = makeString(triggerName);

	AppendShardIdToName(&relationName, shardId);
	String *relationNameValue = makeString(relationName);

	String *schemaNameValue = makeString(pstrdup(schemaName));

	List *shardTriggerNameList =
		list_make3(schemaNameValue, relationNameValue, triggerNameValue);
	dropTriggerStmt->objects = list_make1(shardTriggerNameList);
}


/*
 * ExtractDropStmtTriggerAndRelationName extracts triggerName and relationName
 * from given dropTriggerStmt if arguments are passed as non-null pointers.
 */
static void
ExtractDropStmtTriggerAndRelationName(DropStmt *dropTriggerStmt, char **triggerName,
									  char **relationName)
{
	ErrorIfDropStmtDropsMultipleTriggers(dropTriggerStmt);

	List *targetObjectList = dropTriggerStmt->objects;
	List *triggerObjectNameList = linitial(targetObjectList);
	int objectNameListLength = list_length(triggerObjectNameList);

	if (triggerName != NULL)
	{
		int triggerNameindex = objectNameListLength - 1;
		*triggerName = strVal(safe_list_nth(triggerObjectNameList, triggerNameindex));
	}

	if (relationName != NULL)
	{
		int relationNameIndex = objectNameListLength - 2;
		*relationName = strVal(safe_list_nth(triggerObjectNameList, relationNameIndex));
	}
}


/*
 * ErrorIfDropStmtDropsMultipleTriggers errors out if given drop trigger
 * command drops more than one trigger. Actually, this can't be the case
 * as postgres doesn't support dropping multiple triggers, but we should
 * be on the safe side.
 */
static void
ErrorIfDropStmtDropsMultipleTriggers(DropStmt *dropTriggerStmt)
{
	List *targetObjectList = dropTriggerStmt->objects;
	if (list_length(targetObjectList) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cannot execute DROP TRIGGER command for multiple "
							   "triggers")));
	}
}


/*
 * CitusCreateTriggerCommandDDLJob creates a ddl job to execute given
 * queryString trigger command on shell relation(s) in mx worker(s) and to
 * execute necessary ddl task on citus local table shard (if needed).
 */
List *
CitusCreateTriggerCommandDDLJob(Oid relationId, char *triggerName,
								const char *queryString)
{
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = relationId;
	ddlJob->metadataSyncCommand = queryString;

	if (!triggerName)
	{
		/*
		 * ENABLE/DISABLE TRIGGER ALL/USER commands do not specify trigger
		 * name.
		 */
		ddlJob->taskList = DDLTaskList(relationId, queryString);
		return list_make1(ddlJob);
	}

	bool missingOk = true;
	Oid triggerId = get_trigger_oid(relationId, triggerName, missingOk);
	if (!OidIsValid(triggerId))
	{
		/*
		 * For DROP, ENABLE/DISABLE, ENABLE REPLICA/ALWAYS TRIGGER commands,
		 * we create ddl job in preprocess. So trigger may not exist.
		 */
		return NIL;
	}

	int16 triggerType = GetTriggerTypeById(triggerId);

	/* we don't have truncate triggers on shard relations */
	if (!TRIGGER_FOR_TRUNCATE(triggerType))
	{
		ddlJob->taskList = DDLTaskList(relationId, queryString);
	}

	return list_make1(ddlJob);
}


/*
 * GetTriggerTypeById returns trigger type (tgtype) of the trigger identified
 * by triggerId if it exists. Otherwise, errors out.
 */
static int16
GetTriggerTypeById(Oid triggerId)
{
	bool missingOk = false;
	HeapTuple triggerTuple = GetTriggerTupleById(triggerId, missingOk);

	Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);
	int16 triggerType = triggerForm->tgtype;
	heap_freetuple(triggerTuple);

	return triggerType;
}


/*
 * GetTriggerFunctionId returns OID of the function that the trigger with
 * triggerId executes if the trigger exists. Otherwise, errors out.
 */
Oid
GetTriggerFunctionId(Oid triggerId)
{
	bool missingOk = false;
	HeapTuple triggerTuple = GetTriggerTupleById(triggerId, missingOk);

	Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);
	Oid functionId = triggerForm->tgfoid;
	heap_freetuple(triggerTuple);

	return functionId;
}
