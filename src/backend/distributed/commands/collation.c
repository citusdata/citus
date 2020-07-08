/*-------------------------------------------------------------------------
 * collation.c
 *
 * This file contains functions to create, alter and drop policies on
 * distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/pg_version_constants.h"
#include "distributed/worker_manager.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "miscadmin.h"


static char * CreateCollationDDLInternal(Oid collationId, Oid *collowner,
										 char **quotedCollationName);
static List * FilterNameListForDistributedCollations(List *objects, bool missing_ok,
													 List **addresses);
static void EnsureSequentialModeForCollationDDL(void);


/*
 * GetCreateCollationDDLInternal returns a CREATE COLLATE sql string for the
 * given collationId.
 *
 * It includes 2 out parameters to assist creation of ALTER COLLATION OWNER.
 * quotedCollationName must not be NULL.
 */
static char *
CreateCollationDDLInternal(Oid collationId, Oid *collowner, char **quotedCollationName)
{
	char *schemaName = NULL;
	StringInfoData collationNameDef;
	const char *providerString = NULL;
	HeapTuple heapTuple = NULL;
	Form_pg_collation collationForm = NULL;
	char collprovider;
	const char *collcollate;
	const char *collctype;
	const char *collname;
	Oid collnamespace;
#if PG_VERSION_NUM >= PG_VERSION_12
	bool collisdeterministic;
#endif

	heapTuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationId));
	if (!HeapTupleIsValid(heapTuple))
	{
		elog(ERROR, "citus cache lookup failed for collation %u", collationId);
	}

	collationForm = (Form_pg_collation) GETSTRUCT(heapTuple);
	collprovider = collationForm->collprovider;
	collcollate = NameStr(collationForm->collcollate);
	collctype = NameStr(collationForm->collctype);
	collnamespace = collationForm->collnamespace;
	collname = NameStr(collationForm->collname);
#if PG_VERSION_NUM >= PG_VERSION_12
	collisdeterministic = collationForm->collisdeterministic;
#endif

	if (collowner != NULL)
	{
		*collowner = collationForm->collowner;
	}

	ReleaseSysCache(heapTuple);
	schemaName = get_namespace_name(collnamespace);
	*quotedCollationName = quote_qualified_identifier(schemaName, collname);
	providerString =
		collprovider == COLLPROVIDER_DEFAULT ? "default" :
		collprovider == COLLPROVIDER_ICU ? "icu" :
		collprovider == COLLPROVIDER_LIBC ? "libc" : NULL;

	if (providerString == NULL)
	{
		elog(ERROR, "unknown collation provider: %c", collprovider);
	}

	initStringInfo(&collationNameDef);
	appendStringInfo(&collationNameDef,
					 "CREATE COLLATION %s (provider = '%s'",
					 *quotedCollationName, providerString);

	if (strcmp(collcollate, collctype))
	{
		appendStringInfo(&collationNameDef,
						 ", locale = %s",
						 quote_literal_cstr(collcollate));
	}
	else
	{
		appendStringInfo(&collationNameDef,
						 ", lc_collate = %s, lc_ctype = %s",
						 quote_literal_cstr(collcollate),
						 quote_literal_cstr(collctype));
	}

#if PG_VERSION_NUM >= PG_VERSION_12
	if (!collisdeterministic)
	{
		appendStringInfoString(&collationNameDef, ", deterministic = false");
	}
#endif


	appendStringInfoChar(&collationNameDef, ')');
	return collationNameDef.data;
}


/*
 * CreateCollationDDL wrap CreateCollationDDLInternal to hide the out parameters.
 */
char *
CreateCollationDDL(Oid collationId)
{
	char *quotedCollationName = NULL;
	return CreateCollationDDLInternal(collationId, NULL, &quotedCollationName);
}


/*
 * CreateCollationDDLsIdempotent returns a List of cstrings for creating the collation
 * using create_or_replace_object & includes ALTER COLLATION ROLE.
 */
List *
CreateCollationDDLsIdempotent(Oid collationId)
{
	StringInfoData collationAlterOwnerCommand;
	Oid collowner = InvalidOid;
	char *quotedCollationName = NULL;
	char *createCollationCommand = CreateCollationDDLInternal(collationId, &collowner,
															  &quotedCollationName);

	initStringInfo(&collationAlterOwnerCommand);
	appendStringInfo(&collationAlterOwnerCommand,
					 "ALTER COLLATION %s OWNER TO %s",
					 quotedCollationName,
					 quote_identifier(GetUserNameFromId(collowner, false)));

	return list_make2(WrapCreateOrReplace(createCollationCommand),
					  collationAlterOwnerCommand.data);
}


ObjectAddress
AlterCollationOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Relation relation;

	Assert(stmt->objectType == OBJECT_COLLATION);

	return get_object_address(stmt->objectType, stmt->object, &relation,
							  AccessExclusiveLock, missing_ok);
}


/*
 * FilterNameListForDistributedCollations takes a list of objects to delete.
 * This list is filtered against the collations that are distributed.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 *
 * objectAddresses is replaced with a list of object addresses for the filtered objects.
 */
static List *
FilterNameListForDistributedCollations(List *objects, bool missing_ok,
									   List **objectAddresses)
{
	List *result = NIL;

	*objectAddresses = NIL;

	List *collName = NULL;
	foreach_ptr(collName, objects)
	{
		Oid collOid = get_collation_oid(collName, true);
		ObjectAddress collAddress = { 0 };

		if (!OidIsValid(collOid))
		{
			continue;
		}

		ObjectAddressSet(collAddress, CollationRelationId, collOid);
		if (IsObjectDistributed(&collAddress))
		{
			ObjectAddress *address = palloc0(sizeof(ObjectAddress));
			*address = collAddress;
			*objectAddresses = lappend(*objectAddresses, address);
			result = lappend(result, collName);
		}
	}
	return result;
}


List *
PreprocessDropCollationStmt(Node *node, const char *queryString)
{
	DropStmt *stmt = castNode(DropStmt, node);

	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *distributedTypeAddresses = NIL;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	List *oldCollations = stmt->objects;
	List *distributedCollations =
		FilterNameListForDistributedCollations(oldCollations, stmt->missing_ok,
											   &distributedTypeAddresses);
	if (list_length(distributedCollations) <= 0)
	{
		/* no distributed types to drop */
		return NIL;
	}

	/*
	 * managing collations can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * collations, so we block the call.
	 */
	EnsureCoordinator();

	/*
	 * remove the entries for the distributed objects on dropping
	 */
	ObjectAddress *addressItem = NULL;
	foreach_ptr(addressItem, distributedTypeAddresses)
	{
		UnmarkObjectDistributed(addressItem);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedCollations;
	char *dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = oldCollations;

	EnsureSequentialModeForCollationDDL();

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterCollationOwnerStmt is called for change of ownership of collations
 * before the ownership is changed on the local instance.
 *
 * If the type for which the owner is changed is distributed we execute the change on all
 * the workers to keep the type in sync across the cluster.
 */
List *
PreprocessAlterCollationOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_COLLATION);

	ObjectAddress collationAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&collationAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	char *sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForCollationDDL();
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessRenameCollationStmt is called when the user is renaming the collation. The invocation happens
 * before the statement is applied locally.
 *
 * As the collation already exists we have access to the ObjectAddress for the collation, this is
 * used to check if the collation is distributed. If the collation is distributed the rename is
 * executed on all the workers to keep the collation in sync across the cluster.
 */
List *
PreprocessRenameCollationStmt(Node *node, const char *queryString)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	ObjectAddress collationAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&collationAddress))
	{
		return NIL;
	}

	/* fully qualify */
	QualifyTreeNode((Node *) stmt);

	/* deparse sql*/
	char *renameStmtSql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForCollationDDL();

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) renameStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterCollationSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers.
 */
List *
PreprocessAlterCollationSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_COLLATION);

	ObjectAddress collationAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&collationAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	char *sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForCollationDDL();

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterCollationSchemaStmt is executed after the change has been applied locally, we
 * can now use the new dependencies of the type to ensure all its dependencies exist on
 * the workers before we apply the commands remotely.
 */
List *
PostprocessAlterCollationSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_COLLATION);

	ObjectAddress collationAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&collationAddress))
	{
		return NIL;
	}

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&collationAddress);

	return NIL;
}


/*
 * RenameCollationStmtObjectAddress returns the ObjectAddress of the type that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameCollationStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_COLLATION);

	Oid collationOid = get_collation_oid((List *) stmt->object, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collationOid);

	return address;
}


/*
 * AlterCollationSchemaStmtObjectAddress returns the ObjectAddress of the type that is the
 * subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 *
 * This could be called both before or after it has been applied locally. It will look in
 * the old schema first, if the type cannot be found in that schema it will look in the
 * new schema. Errors if missing_ok is false and the type cannot be found in either of the
 * schemas.
 */
ObjectAddress
AlterCollationSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_COLLATION);

	List *name = (List *) stmt->object;
	Oid collationOid = get_collation_oid(name, true);

	if (collationOid == InvalidOid)
	{
		List *newName = list_make2(makeString(stmt->newschema), lfirst(list_tail(name)));

		collationOid = get_collation_oid(newName, true);

		if (!missing_ok && collationOid == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("type \"%s\" does not exist",
								   NameListToString(name))));
		}
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collationOid);
	return address;
}


/*
 * EnsureSequentialModeForCollationDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the begining.
 *
 * As collations are node scoped objects there exists only 1 instance of the collation used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the type the type needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForCollationDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify collation because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a collation, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Collation is created or altered. To make sure subsequent "
							   "commands see the collation correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * GenerateBackupNameForCollationCollision generates a new collation name for an existing collation.
 * The name is generated in such a way that the new name doesn't overlap with an existing collation
 * by adding a suffix with incrementing number after the new name.
 */
char *
GenerateBackupNameForCollationCollision(const ObjectAddress *address)
{
	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	int count = 0;
	char *baseName = get_collation_name(address->objectId);
	int baseLength = strlen(baseName);
	HeapTuple collationTuple = SearchSysCache1(COLLOID, address->objectId);

	if (!HeapTupleIsValid(collationTuple))
	{
		elog(ERROR, "citus cache lookup failed");
		return NULL;
	}
	Form_pg_collation collationForm = (Form_pg_collation) GETSTRUCT(collationTuple);
	Value *namespace = makeString(get_namespace_name(collationForm->collnamespace));
	ReleaseSysCache(collationTuple);

	while (true)
	{
		int suffixLength = SafeSnprintf(suffix, NAMEDATALEN - 1, "(citus_backup_%d)",
										count);

		/* trim the base name at the end to leave space for the suffix and trailing \0 */
		baseLength = Min(baseLength, NAMEDATALEN - suffixLength - 1);

		/* clear newName before copying the potentially trimmed baseName and suffix */
		memset(newName, 0, NAMEDATALEN);
		strncpy_s(newName, NAMEDATALEN, baseName, baseLength);
		strncpy_s(newName + baseLength, NAMEDATALEN - baseLength, suffix,
				  suffixLength);

		List *newCollationName = list_make2(namespace, makeString(newName));

		/* don't need to rename if the input arguments don't match */
		Oid existingCollationId = get_collation_oid(newCollationName, true);

		if (existingCollationId == InvalidOid)
		{
			return newName;
		}

		count++;
	}
}


ObjectAddress
DefineCollationStmtObjectAddress(Node *node, bool missing_ok)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_COLLATION);

	Oid collOid = get_collation_oid(stmt->defnames, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collOid);

	return address;
}


/*
 * PostprocessDefineCollationStmt executed after the collation has been
 * created locally and before we create it on the worker nodes.
 * As we now have access to ObjectAddress of the collation that is just
 * created, we can mark it as distributed to make sure that its
 * dependencies exist on all nodes.
 */
List *
PostprocessDefineCollationStmt(Node *node, const char *queryString)
{
	Assert(castNode(DefineStmt, node)->kind == OBJECT_COLLATION);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/*
	 * If the create collation command is a part of a multi-statement transaction,
	 * do not propagate it
	 */
	if (IsMultiStatementTransaction())
	{
		return NIL;
	}

	ObjectAddress collationAddress =
		DefineCollationStmtObjectAddress(node, false);

	EnsureDependenciesExistOnAllNodes(&collationAddress);

	MarkObjectDistributed(&collationAddress);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, CreateCollationDDLsIdempotent(
							   collationAddress.objectId));
}
