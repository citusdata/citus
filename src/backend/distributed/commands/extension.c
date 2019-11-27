/*-------------------------------------------------------------------------
 *
 * extension.c
 *    Commands for creating and altering extensions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "catalog/pg_extension_d.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/transaction_management.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parseTree);
static void AddSchemaFieldIfMissing(CreateExtensionStmt *stmt);
static List * FilterDistributedExtensions(List *extensionObjectList);
static List * ExtensionNameListToObjectAddressList(List *extensionObjectList);
static void EnsureSequentialModeForExtensionDDL(void);
static bool ShouldPropagateExtensionCommand(Node *parseTree);
static bool IsDropCitusStmt(Node *parseTree);
static bool IsAlterExtensionSetSchemaCitus(Node *parseTree);
static Node * RecreateExtensionStmt(Oid extensionOid);


/*
 * ErrorIfUnstableCreateOrAlterExtensionStmt compares CITUS_EXTENSIONVERSION
 * and version given CREATE/ALTER EXTENSION statement will create/update to. If
 * they are not same in major or minor version numbers, this function errors
 * out. It ignores the schema version.
 */
void
ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parseTree)
{
	char *newExtensionVersion = ExtractNewExtensionVersion(parseTree);

	if (newExtensionVersion != NULL)
	{
		/*  explicit version provided in CREATE or ALTER EXTENSION UPDATE; verify */
		if (!MajorVersionsCompatible(newExtensionVersion, CITUS_EXTENSIONVERSION))
		{
			ereport(ERROR, (errmsg("specified version incompatible with loaded "
								   "Citus library"),
							errdetail("Loaded library requires %s, but %s was specified.",
									  CITUS_MAJORVERSION, newExtensionVersion),
							errhint("If a newer library is present, restart the database "
									"and try the command again.")));
		}
	}
	else
	{
		/*
		 * No version was specified, so PostgreSQL will use the default_version
		 * from the citus.control file.
		 */
		CheckAvailableVersion(ERROR);
	}
}


/*
 * ExtractNewExtensionVersion returns the palloc'd new extension version specified
 * by a CREATE or ALTER EXTENSION statement. Other inputs are not permitted.
 * This function returns NULL for statements with no explicit version specified.
 */
static char *
ExtractNewExtensionVersion(Node *parseTree)
{
	List *optionsList = NIL;

	if (IsA(parseTree, CreateExtensionStmt))
	{
		optionsList = ((CreateExtensionStmt *) parseTree)->options;
	}
	else if (IsA(parseTree, AlterExtensionStmt))
	{
		optionsList = ((AlterExtensionStmt *) parseTree)->options;
	}
	else
	{
		/* input must be one of the two above types */
		Assert(false);
	}

	Value *newVersionValue = GetExtensionOption(optionsList, "new_version");

	/* return target string safely */
	if (newVersionValue)
	{
		const char *newVersion = strVal(newVersionValue);

		return pstrdup(newVersion);
	}
	else
	{
		return NULL;
	}
}


/*
 * PlanCreateExtensionStmt is called during the creation of an extension.
 * It is executed before the statement is applied locally.
 * We decide if the extension needs to be replicated to the worker, and
 * if that is the case return a list of DDLJob's that describe how and
 * where the extension needs to be created.
 */
List *
PlanCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt, const char *queryString)
{
	if (!ShouldPropagateExtensionCommand((Node *) createExtensionStmt))
	{
		return NIL;
	}

	/*
	 * If the extension command is a part of a bigger multi-statement transaction,
	 * do not propagate it
	 */
	if (IsMultiStatementTransaction())
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will have the extension, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	/*
	 * Here we append "schema" field to the "options" list (if not specified)
	 * to satisfy the schema consistency between worker nodes and the coordinator.
	 */
	AddSchemaFieldIfMissing(createExtensionStmt);

	const char *createExtensionStmtSql = DeparseTreeNode((Node *) createExtensionStmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) createExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * AddSchemaFieldIfMissing adds DefElem item for "schema" (if not specified
 * in statement) to "options" list before deparsing the statement to satisfy
 * the schema consistency between worker nodes and the coordinator.
 */
static void
AddSchemaFieldIfMissing(CreateExtensionStmt *createExtensionStmt)
{
	List *optionsList = createExtensionStmt->options;

	Value *schemaNameValue = GetExtensionOption(optionsList, "schema");

	if (!schemaNameValue)
	{
		/*
		 * As we already created the extension by standard_ProcessUtility,
		 * we actually know the schema it belongs to
		 */
		bool missingOk = false;
		Oid extensionOid = get_extension_oid(createExtensionStmt->extname, missingOk);
		Oid extensionSchemaOid = get_extension_schema(extensionOid);
		char *extensionSchemaName = get_namespace_name(extensionSchemaOid);

		Node *schemaNameArg = (Node *) makeString(extensionSchemaName);

		/* set location to -1 as it is unknown */
		int location = -1;

		DefElem *newDefElement = makeDefElem("schema", schemaNameArg, location);

		createExtensionStmt->options = lappend(createExtensionStmt->options,
											   newDefElement);
	}
}


/*
 * ProcessCreateExtensionStmt is executed after the extension has been
 * created locally and before we create it on the worker nodes.
 * As we now have access to ObjectAddress of the extension that is just
 * created, we can mark it as distributed to make sure that its
 * dependencies exist on all nodes.
 */
void
ProcessCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt, const
						   char *queryString)
{
	if (!ShouldPropagateExtensionCommand((Node *) createExtensionStmt))
	{
		return;
	}


	/*
	 * If the extension command is a part of a bigger multi-statement transaction,
	 * do not propagate it
	 */
	if (IsMultiStatementTransaction())
	{
		return;
	}

	const ObjectAddress *extensionAddress = GetObjectAddressFromParseTree(
		(Node *) createExtensionStmt, false);

	EnsureDependenciesExistsOnAllNodes(extensionAddress);

	MarkObjectDistributed(extensionAddress);
}


/*
 * PlanDropExtensionStmt is called to drop extension(s) in coordinator and
 * in worker nodes if distributed before.
 * We first ensure that we keep only the distributed ones before propagating
 * the statement to worker nodes.
 * If no extensions in the drop list are distributed, then no calls will
 * be made to the workers.
 */
List *
PlanDropExtensionStmt(DropStmt *dropStmt, const char *queryString)
{
	List *allDroppedExtensions = dropStmt->objects;


	ListCell *addressCell = NULL;

	if (!ShouldPropagateExtensionCommand((Node *) dropStmt))
	{
		return NIL;
	}

	/* get distributed extensions to be dropped in worker nodes as well */
	List *distributedExtensions = FilterDistributedExtensions(allDroppedExtensions);

	if (list_length(distributedExtensions) <= 0)
	{
		/* no distributed extensions to drop */
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will drop the extension, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	List *distributedExtensionAddresses = ExtensionNameListToObjectAddressList(
		distributedExtensions);

	/* unmark each distributed extension */
	foreach(addressCell, distributedExtensionAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * Temporary swap the lists of objects to delete with the distributed
	 * objects and deparse to an sql statement for the workers.
	 * Then switch back to allDroppedExtensions to drop all specified
	 * extensions in coordinator after PlanDropExtensionStmt completes
	 * its execution.
	 */
	dropStmt->objects = distributedExtensions;
	const char *deparsedStmt = DeparseTreeNode((Node *) dropStmt);

	dropStmt->objects = allDroppedExtensions;

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) deparsedStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * FilterDistributedExtensions returns the distributed objects in an "objects"
 * list of a DropStmt, a list having the format of a "DropStmt.objects" list.
 * That is, in turn, a list of string "Value"s.
 */
static List *
FilterDistributedExtensions(List *extensionObjectList)
{
	List *extensionNameList = NIL;

	bool missingOk = true;
	ListCell *objectCell = NULL;

	foreach(objectCell, extensionObjectList)
	{
		char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		if (!OidIsValid(extensionOid))
		{
			continue;
		}

		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

		if (!IsObjectDistributed(address))
		{
			continue;
		}

		extensionNameList = lappend(extensionNameList, makeString(extensionName));
	}

	return extensionNameList;
}


/*
 * ExtensionNameListToObjectAddressList returns the object addresses in
 * an ObjectAddress list for an "objects" list of a DropStmt.
 * Callers of this function should ensure that all the objects in the list
 * are valid and distributed.
 */
static List *
ExtensionNameListToObjectAddressList(List *extensionObjectList)
{
	List *extensionObjectAddressList = NIL;

	ListCell *objectCell = NULL;

	foreach(objectCell, extensionObjectList)
	{
		/*
		 * We set missingOk to false as we assume all the objects in
		 * extensionObjectList list are valid and distributed.
		 */
		bool missingOk = false;

		const char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

		extensionObjectAddressList = lappend(extensionObjectAddressList, address);
	}

	return extensionObjectAddressList;
}


/*
 * PlanAlterExtensionSchemaStmt is invoked for alter extension set schema statements.
 */
List *
PlanAlterExtensionSchemaStmt(AlterObjectSchemaStmt *alterExtensionStmt, const
							 char *queryString)
{
	if (!ShouldPropagateExtensionCommand((Node *) alterExtensionStmt))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will update the extension schema after
	 * this transaction finishes and the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	const char *alterExtensionStmtSql = DeparseTreeNode((Node *) alterExtensionStmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * ProcessAlterExtensionSchemaStmt is executed after the change has been applied
 * locally, we can now use the new dependencies (schema) of the extension to ensure
 * all its dependencies exist on the workers before we apply the commands remotely.
 */
void
ProcessAlterExtensionSchemaStmt(AlterObjectSchemaStmt *alterExtensionStmt, const
								char *queryString)
{
	const ObjectAddress *extensionAddress = GetObjectAddressFromParseTree(
		(Node *) alterExtensionStmt, false);

	if (!ShouldPropagateExtensionCommand((Node *) alterExtensionStmt))
	{
		return;
	}

	/* dependencies (schema) have changed let's ensure they exist */
	EnsureDependenciesExistsOnAllNodes(extensionAddress);
}


/*
 * PlanAlterExtensionUpdateStmt is invoked for alter extension update statements.
 */
List *
PlanAlterExtensionUpdateStmt(AlterExtensionStmt *alterExtensionStmt, const
							 char *queryString)
{
	if (!ShouldPropagateExtensionCommand((Node *) alterExtensionStmt))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will update the extension version, because
	 * they will either get it now, or get it in master_add_node after this transaction
	 * finishes and the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	const char *alterExtensionStmtSql = DeparseTreeNode((Node *) alterExtensionStmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * EnsureSequentialModeForExtensionDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginning.
 *
 * As extensions are node scoped objects there exists only 1 instance of the
 * extension used by potentially multiple shards. To make sure all shards in
 * the transaction can interact with the extension the extension needs to be
 * visible on all connections used by the transaction, meaning we can only use
 *  1 connection per node.
 */
static void
EnsureSequentialModeForExtensionDDL(void)
{
	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot run extension command because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail(
							"When running command on/for a distributed extension, Citus needs to "
							"perform all operations over a single connection per "
							"node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "A command for a distributed extension is run. To make sure subsequent "
						 "commands see the type correctly we need to make sure to "
						 "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * ShouldPropagateExtensionCommand determines whether to propagate an extension
 * command to the worker nodes.
 */
static bool
ShouldPropagateExtensionCommand(Node *parseTree)
{
	/* if we disabled object propagation, then we should not propagate anything. */
	if (!EnableDependencyCreation)
	{
		return false;
	}

	/*
	 * If extension command is run for/on citus, leave the rest to standard utility hook
	 * by returning false.
	 */
	if (IsCreateAlterExtensionUpdateCitusStmt(parseTree))
	{
		return false;
	}
	else if (IsDropCitusStmt(parseTree))
	{
		return false;
	}
	else if (IsAlterExtensionSetSchemaCitus(parseTree))
	{
		return false;
	}

	return true;
}


/*
 * IsCreateAlterExtensionUpdateCitusStmt returns whether a given utility is a
 * CREATE or ALTER EXTENSION UPDATE statement which references the citus extension.
 * This function returns false for all other inputs.
 */
bool
IsCreateAlterExtensionUpdateCitusStmt(Node *parseTree)
{
	const char *extensionName = NULL;

	if (IsA(parseTree, CreateExtensionStmt))
	{
		extensionName = ((CreateExtensionStmt *) parseTree)->extname;
	}
	else if (IsA(parseTree, AlterExtensionStmt))
	{
		extensionName = ((AlterExtensionStmt *) parseTree)->extname;
	}
	else
	{
		/*
		 * If it is not a Create Extension or a Alter Extension stmt,
		 * it does not matter if the it is about citus
		 */
		return false;
	}

	/*
	 * Now that we have CreateExtensionStmt or AlterExtensionStmt,
	 * check if it is run for/on citus
	 */
	return (strncasecmp(extensionName, "citus", NAMEDATALEN) == 0);
}


/*
 * IsDropCitusStmt iterates the objects to be dropped in a drop statement
 * and try to find citus there.
 */
static bool
IsDropCitusStmt(Node *parseTree)
{
	ListCell *objectCell = NULL;

	/* if it is not a DropStmt, it is needless to search for citus */
	if (!IsA(parseTree, DropStmt))
	{
		return false;
	}

	/* now that we have a DropStmt, check if citus is among the objects to dropped */
	foreach(objectCell, ((DropStmt *) parseTree)->objects)
	{
		const char *extensionName = strVal(lfirst(objectCell));

		if (strncasecmp(extensionName, "citus", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * IsAlterExtensionSetSchemaCitus returns whether a given utility is an
 * ALTER EXTENSION SET SCHEMA statement which references the citus extension.
 * This function returns false for all other inputs.
 */
static bool
IsAlterExtensionSetSchemaCitus(Node *parseTree)
{
	const char *extensionName = NULL;

	if (IsA(parseTree, AlterObjectSchemaStmt))
	{
		AlterObjectSchemaStmt *alterExtensionSetSchemaStmt =
			(AlterObjectSchemaStmt *) parseTree;

		if (alterExtensionSetSchemaStmt->objectType == OBJECT_EXTENSION)
		{
			extensionName = strVal(((AlterObjectSchemaStmt *) parseTree)->object);

			/*
			 * Now that we have AlterObjectSchemaStmt for an extension,
			 * check if it is run for/on citus
			 */
			return (strncasecmp(extensionName, "citus", NAMEDATALEN) == 0);
		}
	}

	return false;
}


/*
 * CreateExtensionDDLCommand returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the extension addressed by the extensionAddress.
 */
List *
CreateExtensionDDLCommand(const ObjectAddress *extensionAddress)
{
	/* generate a statement for creation of the extension in "if not exists" construct */
	Node *stmt = RecreateExtensionStmt(extensionAddress->objectId);

	/* capture ddl command for the create statement */
	const char *ddlCommand = DeparseTreeNode(stmt);

	List *ddlCommands = list_make1((void *) ddlCommand);

	return ddlCommands;
}


/*
 * RecreateEnumStmt returns a parsetree for a CREATE EXTENSION statement that would
 * recreate the given extension on a new node.
 */
static Node *
RecreateExtensionStmt(Oid extensionOid)
{
	CreateExtensionStmt *createExtensionStmt = makeNode(CreateExtensionStmt);

	char *extensionName = get_extension_name(extensionOid);

	if (!extensionName)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension with oid %u does not exist",
							   extensionOid)));
	}

	/* schema DefElement related variables */

	/* set location to -1 as it is unknown */
	int location = -1;

	/* set extension name and if_not_exists fields */
	createExtensionStmt->extname = extensionName;
	createExtensionStmt->if_not_exists = true;

	/* get schema name that extension was created on */
	Oid extensionSchemaOid = get_extension_schema(extensionOid);
	char *extensionSchemaName = get_namespace_name(extensionSchemaOid);

	/* make DefEleme for extensionSchemaName */
	Node *schemaNameArg = (Node *) makeString(extensionSchemaName);

	DefElem *schemaDefElement = makeDefElem("schema", schemaNameArg, location);

	/* append the schema name DefElem finally */
	createExtensionStmt->options = lappend(createExtensionStmt->options,
										   schemaDefElement);

	return (Node *) createExtensionStmt;
}


/*
 * AlterExtensionSchemaStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 */
ObjectAddress *
AlterExtensionSchemaStmtObjectAddress(AlterObjectSchemaStmt *alterExtensionSchemaStmt,
									  bool missing_ok)
{
	Assert(alterExtensionSchemaStmt->objectType == OBJECT_EXTENSION);

	const char *extensionName = strVal(alterExtensionSchemaStmt->object);

	Oid extensionOid = get_extension_oid(extensionName, missing_ok);

	if (extensionOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddress *extensionAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*extensionAddress, ExtensionRelationId, extensionOid);

	return extensionAddress;
}


/*
 * AlterExtensionUpdateStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterExtensionStmt. Errors if missing_ok is false.
 */
ObjectAddress *
AlterExtensionUpdateStmtObjectAddress(AlterExtensionStmt *alterExtensionStmt,
									  bool missing_ok)
{
	const char *extensionName = alterExtensionStmt->extname;

	Oid extensionOid = get_extension_oid(extensionName, missing_ok);

	if (extensionOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddress *extensionAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*extensionAddress, ExtensionRelationId, extensionOid);

	return extensionAddress;
}
