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
#include "commands/defrem.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
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
static void MarkExistingObjectDependenciesDistributedIfSupported(void);
static bool ShouldPropagateExtensionCommand(Node *parseTree);
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

	DefElem *newVersionValue = GetExtensionOption(optionsList, "new_version");

	/* return target string safely */
	if (newVersionValue)
	{
		const char *newVersion = defGetString(newVersionValue);
		return pstrdup(newVersion);
	}
	else
	{
		return NULL;
	}
}


/*
 * PostprocessCreateExtensionStmt is called after the creation of an extension.
 * We decide if the extension needs to be replicated to the worker, and
 * if that is the case return a list of DDLJob's that describe how and
 * where the extension needs to be created.
 *
 * As we now have access to ObjectAddress of the extension that is just
 * created, we can mark it as distributed to make sure that its
 * dependencies exist on all nodes.
 */
List *
PostprocessCreateExtensionStmt(Node *node, const char *queryString)
{
	CreateExtensionStmt *stmt = castNode(CreateExtensionStmt, node);

	if (!ShouldPropagateExtensionCommand(node))
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialMode(OBJECT_EXTENSION);

	/*
	 * Here we append "schema" field to the "options" list (if not specified)
	 * to satisfy the schema consistency between worker nodes and the coordinator.
	 */
	AddSchemaFieldIfMissing(stmt);

	/* always send commands with IF NOT EXISTS */
	stmt->if_not_exists = true;

	const char *createExtensionStmtSql = DeparseTreeNode(node);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) createExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	ObjectAddress extensionAddress = GetObjectAddressFromParseTree(node, false);

	EnsureDependenciesExistOnAllNodes(&extensionAddress);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
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

	DefElem *schemaNameValue = GetExtensionOption(optionsList, "schema");

	if (!schemaNameValue)
	{
		/*
		 * As we already created the extension by standard_ProcessUtility,
		 * we actually know the schema it belongs to
		 */
		const bool missingOk = false;
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
 * PreprocessDropExtensionStmt is called to drop extension(s) in coordinator and
 * in worker nodes if distributed before.
 * We first ensure that we keep only the distributed ones before propagating
 * the statement to worker nodes.
 * If no extensions in the drop list are distributed, then no calls will
 * be made to the workers.
 */
List *
PreprocessDropExtensionStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);

	if (!ShouldPropagateExtensionCommand(node))
	{
		return NIL;
	}

	/* get distributed extensions to be dropped in worker nodes as well */
	List *allDroppedExtensions = stmt->objects;
	List *distributedExtensions = FilterDistributedExtensions(allDroppedExtensions);

	if (list_length(distributedExtensions) <= 0)
	{
		/* no distributed extensions to drop */
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialMode(OBJECT_EXTENSION);

	List *distributedExtensionAddresses = ExtensionNameListToObjectAddressList(
		distributedExtensions);

	/* unmark each distributed extension */
	ObjectAddress *address = NULL;
	foreach_ptr(address, distributedExtensionAddresses)
	{
		UnmarkObjectDistributed(address);
	}

	/*
	 * Temporary swap the lists of objects to delete with the distributed
	 * objects and deparse to an sql statement for the workers.
	 * Then switch back to allDroppedExtensions to drop all specified
	 * extensions in coordinator after PreprocessDropExtensionStmt completes
	 * its execution.
	 */
	stmt->objects = distributedExtensions;
	const char *deparsedStmt = DeparseTreeNode((Node *) stmt);
	stmt->objects = allDroppedExtensions;

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) deparsedStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
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

	String *objectName = NULL;
	foreach_ptr(objectName, extensionObjectList)
	{
		const char *extensionName = strVal(objectName);
		const bool missingOk = true;

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		if (!OidIsValid(extensionOid))
		{
			continue;
		}

		ObjectAddress address = { 0 };
		ObjectAddressSet(address, ExtensionRelationId, extensionOid);

		if (!IsObjectDistributed(&address))
		{
			continue;
		}

		extensionNameList = lappend(extensionNameList, objectName);
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

	String *objectName;
	foreach_ptr(objectName, extensionObjectList)
	{
		/*
		 * We set missingOk to false as we assume all the objects in
		 * extensionObjectList list are valid and distributed.
		 */
		const char *extensionName = strVal(objectName);
		const bool missingOk = false;

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

		extensionObjectAddressList = lappend(extensionObjectAddressList, address);
	}

	return extensionObjectAddressList;
}


/*
 * PreprocessAlterExtensionSchemaStmt is invoked for alter extension set schema statements.
 */
List *
PreprocessAlterExtensionSchemaStmt(Node *node, const char *queryString,
								   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagateExtensionCommand(node))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialMode(OBJECT_EXTENSION);

	const char *alterExtensionStmtSql = DeparseTreeNode(node);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterExtensionSchemaStmt is executed after the change has been applied
 * locally, we can now use the new dependencies (schema) of the extension to ensure
 * all its dependencies exist on the workers before we apply the commands remotely.
 */
List *
PostprocessAlterExtensionSchemaStmt(Node *node, const char *queryString)
{
	ObjectAddress extensionAddress = GetObjectAddressFromParseTree(node, false);

	if (!ShouldPropagateExtensionCommand(node))
	{
		return NIL;
	}

	/* dependencies (schema) have changed let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&extensionAddress);

	return NIL;
}


/*
 * PreprocessAlterExtensionUpdateStmt is invoked for alter extension update statements.
 */
List *
PreprocessAlterExtensionUpdateStmt(Node *node, const char *queryString,
								   ProcessUtilityContext processUtilityContext)
{
	AlterExtensionStmt *alterExtensionStmt = castNode(AlterExtensionStmt, node);

	if (!ShouldPropagateExtensionCommand((Node *) alterExtensionStmt))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialMode(OBJECT_EXTENSION);

	const char *alterExtensionStmtSql = DeparseTreeNode((Node *) alterExtensionStmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterExtensionStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterExtensionCitusUpdateStmt is called after ALTER EXTENSION
 * citus UPDATE command is executed by standard utility hook.
 *
 * Actually, we do not need such a post process function for ALTER EXTENSION
 * UPDATE commands unless the extension is Citus itself. This is because we
 * need to mark existing objects that are not included in distributed object
 * infrastructure in older versions of Citus (but now should be) as distributed
 * if we really updated Citus to the available version successfully via standard
 * utility hook.
 */
void
PostprocessAlterExtensionCitusUpdateStmt(Node *node)
{
	bool citusIsUpdatedToLatestVersion = InstalledAndAvailableVersionsSame();

	/*
	 * Knowing that Citus version was different than the available version before
	 * standard process utility runs ALTER EXTENSION command, we perform post
	 * process operations if Citus is updated to that available version
	 */
	if (!citusIsUpdatedToLatestVersion)
	{
		return;
	}

	/*
	 * Finally, mark existing objects that are not included in distributed object
	 * infrastructure in older versions of Citus (but now should be) as distributed
	 */
	MarkExistingObjectDependenciesDistributedIfSupported();
}


/*
 * MarkExistingObjectDependenciesDistributedIfSupported marks all objects that could
 * be distributed by resolving dependencies of "existing distributed tables" and
 * "already distributed objects" to introduce the objects created in older versions
 * of Citus to distributed object infrastructure as well.
 *
 * Note that this function is not responsible for ensuring if dependencies exist on
 * nodes and satisfying these dependendencies if not exists, which is already done by
 * EnsureDependenciesExistOnAllNodes on demand. Hence, this function is just designed
 * to be used when "ALTER EXTENSION citus UPDATE" is executed.
 * This is because we want to add existing objects that would have already been in
 * pg_dist_object if we had created them in new version of Citus to pg_dist_object.
 */
static void
MarkExistingObjectDependenciesDistributedIfSupported()
{
	/* resulting object addresses to be marked as distributed */
	List *resultingObjectAddresses = NIL;

	/* resolve dependencies of citus tables */
	List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);

	Oid citusTableId = InvalidOid;
	foreach_oid(citusTableId, citusTableIdList)
	{
		ObjectAddress tableAddress = { 0 };
		ObjectAddressSet(tableAddress, RelationRelationId, citusTableId);

		if (ShouldSyncTableMetadata(citusTableId))
		{
			/* we need to pass pointer allocated in the heap */
			ObjectAddress *addressPointer = palloc0(sizeof(ObjectAddress));
			*addressPointer = tableAddress;

			/* as of Citus 11, tables that should be synced are also considered object */
			resultingObjectAddresses = lappend(resultingObjectAddresses, addressPointer);
		}

		List *distributableDependencyObjectAddresses =
			GetDistributableDependenciesForObject(&tableAddress);

		resultingObjectAddresses = list_concat(resultingObjectAddresses,
											   distributableDependencyObjectAddresses);
	}

	/* resolve dependencies of the objects in pg_dist_object*/
	List *distributedObjectAddressList = GetDistributedObjectAddressList();

	ObjectAddress *distributedObjectAddress = NULL;
	foreach_ptr(distributedObjectAddress, distributedObjectAddressList)
	{
		List *distributableDependencyObjectAddresses =
			GetDistributableDependenciesForObject(distributedObjectAddress);

		resultingObjectAddresses = list_concat(resultingObjectAddresses,
											   distributableDependencyObjectAddresses);
	}

	/* remove duplicates from object addresses list for efficiency */
	List *uniqueObjectAddresses = GetUniqueDependenciesList(resultingObjectAddresses);

	/*
	 * We should sync the new dependencies during ALTER EXTENSION because
	 * we cannot know whether the nodes has already been upgraded or not. If
	 * the nodes are not upgraded at this point, we cannot sync the object. Also,
	 * when the workers upgraded, they'd get the same objects anyway.
	 */
	bool prevMetadataSyncValue = EnableMetadataSync;
	SetLocalEnableMetadataSync(false);

	ObjectAddress *objectAddress = NULL;
	foreach_ptr(objectAddress, uniqueObjectAddresses)
	{
		MarkObjectDistributed(objectAddress);
	}

	SetLocalEnableMetadataSync(prevMetadataSyncValue);
}


/*
 * PreprocessAlterExtensionContentsStmt issues a notice. It does not propagate.
 */
List *
PreprocessAlterExtensionContentsStmt(Node *node, const char *queryString,
									 ProcessUtilityContext processUtilityContext)
{
	ereport(NOTICE, (errmsg(
						 "Citus does not propagate adding/dropping member objects"),
					 errhint(
						 "You can add/drop the member objects on the workers as well.")));

	return NIL;
}


/*
 * ShouldPropagateExtensionCommand determines whether to propagate an extension
 * command to the worker nodes.
 */
static bool
ShouldPropagateExtensionCommand(Node *parseTree)
{
	/* if we disabled object propagation, then we should not propagate anything. */
	if (!EnableMetadataSync)
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
	else if (IsDropCitusExtensionStmt(parseTree))
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
 * IsDropCitusExtensionStmt iterates the objects to be dropped in a drop statement
 * and try to find citus extension there.
 */
bool
IsDropCitusExtensionStmt(Node *parseTree)
{
	/* if it is not a DropStmt, it is needless to search for citus */
	if (!IsA(parseTree, DropStmt))
	{
		return false;
	}
	DropStmt *dropStmt = (DropStmt *) parseTree;

	/* check if the drop command is a DROP EXTENSION command */
	if (dropStmt->removeType != OBJECT_EXTENSION)
	{
		return false;
	}

	/* now that we have a DropStmt, check if citus extension is among the objects to dropped */
	String *objectName;
	foreach_ptr(objectName, dropStmt->objects)
	{
		const char *extensionName = strVal(objectName);

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
 * RecreateExtensionStmt returns a parsetree for a CREATE EXTENSION statement that would
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

	char *extensionVersion = get_extension_version(extensionOid);
	if (extensionVersion != NULL)
	{
		Node *extensionVersionArg = (Node *) makeString(extensionVersion);
		DefElem *extensionVersionElement =
			makeDefElem("new_version", extensionVersionArg, location);

		createExtensionStmt->options = lappend(createExtensionStmt->options,
											   extensionVersionElement);
	}

	return (Node *) createExtensionStmt;
}


/*
 * AlterExtensionSchemaStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterExtensionSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_EXTENSION);

	const char *extensionName = strVal(stmt->object);

	Oid extensionOid = get_extension_oid(extensionName, missing_ok);

	if (extensionOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ExtensionRelationId, extensionOid);

	return address;
}


/*
 * AlterExtensionUpdateStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterExtensionStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterExtensionUpdateStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterExtensionStmt *stmt = castNode(AlterExtensionStmt, node);
	const char *extensionName = stmt->extname;

	Oid extensionOid = get_extension_oid(extensionName, missing_ok);

	if (extensionOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ExtensionRelationId, extensionOid);

	return address;
}
