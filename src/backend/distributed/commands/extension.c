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

#include "access/genam.h"
#include "access/xact.h"
#include "citus_version.h"
#include "catalog/dependency.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension_d.h"
#include "columnar/columnar.h"
#include "catalog/pg_foreign_data_wrapper.h"
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
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parseTree);
static void AddSchemaFieldIfMissing(CreateExtensionStmt *stmt);
static List * FilterDistributedExtensions(List *extensionObjectList);
static List * ExtensionNameListToObjectAddressList(List *extensionObjectList);
static void MarkExistingObjectDependenciesDistributedIfSupported(void);
static List * GetAllViews(void);
static bool ShouldPropagateExtensionCommand(Node *parseTree);
static bool IsAlterExtensionSetSchemaCitus(Node *parseTree);
static Node * RecreateExtensionStmt(Oid extensionOid);
static List * GenerateGrantCommandsOnExtesionDependentFDWs(Oid extensionId);


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

	List *extensionAddresses = GetObjectAddressListFromParseTree(node, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(extensionAddresses) == 1);

	EnsureAllObjectDependenciesExistOnAllNodes(extensionAddresses);

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

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);
		if (!IsAnyObjectDistributed(list_make1(address)))
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
	List *extensionAddresses = GetObjectAddressListFromParseTree(node, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(extensionAddresses) == 1);

	if (!ShouldPropagateExtensionCommand(node))
	{
		return NIL;
	}

	/* dependencies (schema) have changed let's ensure they exist */
	EnsureAllObjectDependenciesExistOnAllNodes(extensionAddresses);

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
 * EnsureAllObjectDependenciesExistOnAllNodes on demand. Hence, this function is just designed
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
		if (!ShouldMarkRelationDistributed(citusTableId))
		{
			continue;
		}

		/* refrain reading the metadata cache for all tables */
		if (ShouldSyncTableMetadataViaCatalog(citusTableId))
		{
			ObjectAddress tableAddress = { 0 };
			ObjectAddressSet(tableAddress, RelationRelationId, citusTableId);

			/*
			 * We mark tables distributed immediately because we also need to mark
			 * views as distributed. We check whether the views that depend on
			 * the table has any auto-distirbutable dependencies below. Citus
			 * currently cannot "auto" distribute tables as dependencies, so we
			 * mark them distributed immediately.
			 */
			MarkObjectDistributedLocally(&tableAddress);

			/*
			 * All the distributable dependencies of a table should be marked as
			 * distributed.
			 */
			List *distributableDependencyObjectAddresses =
				GetDistributableDependenciesForObject(&tableAddress);

			resultingObjectAddresses =
				list_concat(resultingObjectAddresses,
							distributableDependencyObjectAddresses);
		}
	}

	/*
	 * As of Citus 11, views on Citus tables that do not have any unsupported
	 * dependency should also be distributed.
	 *
	 * In general, we mark views distributed as long as it does not have
	 * any unsupported dependencies.
	 */
	List *viewList = GetAllViews();
	Oid viewOid = InvalidOid;
	foreach_oid(viewOid, viewList)
	{
		if (!ShouldMarkRelationDistributed(viewOid))
		{
			continue;
		}

		ObjectAddress viewAddress = { 0 };
		ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

		/*
		 * If a view depends on multiple views, that view will be marked
		 * as distributed while it is processed for the last view
		 * table.
		 */
		MarkObjectDistributedLocally(&viewAddress);

		/* we need to pass pointer allocated in the heap */
		ObjectAddress *addressPointer = palloc0(sizeof(ObjectAddress));
		*addressPointer = viewAddress;

		List *distributableDependencyObjectAddresses =
			GetDistributableDependenciesForObject(&viewAddress);

		resultingObjectAddresses =
			list_concat(resultingObjectAddresses,
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
 * GetAllViews returns list of view oids that exists on this server.
 */
static List *
GetAllViews(void)
{
	List *viewOidList = NIL;

	Relation pgClass = table_open(RelationRelationId, AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgClass, InvalidOid, false, NULL,
													0, NULL);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_class relationForm = (Form_pg_class) GETSTRUCT(heapTuple);

		/* we're only interested in views */
		if (relationForm->relkind == RELKIND_VIEW)
		{
			viewOidList = lappend_oid(viewOidList, relationForm->oid);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgClass, NoLock);

	return viewOidList;
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
 * PreProcessCreateExtensionCitusStmtForColumnar determines whether need to
 * install citus_columnar first or citus_columnar is supported on current
 * citus version, when a given utility is a CREATE statement
 */
void
PreprocessCreateExtensionStmtForCitusColumnar(Node *parsetree)
{
	/*CREATE EXTENSION CITUS (version Z) */
	CreateExtensionStmt *createExtensionStmt = castNode(CreateExtensionStmt,
														parsetree);

	if (strcmp(createExtensionStmt->extname, "citus") == 0)
	{
		int versionNumber = (int) (100 * strtod(CITUS_MAJORVERSION, NULL));
		DefElem *newVersionValue = GetExtensionOption(createExtensionStmt->options,
													  "new_version");

		/*create extension citus version xxx*/
		if (newVersionValue)
		{
			char *newVersion = strdup(defGetString(newVersionValue));
			versionNumber = GetExtensionVersionNumber(newVersion);
		}

		/*citus version >= 11.1 requires install citus_columnar first*/
		if (versionNumber >= 1110 && !CitusHasBeenLoaded())
		{
			if (get_extension_oid("citus_columnar", true) == InvalidOid)
			{
				CreateExtensionWithVersion("citus_columnar", NULL);
			}
		}
	}

	/*Edge case check: citus_columnar are supported on citus version >= 11.1*/
	if (strcmp(createExtensionStmt->extname, "citus_columnar") == 0)
	{
		Oid citusOid = get_extension_oid("citus", true);
		if (citusOid != InvalidOid)
		{
			char *curCitusVersion = strdup(get_extension_version(citusOid));
			int curCitusVersionNum = GetExtensionVersionNumber(curCitusVersion);
			if (curCitusVersionNum < 1110)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg(
									"must upgrade citus to version 11.1-1 first before install citus_columnar")));
			}
		}
	}
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
 * PreprocessAlterExtensionCitusStmtForCitusColumnar pre-process the case when upgrade citus
 * to version that support citus_columnar, or downgrade citus to lower version that
 * include columnar inside citus extension
 */
void
PreprocessAlterExtensionCitusStmtForCitusColumnar(Node *parseTree)
{
	/*upgrade citus: alter extension citus update to 'xxx' */
	DefElem *newVersionValue = GetExtensionOption(
		((AlterExtensionStmt *) parseTree)->options, "new_version");
	Oid citusColumnarOid = get_extension_oid("citus_columnar", true);
	if (newVersionValue)
	{
		char *newVersion = defGetString(newVersionValue);
		double newVersionNumber = GetExtensionVersionNumber(strdup(newVersion));

		/*alter extension citus update to version >= 11.1-1, and no citus_columnar installed */
		if (newVersionNumber >= 1110 && citusColumnarOid == InvalidOid)
		{
			/*it's upgrade citus to 11.1-1 or further version */
			CreateExtensionWithVersion("citus_columnar", CITUS_COLUMNAR_INTERNAL_VERSION);
		}
		else if (newVersionNumber < 1110 && citusColumnarOid != InvalidOid)
		{
			/*downgrade citus, need downgrade citus_columnar to Y */
			AlterExtensionUpdateStmt("citus_columnar", CITUS_COLUMNAR_INTERNAL_VERSION);
		}
	}
	else
	{
		/*alter extension citus update without specifying the version*/
		int versionNumber = (int) (100 * strtod(CITUS_MAJORVERSION, NULL));
		if (versionNumber >= 1110)
		{
			if (citusColumnarOid == InvalidOid)
			{
				CreateExtensionWithVersion("citus_columnar",
										   CITUS_COLUMNAR_INTERNAL_VERSION);
			}
		}
	}
}


/*
 * PostprocessAlterExtensionCitusStmtForCitusColumnar process the case when upgrade citus
 * to version that support citus_columnar, or downgrade citus to lower version that
 * include columnar inside citus extension
 */
void
PostprocessAlterExtensionCitusStmtForCitusColumnar(Node *parseTree)
{
	DefElem *newVersionValue = GetExtensionOption(
		((AlterExtensionStmt *) parseTree)->options, "new_version");
	Oid citusColumnarOid = get_extension_oid("citus_columnar", true);
	if (newVersionValue)
	{
		char *newVersion = defGetString(newVersionValue);
		double newVersionNumber = GetExtensionVersionNumber(strdup(newVersion));
		if (newVersionNumber >= 1110 && citusColumnarOid != InvalidOid)
		{
			/*upgrade citus, after "ALTER EXTENSION citus update to xxx" updates citus_columnar Y to version Z. */
			char *curColumnarVersion = get_extension_version(citusColumnarOid);
			if (strcmp(curColumnarVersion, CITUS_COLUMNAR_INTERNAL_VERSION) == 0)
			{
				AlterExtensionUpdateStmt("citus_columnar", "11.1-1");
			}
		}
		else if (newVersionNumber < 1110 && citusColumnarOid != InvalidOid)
		{
			/*downgrade citus, after "ALTER EXTENSION citus update to xxx" drops citus_columnar extension */
			char *curColumnarVersion = get_extension_version(citusColumnarOid);
			if (strcmp(curColumnarVersion, CITUS_COLUMNAR_INTERNAL_VERSION) == 0)
			{
				RemoveExtensionById(citusColumnarOid);
			}
		}
	}
	else
	{
		/*alter extension citus update, need upgrade citus_columnar from Y to Z*/
		int versionNumber = (int) (100 * strtod(CITUS_MAJORVERSION, NULL));
		if (versionNumber >= 1110)
		{
			char *curColumnarVersion = get_extension_version(citusColumnarOid);
			if (strcmp(curColumnarVersion, CITUS_COLUMNAR_INTERNAL_VERSION) == 0)
			{
				AlterExtensionUpdateStmt("citus_columnar", "11.1-1");
			}
		}
	}
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

	/* any privilege granted on FDWs that belong to the extension should be included */
	List *FDWGrants =
		GenerateGrantCommandsOnExtesionDependentFDWs(extensionAddress->objectId);

	ddlCommands = list_concat(ddlCommands, FDWGrants);

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
 * GenerateGrantCommandsOnExtesionDependentFDWs returns a list of commands that GRANTs
 * the privileges on FDWs that are depending on the given extension.
 */
static List *
GenerateGrantCommandsOnExtesionDependentFDWs(Oid extensionId)
{
	List *commands = NIL;
	List *FDWOids = GetDependentFDWsToExtension(extensionId);

	Oid FDWOid = InvalidOid;
	foreach_oid(FDWOid, FDWOids)
	{
		Acl *aclEntry = GetPrivilegesForFDW(FDWOid);

		if (aclEntry == NULL)
		{
			continue;
		}

		AclItem *privileges = ACL_DAT(aclEntry);
		int numberOfPrivsGranted = ACL_NUM(aclEntry);

		for (int i = 0; i < numberOfPrivsGranted; i++)
		{
			commands = list_concat(commands,
								   GenerateGrantOnFDWQueriesFromAclItem(FDWOid,
																		&privileges[i]));
		}
	}

	return commands;
}


/*
 * GetDependentFDWsToExtension gets an extension oid and returns the list of oids of FDWs
 * that are depending on the given extension.
 */
List *
GetDependentFDWsToExtension(Oid extensionId)
{
	List *extensionFDWs = NIL;
	ScanKeyData key[3];
	int scanKeyCount = 3;
	HeapTuple tup;

	Relation pgDepend = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ExtensionRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(extensionId));
	ScanKeyInit(&key[2],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ForeignDataWrapperRelationId));

	SysScanDesc scan = systable_beginscan(pgDepend, InvalidOid, false,
										  NULL, scanKeyCount, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend pgDependEntry = (Form_pg_depend) GETSTRUCT(tup);

		if (pgDependEntry->deptype == DEPENDENCY_EXTENSION)
		{
			extensionFDWs = lappend_oid(extensionFDWs, pgDependEntry->objid);
		}
	}

	systable_endscan(scan);
	table_close(pgDepend, AccessShareLock);

	return extensionFDWs;
}


/*
 * AlterExtensionSchemaStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 */
List *
AlterExtensionSchemaStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

	return list_make1(address);
}


/*
 * AlterExtensionUpdateStmtObjectAddress returns the ObjectAddress of the extension that is
 * the subject of the AlterExtensionStmt. Errors if missing_ok is false.
 */
List *
AlterExtensionUpdateStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

	return list_make1(address);
}


/*
 * CreateExtensionWithVersion builds and execute create extension statements
 * per given extension name and extension verision
 */
void
CreateExtensionWithVersion(char *extname, char *extVersion)
{
	CreateExtensionStmt *createExtensionStmt = makeNode(CreateExtensionStmt);

	/* set location to -1 as it is unknown */
	int location = -1;

	/* set extension name and if_not_exists fields */
	createExtensionStmt->extname = extname;
	createExtensionStmt->if_not_exists = true;

	if (extVersion == NULL)
	{
		createExtensionStmt->options = NIL;
	}
	else
	{
		Node *extensionVersionArg = (Node *) makeString(extVersion);
		DefElem *extensionVersionElement = makeDefElem("new_version", extensionVersionArg,
													   location);
		createExtensionStmt->options = lappend(createExtensionStmt->options,
											   extensionVersionElement);
	}

	CreateExtension(NULL, createExtensionStmt);
	CommandCounterIncrement();
}


/*
 * GetExtensionVersionNumber convert extension version to real value
 */
int
GetExtensionVersionNumber(char *extVersion)
{
	char *strtokPosition = NULL;
	char *versionVal = strtok_r(extVersion, "-", &strtokPosition);
	double versionNumber = strtod(versionVal, NULL);
	return (int) (versionNumber * 100);
}


/*
 * AlterExtensionUpdateStmt builds and execute Alter extension statements
 * per given extension name and updates extension verision
 */
void
AlterExtensionUpdateStmt(char *extname, char *extVersion)
{
	AlterExtensionStmt *alterExtensionStmt = makeNode(AlterExtensionStmt);

	/* set location to -1 as it is unknown */
	int location = -1;
	alterExtensionStmt->extname = extname;

	if (extVersion == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("alter extension \"%s\" should not be empty",
							   extVersion)));
	}

	Node *extensionVersionArg = (Node *) makeString(extVersion);
	DefElem *extensionVersionElement = makeDefElem("new_version", extensionVersionArg,
												   location);
	alterExtensionStmt->options = lappend(alterExtensionStmt->options,
										  extensionVersionElement);
	ExecAlterExtensionStmt(NULL, alterExtensionStmt);
	CommandCounterIncrement();
}
