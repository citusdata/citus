/*-------------------------------------------------------------------------
 *
 * publication.c
 *    Commands for creating publications
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_create_or_replace.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_version_compat.h"


static CreatePublicationStmt * BuildCreatePublicationStmt(Oid publicationId);
#if (PG_VERSION_NUM >= PG_VERSION_15)
static PublicationObjSpec * BuildPublicationRelationObjSpec(Oid relationId,
															Oid publicationId,
															bool tableOnly);
#endif
static void AppendPublishOptionList(StringInfo str, List *strings);
static char * AlterPublicationOwnerCommand(Oid publicationId);
static bool ShouldPropagateCreatePublication(CreatePublicationStmt *stmt);
static List * ObjectAddressForPublicationName(char *publicationName, bool missingOk);


/*
 * PostProcessCreatePublicationStmt handles CREATE PUBLICATION statements
 * that contain distributed tables.
 */
List *
PostProcessCreatePublicationStmt(Node *node, const char *queryString)
{
	CreatePublicationStmt *stmt = castNode(CreatePublicationStmt, node);

	if (!ShouldPropagateCreatePublication(stmt))
	{
		/* should not propagate right now */
		return NIL;
	}

	/* call into CreatePublicationStmtObjectAddress */
	List *publicationAddresses = GetObjectAddressListFromParseTree(node, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(publicationAddresses) == 1);

	if (IsAnyObjectAddressOwnedByExtension(publicationAddresses, NULL))
	{
		/* should not propagate publications owned by extensions */
		return NIL;
	}

	EnsureAllObjectDependenciesExistOnAllNodes(publicationAddresses);

	const ObjectAddress *pubAddress = linitial(publicationAddresses);

	List *commands = NIL;
	commands = lappend(commands, DISABLE_DDL_PROPAGATION);
	commands = lappend(commands, CreatePublicationDDLCommand(pubAddress->objectId));
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * CreatePublicationDDLCommandsIdempotent returns a list of DDL statements to be
 * executed on a node to recreate the publication addressed by the publicationAddress.
 */
List *
CreatePublicationDDLCommandsIdempotent(const ObjectAddress *publicationAddress)
{
	Assert(publicationAddress->classId == PublicationRelationId);

	char *ddlCommand =
		CreatePublicationDDLCommand(publicationAddress->objectId);

	char *alterPublicationOwnerSQL =
		AlterPublicationOwnerCommand(publicationAddress->objectId);

	return list_make2(
		WrapCreateOrReplace(ddlCommand),
		alterPublicationOwnerSQL);
}


/*
 * CreatePublicationDDLCommand returns the CREATE PUBLICATION string that
 * can be used to recreate a given publication.
 */
char *
CreatePublicationDDLCommand(Oid publicationId)
{
	CreatePublicationStmt *createPubStmt = BuildCreatePublicationStmt(publicationId);

	/* we took the WHERE clause from the catalog where it is already transformed */
	bool whereClauseRequiresTransform = false;

	/* only propagate Citus tables in publication */
	bool includeLocalTables = false;

	return DeparseCreatePublicationStmtExtended((Node *) createPubStmt,
												whereClauseRequiresTransform,
												includeLocalTables);
}


/*
 * BuildCreatePublicationStmt constructs a CreatePublicationStmt struct for the
 * given publication.
 */
static CreatePublicationStmt *
BuildCreatePublicationStmt(Oid publicationId)
{
	CreatePublicationStmt *createPubStmt = makeNode(CreatePublicationStmt);

	HeapTuple publicationTuple =
		SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(publicationId));

	if (!HeapTupleIsValid(publicationTuple))
	{
		ereport(ERROR, (errmsg("cannot find publication with oid: %d", publicationId)));
	}

	Form_pg_publication publicationForm =
		(Form_pg_publication) GETSTRUCT(publicationTuple);

	/* CREATE PUBLICATION <name> */
	createPubStmt->pubname = pstrdup(NameStr(publicationForm->pubname));

	/* FOR ALL TABLES */
	createPubStmt->for_all_tables = publicationForm->puballtables;

	ReleaseSysCache(publicationTuple);

#if (PG_VERSION_NUM >= PG_VERSION_15)
	List *schemaIds = GetPublicationSchemas(publicationId);
	Oid schemaId = InvalidOid;

	foreach_oid(schemaId, schemaIds)
	{
		char *schemaName = get_namespace_name(schemaId);

		PublicationObjSpec *publicationObject = makeNode(PublicationObjSpec);
		publicationObject->pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA;
		publicationObject->pubtable = NULL;
		publicationObject->name = schemaName;
		publicationObject->location = -1;

		createPubStmt->pubobjects = lappend(createPubStmt->pubobjects, publicationObject);
	}
#endif

	List *relationIds = GetPublicationRelations(publicationId,
												publicationForm->pubviaroot ?
												PUBLICATION_PART_ROOT :
												PUBLICATION_PART_LEAF);
	Oid relationId = InvalidOid;
	int citusTableCount PG_USED_FOR_ASSERTS_ONLY = 0;

	/* mainly for consistent ordering in test output */
	relationIds = SortList(relationIds, CompareOids);

	foreach_oid(relationId, relationIds)
	{
#if (PG_VERSION_NUM >= PG_VERSION_15)
		bool tableOnly = false;

		/* since postgres 15, tables can have a column list and filter */
		PublicationObjSpec *publicationObject =
			BuildPublicationRelationObjSpec(relationId, publicationId, tableOnly);

		createPubStmt->pubobjects = lappend(createPubStmt->pubobjects, publicationObject);
#else

		/* before postgres 15, only full tables are supported */
		char *schemaName = get_namespace_name(get_rel_namespace(relationId));
		char *tableName = get_rel_name(relationId);
		RangeVar *rangeVar = makeRangeVar(schemaName, tableName, -1);

		createPubStmt->tables = lappend(createPubStmt->tables, rangeVar);
#endif

		if (IsCitusTable(relationId))
		{
			citusTableCount++;
		}
	}

	/* WITH (publish_via_partition_root = true) option */
	bool publishViaRoot = publicationForm->pubviaroot;
	char *publishViaRootString = publishViaRoot ? "true" : "false";
	DefElem *pubViaRootOption = makeDefElem("publish_via_partition_root",
											(Node *) makeString(publishViaRootString),
											-1);
	createPubStmt->options = lappend(createPubStmt->options, pubViaRootOption);

	/* WITH (publish = 'insert, update, delete, truncate') option */
	List *publishList = NIL;

	if (publicationForm->pubinsert)
	{
		publishList = lappend(publishList, makeString("insert"));
	}

	if (publicationForm->pubupdate)
	{
		publishList = lappend(publishList, makeString("update"));
	}

	if (publicationForm->pubdelete)
	{
		publishList = lappend(publishList, makeString("delete"));
	}

	if (publicationForm->pubtruncate)
	{
		publishList = lappend(publishList, makeString("truncate"));
	}

	if (list_length(publishList) > 0)
	{
		StringInfo optionValue = makeStringInfo();
		AppendPublishOptionList(optionValue, publishList);

		DefElem *publishOption = makeDefElem("publish",
											 (Node *) makeString(optionValue->data), -1);
		createPubStmt->options = lappend(createPubStmt->options, publishOption);
	}


	return createPubStmt;
}


/*
 * AppendPublishOptionList appends a list of publication options in
 * comma-separate form.
 */
static void
AppendPublishOptionList(StringInfo str, List *options)
{
	ListCell *stringCell = NULL;
	foreach(stringCell, options)
	{
		const char *string = strVal(lfirst(stringCell));
		if (stringCell != list_head(options))
		{
			appendStringInfoString(str, ", ");
		}

		/* we cannot escape these strings */
		appendStringInfoString(str, string);
	}
}


#if (PG_VERSION_NUM >= PG_VERSION_15)

/*
 * BuildPublicationRelationObjSpec returns a PublicationObjSpec that
 * can be included in a CREATE or ALTER PUBLICATION statement.
 */
static PublicationObjSpec *
BuildPublicationRelationObjSpec(Oid relationId, Oid publicationId,
								bool tableOnly)
{
	HeapTuple pubRelationTuple = SearchSysCache2(PUBLICATIONRELMAP,
												 ObjectIdGetDatum(relationId),
												 ObjectIdGetDatum(publicationId));
	if (!HeapTupleIsValid(pubRelationTuple))
	{
		ereport(ERROR, (errmsg("cannot find relation with oid %d in publication "
							   "with oid %d", relationId, publicationId)));
	}

	List *columnNameList = NIL;
	Node *whereClause = NULL;

	/* build the column list  */
	if (!tableOnly)
	{
		bool isNull = false;
		Datum attributesDatum = SysCacheGetAttr(PUBLICATIONRELMAP, pubRelationTuple,
												Anum_pg_publication_rel_prattrs,
												&isNull);
		if (!isNull)
		{
			ArrayType *attributesArray = DatumGetArrayTypeP(attributesDatum);
			int attributeCount = ARR_DIMS(attributesArray)[0];
			int16 *elems = (int16 *) ARR_DATA_PTR(attributesArray);

			for (int attNumIndex = 0; attNumIndex < attributeCount; attNumIndex++)
			{
				AttrNumber attributeNumber = elems[attNumIndex];
				char *columnName = get_attname(relationId, attributeNumber, false);

				columnNameList = lappend(columnNameList, makeString(columnName));
			}
		}

		/* build the WHERE clause */
		Datum whereClauseDatum = SysCacheGetAttr(PUBLICATIONRELMAP, pubRelationTuple,
												 Anum_pg_publication_rel_prqual,
												 &isNull);
		if (!isNull)
		{
			/*
			 * We use the already-transformed parse tree form here, which does
			 * not match regular CreatePublicationStmt
			 */
			whereClause = stringToNode(TextDatumGetCString(whereClauseDatum));
		}
	}

	ReleaseSysCache(pubRelationTuple);

	char *schemaName = get_namespace_name(get_rel_namespace(relationId));
	char *tableName = get_rel_name(relationId);
	RangeVar *rangeVar = makeRangeVar(schemaName, tableName, -1);

	/* build the FOR TABLE */
	PublicationTable *publicationTable =
		makeNode(PublicationTable);
	publicationTable->relation = rangeVar;
	publicationTable->whereClause = whereClause;
	publicationTable->columns = columnNameList;

	PublicationObjSpec *publicationObject = makeNode(PublicationObjSpec);
	publicationObject->pubobjtype = PUBLICATIONOBJ_TABLE;
	publicationObject->pubtable = publicationTable;
	publicationObject->name = NULL;
	publicationObject->location = -1;

	return publicationObject;
}


#endif


/*
 * PreprocessAlterPublicationStmt handles ALTER PUBLICATION statements
 * in a way that is mostly similar to PreprocessAlterDistributedObjectStmt,
 * except we do not ensure sequential mode (publications do not interact with
 * shards) and can handle NULL deparse commands for ALTER PUBLICATION commands
 * that only involve local tables.
 */
List *
PreprocessAlterPublicationStmt(Node *stmt, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	List *addresses = GetObjectAddressListFromParseTree(stmt, false, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	EnsureCoordinator();
	QualifyTreeNode(stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);
	if (sql == NULL)
	{
		/*
		 * Deparsing logic decided that there is nothing to propagate, e.g.
		 * because the command only concerns local tables.
		 */
		return NIL;
	}

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * GetAlterPublicationDDLCommandsForTable gets a list of ALTER PUBLICATION .. ADD/DROP
 * commands for the given table.
 *
 * If isAdd is true, it return ALTER PUBLICATION .. ADD TABLE commands for all
 * publications.
 *
 * Otherwise, it returns ALTER PUBLICATION  .. DROP TABLE commands for all
 * publications.
 */
List *
GetAlterPublicationDDLCommandsForTable(Oid relationId, bool isAdd)
{
	List *commands = NIL;

	List *publicationIds = GetRelationPublications(relationId);
	Oid publicationId = InvalidOid;

	foreach_oid(publicationId, publicationIds)
	{
		char *command = GetAlterPublicationTableDDLCommand(publicationId,
														   relationId, isAdd);

		commands = lappend(commands, command);
	}

	return commands;
}


/*
 * GetAlterPublicationTableDDLCommand generates an ALTer PUBLICATION .. ADD/DROP TABLE
 * command for the given publication and relation ID.
 *
 * If isAdd is true, it return an ALTER PUBLICATION .. ADD TABLE command.
 * Otherwise, it returns ALTER PUBLICATION  .. DROP TABLE command.
 */
char *
GetAlterPublicationTableDDLCommand(Oid publicationId, Oid relationId,
								   bool isAdd)
{
	HeapTuple pubTuple = SearchSysCache1(PUBLICATIONOID,
										 ObjectIdGetDatum(publicationId));
	if (!HeapTupleIsValid(pubTuple))
	{
		ereport(ERROR, (errmsg("cannot find publication with oid: %d",
							   publicationId)));
	}

	Form_pg_publication pubForm = (Form_pg_publication) GETSTRUCT(pubTuple);

	AlterPublicationStmt *alterPubStmt = makeNode(AlterPublicationStmt);
	alterPubStmt->pubname = pstrdup(NameStr(pubForm->pubname));

	ReleaseSysCache(pubTuple);

#if (PG_VERSION_NUM >= PG_VERSION_15)
	bool tableOnly = !isAdd;

	/* since postgres 15, tables can have a column list and filter */
	PublicationObjSpec *publicationObject =
		BuildPublicationRelationObjSpec(relationId, publicationId, tableOnly);

	alterPubStmt->pubobjects = lappend(alterPubStmt->pubobjects, publicationObject);
	alterPubStmt->action = isAdd ? AP_AddObjects : AP_DropObjects;
#else

	/* before postgres 15, only full tables are supported */
	char *schemaName = get_namespace_name(get_rel_namespace(relationId));
	char *tableName = get_rel_name(relationId);
	RangeVar *rangeVar = makeRangeVar(schemaName, tableName, -1);

	alterPubStmt->tables = lappend(alterPubStmt->tables, rangeVar);
	alterPubStmt->tableAction = isAdd ? DEFELEM_ADD : DEFELEM_DROP;
#endif

	/* we take the WHERE clause from the catalog where it is already transformed */
	bool whereClauseNeedsTransform = false;

	/*
	 * We use these commands to restore publications before/after transforming a
	 * table, including transformations to/from local tables.
	 */
	bool includeLocalTables = true;

	char *command = DeparseAlterPublicationStmtExtended((Node *) alterPubStmt,
														whereClauseNeedsTransform,
														includeLocalTables);

	return command;
}


/*
 * AlterPublicationOwnerCommand returns "ALTER PUBLICATION .. OWNER TO .."
 * statement for the specified publication.
 */
static char *
AlterPublicationOwnerCommand(Oid publicationId)
{
	HeapTuple publicationTuple =
		SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(publicationId));

	if (!HeapTupleIsValid(publicationTuple))
	{
		ereport(ERROR, (errmsg("cannot find publication with oid: %d",
							   publicationId)));
	}

	Form_pg_publication publicationForm =
		(Form_pg_publication) GETSTRUCT(publicationTuple);

	char *publicationName = NameStr(publicationForm->pubname);
	Oid publicationOwnerId = publicationForm->pubowner;

	char *publicationOwnerName = GetUserNameFromId(publicationOwnerId, false);

	StringInfo alterCommand = makeStringInfo();
	appendStringInfo(alterCommand, "ALTER PUBLICATION %s OWNER TO %s",
					 quote_identifier(publicationName),
					 quote_identifier(publicationOwnerName));

	ReleaseSysCache(publicationTuple);

	return alterCommand->data;
}


/*
 * ShouldPropagateCreatePublication tests if we need to propagate a CREATE PUBLICATION
 * statement.
 */
static bool
ShouldPropagateCreatePublication(CreatePublicationStmt *stmt)
{
	if (!ShouldPropagate())
	{
		return false;
	}

	return true;
}


/*
 * AlterPublicationStmtObjectAddress generates the object address for the
 * publication altered by a regular ALTER PUBLICATION .. statement.
 */
List *
AlterPublicationStmtObjectAddress(Node *node, bool missingOk, bool isPostProcess)
{
	AlterPublicationStmt *stmt = castNode(AlterPublicationStmt, node);

	return ObjectAddressForPublicationName(stmt->pubname, missingOk);
}


/*
 * AlterPublicationOwnerStmtObjectAddress generates the object address for the
 * publication altered by the given ALTER PUBLICATION .. OWNER TO statement.
 */
List *
AlterPublicationOwnerStmtObjectAddress(Node *node, bool missingOk, bool isPostProcess)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);

	return ObjectAddressForPublicationName(strVal(stmt->object), missingOk);
}


/*
 * CreatePublicationStmtObjectAddress generates the object address for the
 * publication created by the given CREATE PUBLICATION statement.
 */
List *
CreatePublicationStmtObjectAddress(Node *node, bool missingOk, bool isPostProcess)
{
	CreatePublicationStmt *stmt = castNode(CreatePublicationStmt, node);

	return ObjectAddressForPublicationName(stmt->pubname, missingOk);
}


/*
 * RenamePublicationStmtObjectAddress generates the object address for the
 * publication altered by the given ALter PUBLICATION .. RENAME TO statement.
 */
List *
RenamePublicationStmtObjectAddress(Node *node, bool missingOk, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	return ObjectAddressForPublicationName(strVal(stmt->object), missingOk);
}


/*
 * ObjectAddressForPublicationName returns the object address for a given publication
 * name.
 */
static List *
ObjectAddressForPublicationName(char *publicationName, bool missingOk)
{
	Oid publicationId = InvalidOid;

	HeapTuple publicationTuple =
		SearchSysCache1(PUBLICATIONNAME, CStringGetDatum(publicationName));
	if (HeapTupleIsValid(publicationTuple))
	{
		Form_pg_publication publicationForm =
			(Form_pg_publication) GETSTRUCT(publicationTuple);
		publicationId = publicationForm->oid;

		ReleaseSysCache(publicationTuple);
	}
	else if (!missingOk)
	{
		/* it should have just been created */
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("publication \"%s\" does not exist", publicationName)));
	}

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, PublicationRelationId, publicationId);

	return list_make1(address);
}
