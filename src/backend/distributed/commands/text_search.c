/*-------------------------------------------------------------------------
 *
 * text_search.c
 *    Commands for creating and altering TEXT SEARCH objects
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "commands/comment.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_public.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_create_or_replace.h"


static List * GetDistributedTextSearchConfigurationNames(DropStmt *stmt);
static DefineStmt * GetTextSearchConfigDefineStmt(Oid tsconfigOid);
static List * GetTextSearchConfigCommentStmt(Oid tsconfigOid);
static List * get_ts_parser_namelist(Oid tsparserOid);
static List * GetTextSearchConfigMappingStmt(Oid tsconfigOid);
static List * GetTextSearchConfigOwnerStmts(Oid tsconfigOid);

static List * get_ts_dict_namelist(Oid tsdictOid);
static Oid get_ts_config_parser_oid(Oid tsconfigOid);
static char * get_ts_parser_tokentype_name(Oid parserOid, int32 tokentype);

/*
 * PostprocessCreateTextSearchConfigurationStmt is called after the TEXT SEARCH
 * CONFIGURATION has been created locally.
 *
 * Contrary to many other objects a text search configuration is often created as a copy
 * of an existing configuration. After the copy there is no relation to the configuration
 * that has been copied. This prevents our normal approach of ensuring dependencies to
 * exist before forwarding a close ressemblance of the statement the user executed.
 *
 * Instead we recreate the object based on what we find in our own catalog, hence the
 * amount of work we perform in the postprocess function, contrary to other objects.
 */
List *
PostprocessCreateTextSearchConfigurationStmt(Node *node, const char *queryString)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_TSCONFIGURATION);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	EnsureDependenciesExistOnAllNodes(&address);

	/*
	 * TEXT SEARCH CONFIGURATION objects are more complex with their mappings and the
	 * possibility of copying from existing templates that we will require the idempotent
	 * recreation commands to be run for successful propagation
	 */
	List *commands = CreateTextSearchConfigDDLCommandsIdempotent(&address);

	commands = lcons(DISABLE_DDL_PROPAGATION, commands);
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


List *
GetCreateTextSearchConfigStatements(const ObjectAddress *address)
{
	Assert(address->classId == TSConfigRelationId);
	List *stmts = NIL;

	/* CREATE TEXT SEARCH CONFIGURATION ...*/
	stmts = lappend(stmts, GetTextSearchConfigDefineStmt(address->objectId));

	/* ALTER TEXT SEARCH CONFIGURATION ... OWNER TO ...*/
	stmts = list_concat(stmts, GetTextSearchConfigOwnerStmts(address->objectId));

	/* COMMENT ON TEXT SEARCH CONFIGURATION ... */
	stmts = list_concat(stmts, GetTextSearchConfigCommentStmt(address->objectId));


	/* ALTER TEXT SEARCH CONFIGURATION ... ADD MAPPING FOR ... WITH ... */
	stmts = list_concat(stmts, GetTextSearchConfigMappingStmt(address->objectId));

	return stmts;
}


/*
 * CreateTextSearchConfigDDLCommandsIdempotent creates a list of ddl commands to recreate
 * a TEXT SERACH CONFIGURATION object in an idempotent manner on workers.
 */
List *
CreateTextSearchConfigDDLCommandsIdempotent(const ObjectAddress *address)
{
	List *stmts = GetCreateTextSearchConfigStatements(address);
	List *sqls = DeparseTreeNodes(stmts);
	return list_make1(WrapCreateOrReplaceList(sqls));
}


/*
 * PreprocessDropTextSearchConfigurationStmt prepares the statements we need to send to
 * the workers. After we have dropped the schema's locally they also got removed from
 * pg_dist_object so it is important to do all distribution checks before the change is
 * made locally.
 */
List *
PreprocessDropTextSearchConfigurationStmt(Node *node, const char *queryString,
										  ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_TSCONFIGURATION);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	List *distributedObjects = GetDistributedTextSearchConfigurationNames(stmt);
	if (list_length(distributedObjects) == 0)
	{
		/* no distributed objects to remove */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	/*
	 * Temporarily replace the list of objects being dropped with only the list
	 * containing the distributed objects. After we have created the sql statement we
	 * restore the original list of objects to execute on locally.
	 *
	 * Because searchpaths on coordinator and workers might not be in sync we fully
	 * qualify the list before deparsing. This is safe because qualification doesn't
	 * change the original names in place, but insteads creates new ones.
	 */
	List *originalObjects = stmt->objects;
	stmt->objects = distributedObjects;
	QualifyTreeNode((Node *) stmt);
	const char *dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = originalObjects;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * GetDistributedTextSearchConfigurationNames iterates over all text search configurations
 * dropped, and create a list containign all configurations that are distributed.
 */
static List *
GetDistributedTextSearchConfigurationNames(DropStmt *stmt)
{
	List *objName = NULL;
	List *distributedObjects = NIL;
	foreach_ptr(objName, stmt->objects)
	{
		Oid tsconfigOid = get_ts_config_oid(objName, stmt->missing_ok);
		if (!OidIsValid(tsconfigOid))
		{
			/* skip missing configuration names, they can't be dirstibuted */
			continue;
		}

		ObjectAddress address = { 0 };
		ObjectAddressSet(address, TSConfigRelationId, tsconfigOid);
		if (!IsObjectDistributed(&address))
		{
			continue;
		}
		distributedObjects = lappend(distributedObjects, objName);
	}
	return distributedObjects;
}


/*
 * PreprocessAlterTextSearchConfigurationStmt verifies if the configuration being altered
 * is distributed in the cluster. If that is the case it will prepare the list of commands
 * to send to the worker to apply the same changes remote.
 */
List *
PreprocessAlterTextSearchConfigurationStmt(Node *node, const char *queryString,
										   ProcessUtilityContext processUtilityContext)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	QualifyTreeNode((Node *) stmt);
	const char *alterStmtSql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * PreprocessRenameTextSearchConfigurationStmt verifies if the configuration being altered
 * is distributed in the cluster. If that is the case it will prepare the list of commands
 * to send to the worker to apply the same changes remote.
 */
List *
PreprocessRenameTextSearchConfigurationStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) ddlCommand,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * PreprocessAlterTextSearchConfigurationSchemaStmt verifies if the configuration being
 * altered is distributed in the cluster. If that is the case it will prepare the list of
 * commands to send to the worker to apply the same changes remote.
 */
List *
PreprocessAlterTextSearchConfigurationSchemaStmt(Node *node, const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * PostprocessAlterTextSearchConfigurationSchemaStmt is invoked after the schema has been
 * changed locally. Since changing the schema could result in new dependencies being found
 * for this object we re-ensure all the dependencies for the configuration do exist. This
 * is solely to propagate the new schema (and all its dependencies) if it was not already
 * distributed in the cluster.
 */
List *
PostprocessAlterTextSearchConfigurationSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


/*
 * PreprocessTextSearchConfigurationCommentStmt propagates any comment on a distributed
 * configuration to the workers. Since comments for configurations are promenently shown
 * when listing all text search configurations this is purely a cosmetic thing when
 * running in MX.
 */
List *
PreprocessTextSearchConfigurationCommentStmt(Node *node, const char *queryString,
											 ProcessUtilityContext processUtilityContext)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * PreprocessAlterTextSearchConfigurationOwnerStmt verifies if the configuration being
 * altered is distributed in the cluster. If that is the case it will prepare the list of
 * commands to send to the worker to apply the same changes remote.
 */
List *
PreprocessAlterTextSearchConfigurationOwnerStmt(Node *node, const char *queryString,
												ProcessUtilityContext
												processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_TSCONFIGURATION);

	QualifyTreeNode((Node *) stmt);
	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterTextSearchConfigurationOwnerStmt is invoked after the owner has been
 * changed locally. Since changing the owner could result in new dependencies being found
 * for this object we re-ensure all the dependencies for the configuration do exist. This
 * is solely to propagate the new owner (and all its dependencies) if it was not already
 * distributed in the cluster.
 */
List *
PostprocessAlterTextSearchConfigurationOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	/* dependencies have changed (owner) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


/*
 * GetTextSearchConfigDefineStmt returns the DefineStmt for a TEXT SEARCH CONFIGURATION
 * based on the configuration as defined in the catalog identified by tsconfigOid.
 *
 * This statement will only contain the parser, as all other properties for text search
 * configurations are stored as mappings in a different catalog.
 */
static DefineStmt *
GetTextSearchConfigDefineStmt(Oid tsconfigOid)
{
	HeapTuple tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(tsconfigOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search configuration %u",
			 tsconfigOid);
	}
	Form_pg_ts_config config = (Form_pg_ts_config) GETSTRUCT(tup);

	DefineStmt *stmt = makeNode(DefineStmt);
	stmt->kind = OBJECT_TSCONFIGURATION;

	stmt->defnames = get_ts_config_namelist(tsconfigOid);

	List *parserNameList = get_ts_parser_namelist(config->cfgparser);
	TypeName *parserTypeName = makeTypeNameFromNameList(parserNameList);
	stmt->definition = list_make1(makeDefElem("parser", (Node *) parserTypeName, -1));

	ReleaseSysCache(tup);
	return stmt;
}


/*
 * GetTextSearchConfigCommentStmt returns a list containing all entries to recreate a
 * comment on the configuration identified by tsconfigOid. The list could be empty if
 * there is no comment on a configuration.
 *
 * The reason for a list is for easy use when building a list of all statements to invoke
 * to recreate the text search configuration. An empty list can easily be concatinated
 * without inspection, contrary to a NULL ptr if we would return the CommentStmt struct.
 */
static List *
GetTextSearchConfigCommentStmt(Oid tsconfigOid)
{
	char *comment = GetComment(tsconfigOid, TSConfigRelationId, 0);
	if (!comment)
	{
		return NIL;
	}

	CommentStmt *stmt = makeNode(CommentStmt);
	stmt->objtype = OBJECT_TSCONFIGURATION;

	stmt->object = (Node *) get_ts_config_namelist(tsconfigOid);
	stmt->comment = comment;
	return list_make1(stmt);
}


/*
 * GetTextSearchConfigMappingStmt returns a list of all mappings from token_types to
 * dictionaries configured on a text search configuration identified by tsconfigOid.
 *
 * Many mappings can exist on a configuration which all require their own statement to
 * recreate.
 */
static List *
GetTextSearchConfigMappingStmt(Oid tsconfigOid)
{
	ScanKeyData mapskey = { 0 };

	/* mapcfg = tsconfigOid */
	ScanKeyInit(&mapskey,
				Anum_pg_ts_config_map_mapcfg,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tsconfigOid));

	Relation maprel = table_open(TSConfigMapRelationId, AccessShareLock);
	Relation mapidx = index_open(TSConfigMapIndexId, AccessShareLock);
	SysScanDesc mapscan = systable_beginscan_ordered(maprel, mapidx, NULL, 1, &mapskey);

	List *stmts = NIL;
	AlterTSConfigurationStmt *stmt = NULL;

	/*
	 * We iterate the config mappings on the index order filtered by mapcfg. Meaning we
	 * get equal maptokentype's in 1 run. By comparing the current tokentype to the last
	 * we know when we can create a new stmt and append the previous constructed one to
	 * the list.
	 */
	int lastTokType = -1;

	/*
	 * We read all mappings filtered by config id, hence we only need to load the name
	 * once and can reuse for every statement.
	 */
	List *configName = get_ts_config_namelist(tsconfigOid);

	Oid parserOid = get_ts_config_parser_oid(tsconfigOid);

	HeapTuple maptup = NULL;
	while ((maptup = systable_getnext_ordered(mapscan, ForwardScanDirection)) != NULL)
	{
		Form_pg_ts_config_map cfgmap = (Form_pg_ts_config_map) GETSTRUCT(maptup);
		if (lastTokType != cfgmap->maptokentype)
		{
			/* creating a new statement, appending the previous one (if existing) */
			if (stmt != NULL)
			{
				stmts = lappend(stmts, stmt);
			}

			stmt = makeNode(AlterTSConfigurationStmt);
			stmt->cfgname = configName;
			stmt->kind = ALTER_TSCONFIG_ADD_MAPPING;
			stmt->tokentype = list_make1(makeString(
											 get_ts_parser_tokentype_name(parserOid,
																		  cfgmap->
																		  maptokentype)));

			lastTokType = cfgmap->maptokentype;
		}

		stmt->dicts = lappend(stmt->dicts, get_ts_dict_namelist(cfgmap->mapdict));
	}

	/*
	 * If we have ran atleast 1 iteration above we have the last stmt not added to the
	 * stmts list.
	 */
	if (stmt != NULL)
	{
		stmts = lappend(stmts, stmt);
		stmt = NULL;
	}

	systable_endscan_ordered(mapscan);
	index_close(mapidx, NoLock);
	table_close(maprel, NoLock);

	return stmts;
}


/*
 * GetTextSearchConfigOwnerStmts returns a potentially empty list of statements to change
 * the ownership of a TEXT SEARCH CONFIGURATION object.
 *
 * The list is for convenienve when building a full list of statements to recreate the
 * configuration.
 */
static List *
GetTextSearchConfigOwnerStmts(Oid tsconfigOid)
{
	HeapTuple tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(tsconfigOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search configuration %u",
			 tsconfigOid);
	}
	Form_pg_ts_config config = (Form_pg_ts_config) GETSTRUCT(tup);

	AlterOwnerStmt *stmt = makeNode(AlterOwnerStmt);
	stmt->objectType = OBJECT_TSCONFIGURATION;
	stmt->object = (Node *) get_ts_config_namelist(tsconfigOid);
	stmt->newowner = GetRoleSpecObjectForUser(config->cfgowner);

	ReleaseSysCache(tup);
	return list_make1(stmt);
}


/*
 * get_ts_config_namelist based on the tsconfigOid this function creates the namelist that
 * identifies the configuration in a fully qualified manner, irregardless of the schema
 * existing on the search_path.
 */
List *
get_ts_config_namelist(Oid tsconfigOid)
{
	HeapTuple tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(tsconfigOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search configuration %u",
			 tsconfigOid);
	}
	Form_pg_ts_config config = (Form_pg_ts_config) GETSTRUCT(tup);

	char *schema = get_namespace_name(config->cfgnamespace);
	char *configName = pstrdup(NameStr(config->cfgname));
	List *names = list_make2(makeString(schema), makeString(configName));

	ReleaseSysCache(tup);
	return names;
}


/*
 * get_ts_dict_namelist based on the tsdictOid this function creates the namelist that
 * identifies the dictionary in a fully qualified manner, irregardless of the schema
 * existing on the search_path.
 */
static List *
get_ts_dict_namelist(Oid tsdictOid)
{
	HeapTuple tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(tsdictOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search dictionary %u", tsdictOid);
	}
	Form_pg_ts_dict dict = (Form_pg_ts_dict) GETSTRUCT(tup);

	char *schema = get_namespace_name(dict->dictnamespace);
	char *dictName = pstrdup(NameStr(dict->dictname));
	List *names = list_make2(makeString(schema), makeString(dictName));

	ReleaseSysCache(tup);
	return names;
}


/*
 * get_ts_config_parser_oid based on the tsconfigOid this function returns the Oid of the
 * parser used in the configuration.
 */
static Oid
get_ts_config_parser_oid(Oid tsconfigOid)
{
	HeapTuple tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(tsconfigOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search configuration %u", tsconfigOid);
	}
	Form_pg_ts_config config = (Form_pg_ts_config) GETSTRUCT(tup);
	Oid parserOid = config->cfgparser;

	ReleaseSysCache(tup);
	return parserOid;
}


/*
 * get_ts_parser_tokentype_name returns the name of the token as known to the parser by
 * its tokentype identifier. The parser used to resolve the token name is identified by
 * parserOid and should be the same that emitted the tokentype to begin with.
 */
static char *
get_ts_parser_tokentype_name(Oid parserOid, int32 tokentype)
{
	TSParserCacheEntry *parserCache = lookup_ts_parser_cache(parserOid);
	if (!OidIsValid(parserCache->lextypeOid))
	{
		elog(ERROR, "method lextype isn't defined for text search parser %u", parserOid);
	}

	/* take lextypes from parser */
	LexDescr *tokenlist = (LexDescr *) DatumGetPointer(
		OidFunctionCall1(parserCache->lextypeOid, Int32GetDatum(0)));

	/* and find the one with lexid = tokentype */
	int tokenIndex = 0;
	while (tokenlist && tokenlist[tokenIndex].lexid)
	{
		if (tokenlist[tokenIndex].lexid == tokentype)
		{
			return pstrdup(tokenlist[tokenIndex].alias);
		}
		tokenIndex++;
	}

	/* we haven't found the token */
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("token type \"%d\" does not exist in parser", tokentype)));
}


/*
 * get_ts_parser_namelist based on the tsparserOid this function creates the namelist that
 * identifies the parser in a fully qualified manner, irregardless of the schema existing
 * on the search_path.
 */
static List *
get_ts_parser_namelist(Oid tsparserOid)
{
	HeapTuple tup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(tsparserOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for text search parser %u",
			 tsparserOid);
	}
	Form_pg_ts_parser parser = (Form_pg_ts_parser) GETSTRUCT(tup);

	char *schema = get_namespace_name(parser->prsnamespace);
	char *parserName = pstrdup(NameStr(parser->prsname));
	List *names = list_make2(makeString(schema), makeString(parserName));

	ReleaseSysCache(tup);
	return names;
}


/*
 * CreateTextSearchConfigurationObjectAddress resolves the ObjectAddress for the object
 * being created. If missing_pk is false the function will error, explaining to the user
 * the text search configuration described in the statement doesn't exist.
 */
ObjectAddress
CreateTextSearchConfigurationObjectAddress(Node *node, bool missing_ok)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_TSCONFIGURATION);

	Oid objid = get_ts_config_oid(stmt->defnames, missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TSConfigRelationId, objid);
	return address;
}


/*
 * RenameTextSearchConfigurationStmtObjectAddress resolves the ObjectAddress for the TEXT
 * SEARCH CONFIGURATION being renamed. Optionally errors if the configuration does not
 * exist based on the missing_ok flag passed in by the caller.
 */
ObjectAddress
RenameTextSearchConfigurationStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSCONFIGURATION);

	Oid objid = get_ts_config_oid(castNode(List, stmt->object), missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TSConfigRelationId, objid);
	return address;
}


/*
 * AlterTextSearchConfigurationStmtObjectAddress resolves the ObjectAddress for the TEXT
 * SEARCH CONFIGURATION being altered. Optionally errors if the configuration does not
 * exist based on the missing_ok flag passed in by the caller.
 */
ObjectAddress
AlterTextSearchConfigurationStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	Oid objid = get_ts_config_oid(stmt->cfgname, missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TSConfigRelationId, objid);
	return address;
}


/*
 * AlterTextSearchConfigurationSchemaStmtObjectAddress resolves the ObjectAddress for the
 * TEXT SEARCH CONFIGURATION being moved to a different schema. Optionally errors if the
 * configuration does not exist based on the missing_ok flag passed in by the caller.
 *
 * This can be called, either before or after the move of schema has been executed, hence
 * the triple checking before the error might be thrown. Errors for non-existing schema's
 * in edgecases will be raised by postgres while executing the move.
 */
ObjectAddress
AlterTextSearchConfigurationSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	Oid objid = get_ts_config_oid(castNode(List, stmt->object), true);

	if (!OidIsValid(objid))
	{
		/*
		 * couldn't find the text search configuration, might have already been moved to
		 * the new schema, we construct a new sequence name that uses the new schema to
		 * search in.
		 */
		char *schemaname = NULL;
		char *config_name = NULL;
		DeconstructQualifiedName(castNode(List, stmt->object), &schemaname, &config_name);

		char *newSchemaName = stmt->newschema;
		List *names = list_make2(makeString(newSchemaName), makeString(config_name));
		objid = get_ts_config_oid(names, true);

		if (!missing_ok && !OidIsValid(objid))
		{
			/*
			 * if the text search config id is still invalid we couldn't find it, error
			 * with the same message postgres would error with if missing_ok is false
			 * (not ok to miss)
			 */

			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("text search configuration \"%s\" does not exist",
							NameListToString(castNode(List, stmt->object)))));
		}
	}

	ObjectAddress sequenceAddress = { 0 };
	ObjectAddressSet(sequenceAddress, TSConfigRelationId, objid);
	return sequenceAddress;
}


/*
 * TextSearchConfigurationCommentObjectAddress resolves the ObjectAddress for the TEXT
 * SEARCH CONFIGURATION on which the comment is placed. Optionally errors if the
 * configuration does not exist based on the missing_ok flag passed in by the caller.
 */
ObjectAddress
TextSearchConfigurationCommentObjectAddress(Node *node, bool missing_ok)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSCONFIGURATION);

	Oid objid = get_ts_config_oid(castNode(List, stmt->object), missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TSConfigRelationId, objid);
	return address;
}


/*
 * AlterTextSearchConfigurationOwnerObjectAddress resolves the ObjectAddress for the TEXT
 * SEARCH CONFIGURATION for which the owner is changed. Optionally errors if the
 * configuration does not exist based on the missing_ok flag passed in by the caller.
 */
ObjectAddress
AlterTextSearchConfigurationOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Relation relation = NULL;

	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	return get_object_address(stmt->objectType, stmt->object, &relation, AccessShareLock,
							  missing_ok);
}


/*
 * GenerateBackupNameForTextSearchConfiguration generates a safe name that is not in use
 * already that can be used to rename an existing TEXT SEARCH CONFIGURATION to allow the
 * configuration with a specific name to be created, even if this would not have been
 * possible due to name collisions.
 */
char *
GenerateBackupNameForTextSearchConfiguration(const ObjectAddress *address)
{
	Assert(address->classId == TSConfigRelationId);
	List *names = get_ts_config_namelist(address->objectId);

	RangeVar *rel = makeRangeVarFromNameList(names);

	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	char *baseName = rel->relname;
	int baseLength = strlen(baseName);
	int count = 0;

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


		rel->relname = newName;
		List *newNameList = MakeNameListFromRangeVar(rel);

		Oid tsconfigOid = get_ts_config_oid(newNameList, true);
		if (!OidIsValid(tsconfigOid))
		{
			return newName;
		}

		count++;
	}
}
