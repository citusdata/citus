/*-------------------------------------------------------------------------
 *
 * text_search.c
 *    Commands for creating and altering TEXT SEARCG objects
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


static DefineStmt * GetTextSearchConfigDefineStmt(Oid tsconfigOid);
static List * GetTextSearchConfigCommentStmt(Oid tsconfigOid);
static List * GetTSParserNameList(Oid tsparserOid);
static List * GetTextSearchConfigMappingStmt(Oid tsconfigOid);
static List * GetTextSearchConfigOwnerStmts(Oid tsconfigOid);

static List * get_ts_dict_namelist(Oid tsdictOid);
static Oid get_ts_config_parser(Oid tsconfigOid);
static char * get_ts_parser_tokentype_name(Oid parserOid, int32 tokentype);

static void EnsureSequentialModeForTextSearchConfigurationDDL(void);

List *
PostprocessCreateTextSearchConfigurationStmt(Node *node, const char *queryString)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_TSCONFIGURATION);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/*
	 * If the create command is a part of a multi-statement transaction that is not in
	 * sequential mode, don't propagate. Instead we will rely on back filling.
	 */
	if (IsMultiStatementTransaction())
	{
		if (MultiShardConnectionType != SEQUENTIAL_CONNECTION)
		{
			return NIL;
		}
	}

	EnsureCoordinator();
	EnsureSequentialModeForTextSearchConfigurationDDL();

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


/*
 * CreateTextSearchConfigDDLCommandsIdempotent creates a list of ddl commands to recreate
 * a TEXT SERACH CONFIGURATION object in an idempotent manner on workers.
 */
List *
CreateTextSearchConfigDDLCommandsIdempotent(const ObjectAddress *address)
{
	Assert(address->classId == TSConfigRelationId);
	List *commands = NIL;

	/* CREATE TEXT SEARCH CONFIGURATION ...*/
	DefineStmt *defineStmt = GetTextSearchConfigDefineStmt(address->objectId);
	commands = lappend(commands,
					   WrapCreateOrReplace(DeparseTreeNode((Node *) defineStmt)));

	List *configurationStmts = NIL;

	/* ALTER TEXT SEARCH CONFIGURATION ... OWNER TO ...*/
	configurationStmts = list_concat(configurationStmts,
									 GetTextSearchConfigOwnerStmts(address->objectId));

	/* COMMENT ON TEXT SEARCH CONFIGURATION ... */
	configurationStmts = list_concat(configurationStmts,
									 GetTextSearchConfigCommentStmt(address->objectId));


	/* ALTER TEXT SEARCH CONFIGURATION ... ADD MAPPING FOR ... WITH ... */
	configurationStmts = list_concat(configurationStmts,
									 GetTextSearchConfigMappingStmt(address->objectId));

	/* deparse all statements into sql */
	Node *stmt = NULL;
	foreach_ptr(stmt, configurationStmts)
	{
		char *sql = DeparseTreeNode(stmt);
		commands = lappend(commands, sql);
	}

	return commands;
}


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

	/*
	 * iterate over all text search configurations dropped, and create a list
	 * of all objects that are distributed.
	 */
	List *objName = NULL;
	List *distributedObjects = NIL;
	foreach_ptr(objName, stmt->objects)
	{
		Oid tsconfigOid = get_ts_config_oid(objName, false);
		ObjectAddress address = { 0 };
		ObjectAddressSet(address, TSConfigRelationId, tsconfigOid);
		if (!IsObjectDistributed(&address))
		{
			continue;
		}
		distributedObjects = lappend(distributedObjects, objName);
	}

	if (list_length(distributedObjects) == 0)
	{
		/* no distributed objects to remove */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForTextSearchConfigurationDDL();

	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedObjects;
	QualifyTreeNode((Node *) stmtCopy);
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


List *
PreprocessAlterTextSearchConfigurationStmt(Node *node, const char *queryString,
										   ProcessUtilityContext processUtilityContext)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForTextSearchConfigurationDDL();

	QualifyTreeNode((Node *) stmt);
	const char *alterStmtSql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


List *
PreprocessRenameTextSearchConfigurationStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSCONFIGURATION);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(&address))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialModeForTextSearchConfigurationDDL();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) ddlCommand,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


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
	EnsureSequentialModeForTextSearchConfigurationDDL();
	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


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
	EnsureSequentialModeForTextSearchConfigurationDDL();

	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


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
	EnsureSequentialModeForTextSearchConfigurationDDL();

	QualifyTreeNode((Node *) stmt);
	char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


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

	List *parserNameList = GetTSParserNameList(config->cfgparser);
	TypeName *parserTypeName = makeTypeNameFromNameList(parserNameList);
	stmt->definition = list_make1(makeDefElem("parser", (Node *) parserTypeName, -1));

	ReleaseSysCache(tup);
	return stmt;
}


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

	Oid parserOid = get_ts_config_parser(tsconfigOid);

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
	index_close(mapidx, AccessShareLock);
	table_close(maprel, AccessShareLock);

	return stmts;
}


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


static Oid
get_ts_config_parser(Oid tsconfigOid)
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
	int j = 0;
	while (tokenlist && tokenlist[j].lexid)
	{
		if (tokenlist[j].lexid == tokentype)
		{
			return pstrdup(tokenlist[j].alias);
		}
		j++;
	}

	/* we haven't found the token */
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("token type \"%d\" does not exist in parser", tokentype)));
}


static List *
GetTSParserNameList(Oid tsparserOid)
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


ObjectAddress
AlterTextSearchConfigurationStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	Oid objid = get_ts_config_oid(stmt->cfgname, missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TSConfigRelationId, objid);
	return address;
}


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


ObjectAddress
AlterTextSearchConfigurationOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Relation relation = NULL;

	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	return get_object_address(stmt->objectType, stmt->object, &relation, AccessShareLock,
							  missing_ok);
}


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


/*
 * EnsureSequentialModeForTextSearchConfigurationDDL makes sure that the current
 * transaction is already in sequential mode, or can still safely be put in sequential
 * mode, it errors if that is not possible. The error contains information for the user to
 * retry the transaction with sequential mode set from the beginning.
 *
 * As text search configurations are node scoped objects there exists only 1 instance of
 * the text search configuration used by  potentially multiple shards. To make sure all
 * shards in the transaction can interact with the text search configuration needs to be
 * visible on all connections used by the transaction, meaning we can only use 1
 * connection per node.
 */
static void
EnsureSequentialModeForTextSearchConfigurationDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify text search configuration "
							   "because there was a parallel operation on a distributed "
							   "table in the transaction"),
						errdetail(
							"When creating or altering a text search configuration, Citus "
							"needs to perform all operations over a single connection per "
							"node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "Text search configuration is created or altered. To make sure "
						 "subsequent commands see the Text search configuration correctly "
						 "we need to make sure to use only one connection for all future "
						 "commands")));
	SetLocalMultiShardModifyModeToSequential();
}
