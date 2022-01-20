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

#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_parser.h"
#include "commands/extension.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"


static List * GetTextSearchConfigCreateStmts(Oid tsconfigOid);
static DefineStmt * GetTextSearchConfigDefineStmt(Oid tsconfigOid);
static List * GetTSParserNameList(Oid tsparserOid);

/*
 * CreateTextSearchConfigDDLCommandsIdempotent creates a list of ddl commands to recreate
 * a TEXT SERACH CONFIGURATION object in an idempotent manner on workers.
 */
List *
CreateTextSearchConfigDDLCommandsIdempotent(const ObjectAddress *address)
{
	Assert(address->classId == TSConfigRelationId);

	List *stmts = GetTextSearchConfigCreateStmts(address->objectId);

	List *commands = NIL;
	Node *stmt = NULL;
	foreach_ptr(stmt, stmts)
	{
		char *command = DeparseTreeNode(stmt);
		commands = lappend(commands, command);
	}

	return commands;
}


List *
PreprocessDropTextSearchConfigurationStmt(Node *node, const char *queryString,
										  ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_TSCONFIGURATION);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, text search
		 * configurations cascading from an extension should therefore not be
		 * propagated here.
		 */
		return NIL;
	}

	if (!EnableDependencyCreation)
	{
		/* disabled object propagation, should not propagate anything */
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

	/* TODO do we need to unmark objects distributed? */

	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedObjects;
	QualifyTreeNode((Node *) stmtCopy);
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * GetTextSearchConfigCreateStmts creates statements for a TEXT SEARCH CONFIGURATION to
 * recreate the config based on the entries found in the local catalog.
 */
static List *
GetTextSearchConfigCreateStmts(Oid tsconfigOid)
{
	List *stmts = NIL;

	DefineStmt *defineStmt = GetTextSearchConfigDefineStmt(tsconfigOid);
	stmts = lappend(stmts, defineStmt);

	return stmts;
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

	char *schema = get_namespace_name(config->cfgnamespace);
	char *configName = pstrdup(NameStr(config->cfgname));
	stmt->defnames = list_make2(makeString(schema), makeString(configName));

	List *parserNameList = GetTSParserNameList(config->cfgparser);
	TypeName *parserTypeName = makeTypeNameFromNameList(parserNameList);
	stmt->definition = list_make1(makeDefElem("parser", (Node *) parserTypeName, -1));

	ReleaseSysCache(tup);
	return stmt;
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
