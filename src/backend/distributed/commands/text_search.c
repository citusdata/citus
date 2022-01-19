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

#include "catalog/objectaddress.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "nodes/makefuncs.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"


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
