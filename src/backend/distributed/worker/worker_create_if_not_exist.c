/*-------------------------------------------------------------------------
 *
 * worker_create_if_not_exist.c
 *   TODO rename file and document, was named after old function
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "parser/parse_type.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/regproc.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/worker_protocol.h"

PG_FUNCTION_INFO_V1(worker_create_or_replace);
PG_FUNCTION_INFO_V1(type_recreate_command);


static bool object_from_create_exists(Node *parseTree);
static DropStmt * drop_stmt_from_object_create(Node *createStmt);


static bool
object_from_create_exists(Node *parseTree)
{
	switch (parseTree->type)
	{
		case T_CompositeTypeStmt:
		{
			return CompositeTypeExists(castNode(CompositeTypeStmt, parseTree));
		}

		case T_CreateEnumStmt:
		{
			return EnumTypeExists(castNode(CreateEnumStmt, parseTree));
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported statement to check existence for"),
							errhint("The coordinator send an unsupported command to the "
									"worker")));
			return false;
		}
	}
}


static DropStmt *
drop_stmt_from_object_create(Node *createStmt)
{
	switch (nodeTag(createStmt))
	{
		case T_CompositeTypeStmt:
		{
			return CompositeTypeStmtToDrop(castNode(CompositeTypeStmt, createStmt));
		}

		case T_CreateEnumStmt:
		{
			return CreateEnumStmtToDrop(castNode(CreateEnumStmt, createStmt));
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported statement to check existence for"),
							errhint("The coordinator send an unsupported command to the "
									"worker")));
		}
	}
}


Datum
worker_create_or_replace(PG_FUNCTION_ARGS)
{
	text *sqlStatementText = PG_GETARG_TEXT_P(0);
	const char *sqlStatement = text_to_cstring(sqlStatementText);

	Node *parseTree = ParseTreeNode(sqlStatement);

	/*
	 * since going to the drop statement might require some resolving we will do a check
	 * if the type actually exists instead of adding the IF EXISTS keyword to the
	 * statement.
	 */
	if (object_from_create_exists(parseTree))
	{
		/* TODO check if object is equal to what we plan to create */

		DropStmt *dropStmtParseTree = NULL;

		/*
		 * there might be dependencies left on the worker on this type, these are not
		 * managed by citus anyway so it should be ok to drop, thus we cascade to any such
		 * dependencies
		 */
		dropStmtParseTree = drop_stmt_from_object_create(parseTree);
		dropStmtParseTree->behavior = DROP_CASCADE;

		if (dropStmtParseTree != NULL)
		{
			const char *sqlDropStmt = DeparseTreeNode((Node *) dropStmtParseTree);
			CitusProcessUtility((Node *) dropStmtParseTree, sqlDropStmt,
								PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);
		}
	}

	CitusProcessUtility(parseTree, sqlStatement, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	/* type has been created */
	PG_RETURN_BOOL(true);
}


/*
 * type_recreate_command(typename text) text
 */
Datum
type_recreate_command(PG_FUNCTION_ARGS)
{
	text *typeNameText = PG_GETARG_TEXT_P(0);
	const char *typeNameStr = text_to_cstring(typeNameText);
	List *typeNameList = stringToQualifiedNameList(typeNameStr);
	TypeName *typeName = makeTypeNameFromNameList(typeNameList);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);

	Node *stmt = RecreateTypeStatement(typeOid);

	switch (stmt->type)
	{
		case T_CreateEnumStmt:
		{
			const char *createEnumStmtSql = NULL;
			createEnumStmtSql = deparse_create_enum_stmt(castNode(CreateEnumStmt, stmt));
			return CStringGetTextDatum(createEnumStmtSql);
		}

		case T_CompositeTypeStmt:
		{
			const char *compositeTypeStmtSql = NULL;
			compositeTypeStmtSql = deparse_composite_type_stmt(castNode(CompositeTypeStmt,
																		stmt));
			return CStringGetTextDatum(compositeTypeStmtSql);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported statement for deparse")));
		}
	}

	PG_RETURN_VOID();
}
