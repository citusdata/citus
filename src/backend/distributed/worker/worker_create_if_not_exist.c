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

#include "catalog/dependency.h"
#include "catalog/pg_type.h"
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
#include "distributed/metadata/distobject.h"
#include "distributed/worker_protocol.h"

PG_FUNCTION_INFO_V1(worker_create_or_replace);


static const ObjectAddress * GetObjectAddressFromParseTree(Node *parseTree, bool
														   missing_ok);
static DropStmt * drop_stmt_from_object_create(Node *createStmt);


static const ObjectAddress *
GetObjectAddressFromParseTree(Node *parseTree, bool missing_ok)
{
	switch (parseTree->type)
	{
		case T_CompositeTypeStmt:
		{
			return CompositeTypeStmtObjectAddress(castNode(CompositeTypeStmt, parseTree),
												  missing_ok);
		}

		case T_CreateEnumStmt:
		{
			return CreateEnumStmtObjectAddress(castNode(CreateEnumStmt, parseTree),
											   missing_ok);
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported statement to get object address for"),
							errhint("The coordinator send an unsupported command to the "
									"worker")));
			return NULL;
		}
	}
}


static Node *
CreateStmtByObjectAddress(const ObjectAddress *address)
{
	switch (getObjectClass(address))
	{
		case OCLASS_TYPE:
		{
			return CreateTypeStmtByObjectAddress(address);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported object to construct a create statment")));
		}
	}
}


/* TODO rewrite to create based on ObjectAddress */
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


/*
 * worker_create_or_replace(statement text)
 *
 * function is called, by the coordinator, with a CREATE statement for an object. This
 * function implements the CREATE ... IF NOT EXISTS functionality for objects that do not
 * have this functionality or where their implementation is not sufficient.
 *
 * Besides checking if an object of said name exists it tries to compare the object to be
 * created with the one in the local catalog. If there is a difference the on in the local
 * catalog will be renamed after which the statement can be executed on this worker to
 * create the object.
 *
 * Renaming has two purposes
 *  - free the identifier for creation
 *  - non destructive if there is data store that would be destroyed if the object was
 *    used in a table on this node, eg. types. If the type would be dropped with a cascade
 *    it would drop any column holding user data for this type.
 */
Datum
worker_create_or_replace(PG_FUNCTION_ARGS)
{
	text *sqlStatementText = PG_GETARG_TEXT_P(0);
	const char *sqlStatement = text_to_cstring(sqlStatementText);
	const ObjectAddress *address = NULL;
	Node *parseTree = ParseTreeNode(sqlStatement);

	/*
	 * since going to the drop statement might require some resolving we will do a check
	 * if the type actually exists instead of adding the IF EXISTS keyword to the
	 * statement.
	 */
	address = GetObjectAddressFromParseTree(parseTree, true);
	if (ObjectExists(address))
	{
		Node *localCreateStmt = NULL;
		const char *localSqlStatement = NULL;
		DropStmt *dropStmtParseTree = NULL;

		localCreateStmt = CreateStmtByObjectAddress(address);
		localSqlStatement = DeparseTreeNode(localCreateStmt);
		if (strcmp(sqlStatement, localSqlStatement) == 0)
		{
			/*
			 * TODO string compare is a poor mans comparison, but calling equal on the
			 * parsetree's returns false because there is extra information list character
			 * position of some sort
			 */

			/*
			 * parseTree sent by the coordinator is the same as we would create for our
			 * object, therefore we can omit the create statement locally and not create
			 * the object as it already exists.
			 *
			 * We let the coordinator know we didn't create the object.
			 */
			PG_RETURN_BOOL(false);
		}

		/* TODO for now we assume that we always recreate the same object crash if not */
		Assert(false);

		/* TODO don't drop, instead rename as described in documentation */

		/*
		 * there might be dependencies left on the worker on this type, these are not
		 * managed by citus anyway so it should be ok to drop, thus we cascade to any such
		 * dependencies
		 */
		dropStmtParseTree = drop_stmt_from_object_create(parseTree);

		if (dropStmtParseTree != NULL)
		{
			const char *sqlDropStmt = NULL;

			/* force the drop */
			dropStmtParseTree->behavior = DROP_CASCADE;

			sqlDropStmt = DeparseTreeNode((Node *) dropStmtParseTree);
			CitusProcessUtility((Node *) dropStmtParseTree, sqlDropStmt,
								PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);
		}
	}

	/* apply create statement locally */
	CitusProcessUtility(parseTree, sqlStatement, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	/* type has been created */
	PG_RETURN_BOOL(true);
}
