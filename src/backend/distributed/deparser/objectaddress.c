/*-------------------------------------------------------------------------
 *
 * objectaddress.c
 *    Parstrees almost always target a object that postgres can address by
 *    an ObjectAddress. Here we have a walker for parsetrees to find the
 *    address of the object targeted.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_extension_d.h"

static ObjectAddress * AlterTableStmtObjectAddress(AlterTableStmt *stmt, bool missing_ok);
static ObjectAddress * RenameStmtObjectAddress(RenameStmt *stmt, bool missing_ok);
static ObjectAddress * AlterObjectSchemaStmtObjectAddress(AlterObjectSchemaStmt *stmt,
														  bool missing_ok);
static ObjectAddress * RenameAttributeStmtObjectAddress(RenameStmt *stmt, bool
														missing_ok);
static ObjectAddress * AlterOwnerStmtObjectAddress(AlterOwnerStmt *stmt, bool missing_ok);
static ObjectAddress * AlterObjectDependsStmtObjectAddress(AlterObjectDependsStmt *stmt,
														   bool missing_ok);
static ObjectAddress * CreateExtensionStmtObjectAddress(CreateExtensionStmt *stmt, bool
														missing_ok);
static ObjectAddress * AlterExtensionStmtObjectAddress(
	AlterExtensionStmt *alterExtensionStmt, bool missing_ok);

/*
 * GetObjectAddressFromParseTree returns the ObjectAddress of the main target of the parse
 * tree.
 */
ObjectAddress *
GetObjectAddressFromParseTree(Node *parseTree, bool missing_ok)
{
	switch (parseTree->type)
	{
		case T_CompositeTypeStmt:
		{
			return CompositeTypeStmtObjectAddress(castNode(CompositeTypeStmt, parseTree),
												  missing_ok);
		}

		case T_AlterTableStmt:
		{
			return AlterTableStmtObjectAddress(castNode(AlterTableStmt, parseTree),
											   missing_ok);
		}

		case T_CreateEnumStmt:
		{
			return CreateEnumStmtObjectAddress(castNode(CreateEnumStmt, parseTree),
											   missing_ok);
		}

		case T_AlterEnumStmt:
		{
			return AlterEnumStmtObjectAddress(castNode(AlterEnumStmt, parseTree),
											  missing_ok);
		}

		case T_RenameStmt:
		{
			return RenameStmtObjectAddress(castNode(RenameStmt, parseTree), missing_ok);
		}

		case T_AlterObjectSchemaStmt:
		{
			return AlterObjectSchemaStmtObjectAddress(castNode(AlterObjectSchemaStmt,
															   parseTree), missing_ok);
		}

		case T_AlterOwnerStmt:
		{
			return AlterOwnerStmtObjectAddress(castNode(AlterOwnerStmt, parseTree),
											   missing_ok);
		}

		case T_AlterFunctionStmt:
		{
			return AlterFunctionStmtObjectAddress(castNode(AlterFunctionStmt, parseTree),
												  missing_ok);
		}

		case T_CreateFunctionStmt:
		{
			return CreateFunctionStmtObjectAddress(
				castNode(CreateFunctionStmt, parseTree), missing_ok);
		}

		case T_AlterObjectDependsStmt:
		{
			return AlterObjectDependsStmtObjectAddress(
				castNode(AlterObjectDependsStmt, parseTree), missing_ok);
		}

		case T_DefineStmt:
		{
			DefineStmt *stmt = castNode(DefineStmt, parseTree);

			switch (stmt->kind)
			{
				case OBJECT_AGGREGATE:
				{
					return DefineAggregateStmtObjectAddress(stmt, missing_ok);
				}

				case OBJECT_COLLATION:
				{
					return DefineCollationStmtObjectAddress(stmt, missing_ok);
				}

				default:
				{
					break;
				}
			}

			ereport(ERROR, (errmsg(
								"unsupported object type to get object address for DefineStmt")));
			return NULL;
		}

		case T_CreateExtensionStmt:
		{
			return CreateExtensionStmtObjectAddress(castNode(CreateExtensionStmt,
															 parseTree), missing_ok);
		}

		case T_AlterExtensionStmt:
		{
			return AlterExtensionStmtObjectAddress(castNode(AlterExtensionStmt,
															parseTree), missing_ok);
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported statement to get object address for")));
			return NULL;
		}
	}
}


static ObjectAddress *
AlterTableStmtObjectAddress(AlterTableStmt *stmt, bool missing_ok)
{
	switch (stmt->relkind)
	{
		case OBJECT_TYPE:
		{
			return AlterTypeStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter statement to get object address for"
								   )));
		}
	}
}


static ObjectAddress *
RenameStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			return RenameTypeStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_ATTRIBUTE:
		{
			return RenameAttributeStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_COLLATION:
		{
			return RenameCollationStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return RenameFunctionStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename statement to get object address "
								   "for")));
		}
	}
}


static ObjectAddress *
AlterObjectSchemaStmtObjectAddress(AlterObjectSchemaStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return AlterTypeSchemaStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_COLLATION:
		{
			return AlterCollationSchemaStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionSchemaStmtObjectAddress(stmt, missing_ok);
		}

		case OBJECT_EXTENSION:
		{
			return AlterExtensionSchemaStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter schema statement to get object "
								   "address for")));
		}
	}
}


static ObjectAddress *
RenameAttributeStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return RenameTypeAttributeStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter rename attribute statement to get "
								   "object address for")));
		}
	}
}


static ObjectAddress *
AlterOwnerStmtObjectAddress(AlterOwnerStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_COLLATION:
		{
			ObjectAddress *address = palloc(sizeof(ObjectAddress));
			*address = AlterCollationOwnerObjectAddress(stmt);
			return address;
		}

		case OBJECT_TYPE:
		{
			return AlterTypeOwnerObjectAddress(stmt, missing_ok);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionOwnerObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter owner statement to get object "
								   "address for")));
		}
	}
}


/*
 * AlterObjectDependsStmtObjectAddress resolves the ObjectAddress for the object targeted
 * by the AlterObjectDependStmt. This is done by dispatching the call to the object
 * specific implementation based on the ObjectType captured in the original statement. If
 * a specific implementation is not present an error will be raised. This is a developer
 * error since this function should only be reachable by calls of supported types.
 *
 * If missing_ok is set to fails the object specific implementation is supposed to raise
 * an error explaining the user the object is not existing.
 */
static ObjectAddress *
AlterObjectDependsStmtObjectAddress(AlterObjectDependsStmt *stmt, bool missing_ok)
{
	switch (stmt->objectType)
	{
		case OBJECT_PROCEDURE:
		case OBJECT_FUNCTION:
		{
			return AlterFunctionDependsStmtObjectAddress(stmt, missing_ok);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported alter depends on extension statement to "
								   "get object address for")));
		}
	}
}


/*
 * CreateExtensionStmtObjectAddress finds the ObjectAddress for the extension described
 * by the CreateExtensionStmt. If missing_ok is false, then this function throws an
 * error if the extension does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
static ObjectAddress *
CreateExtensionStmtObjectAddress(CreateExtensionStmt *createExtensionStmt, bool
								 missing_ok)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	const char *extensionName = createExtensionStmt->extname;

	Oid extensionoid = get_extension_oid(extensionName, missing_ok);

	/* if we couldn't find the extension, error if missing_ok is false */
	if (!missing_ok && extensionoid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

	return address;
}


/*
 * AlterExtensionStmtObjectAddress finds the ObjectAddress for the extension described
 * by the AlterExtensionStmt. If missing_ok is false, then this function throws an
 * error if the extension is not created before.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
static ObjectAddress *
AlterExtensionStmtObjectAddress(AlterExtensionStmt *alterExtensionStmt, bool
								missing_ok)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	const char *extensionName = alterExtensionStmt->extname;

	Oid extensionoid = get_extension_oid(extensionName, missing_ok);

	/* if we couldn't find the extension, error if missing_ok is false */
	if (!missing_ok && extensionoid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("extension \"%s\" does not exist",
							   extensionName)));
	}

	ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

	return address;
}
