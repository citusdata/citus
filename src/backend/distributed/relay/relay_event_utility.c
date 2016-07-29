/*-------------------------------------------------------------------------
 *
 * relay_event_utility.c
 *
 * Routines for handling DDL statements that relate to relay files. These
 * routines extend relation, index and constraint names in utility commands.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stdio.h>
#include <string.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "utils/relcache.h"


/* Local functions forward declarations */
static bool TypeAddIndexConstraint(const AlterTableCmd *command);
static bool TypeDropIndexConstraint(const AlterTableCmd *command,
									const RangeVar *relation, uint64 shardId);
static void AppendShardIdToConstraintName(AlterTableCmd *command, uint64 shardId);
static void SetSchemaNameIfNotExist(char **schemaName, char *newSchemaName);


/*
 * RelayEventExtendNames extends relation names in the given parse tree for
 * certain utility commands. The function more specifically extends table and
 * index names in the parse tree by appending the given shardId; thereby
 * avoiding name collisions in the database among sharded tables. This function
 * has the side effect of extending relation names in the parse tree.
 */
void
RelayEventExtendNames(Node *parseTree, char *schemaName, uint64 shardId)
{
	/* we don't extend names in extension or schema commands */
	NodeTag nodeType = nodeTag(parseTree);
	if (nodeType == T_CreateExtensionStmt || nodeType == T_CreateSchemaStmt ||
		nodeType == T_CreateSeqStmt || nodeType == T_AlterSeqStmt)
	{
		return;
	}

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			/*
			 * We append shardId to the very end of table and index names to
			 * avoid name collisions. We usually do not touch constraint names,
			 * except for cases where they refer to index names. In such cases,
			 * we also append to constraint names.
			 */

			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parseTree;
			char **relationName = &(alterTableStmt->relation->relname);
			char **relationSchemaName = &(alterTableStmt->relation->schemaname);
			RangeVar *relation = alterTableStmt->relation;  /* for constraints */

			List *commandList = alterTableStmt->cmds;
			ListCell *commandCell = NULL;

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			/* append shardId to base relation name */
			AppendShardIdToName(relationName, shardId);

			foreach(commandCell, commandList)
			{
				AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);

				if (TypeAddIndexConstraint(command) ||
					TypeDropIndexConstraint(command, relation, shardId))
				{
					AppendShardIdToConstraintName(command, shardId);
				}
				else if (command->subtype == AT_ClusterOn)
				{
					char **indexName = &(command->name);
					AppendShardIdToName(indexName, shardId);
				}
			}

			break;
		}

		case T_ClusterStmt:
		{
			ClusterStmt *clusterStmt = (ClusterStmt *) parseTree;
			char **relationName = NULL;
			char **relationSchemaName = NULL;

			/* we do not support clustering the entire database */
			if (clusterStmt->relation == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for multi-relation cluster")));
			}

			relationName = &(clusterStmt->relation->relname);
			relationSchemaName = &(clusterStmt->relation->schemaname);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);

			if (clusterStmt->indexname != NULL)
			{
				char **indexName = &(clusterStmt->indexname);
				AppendShardIdToName(indexName, shardId);
			}

			break;
		}

		case T_CreateForeignServerStmt:
		{
			CreateForeignServerStmt *serverStmt = (CreateForeignServerStmt *) parseTree;
			char **serverName = &(serverStmt->servername);

			AppendShardIdToName(serverName, shardId);
			break;
		}

		case T_CreateForeignTableStmt:
		{
			CreateForeignTableStmt *createStmt = (CreateForeignTableStmt *) parseTree;
			char **serverName = &(createStmt->servername);

			AppendShardIdToName(serverName, shardId);

			/*
			 * Since CreateForeignTableStmt inherits from CreateStmt and any change
			 * performed on CreateStmt should be done here too, we simply *fall
			 * through* to avoid code repetition.
			 */
		}

		case T_CreateStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) parseTree;
			char **relationName = &(createStmt->relation->relname);
			char **relationSchemaName = &(createStmt->relation->schemaname);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);
			break;
		}

		case T_DropStmt:
		{
			DropStmt *dropStmt = (DropStmt *) parseTree;
			ObjectType objectType = dropStmt->removeType;

			if (objectType == OBJECT_TABLE || objectType == OBJECT_INDEX ||
				objectType == OBJECT_FOREIGN_TABLE || objectType == OBJECT_FOREIGN_SERVER)
			{
				List *relationNameList = NULL;
				int relationNameListLength = 0;
				Value *relationSchemaNameValue = NULL;
				Value *relationNameValue = NULL;
				char **relationName = NULL;

				uint32 dropCount = list_length(dropStmt->objects);
				if (dropCount > 1)
				{
					ereport(ERROR,
							(errmsg("cannot extend name for multiple drop objects")));
				}

				/*
				 * We now need to extend a single relation or index name. To be
				 * able to do this extension, we need to extract the names'
				 * addresses from the value objects they are stored in. Other-
				 * wise, the repalloc called in AppendShardIdToName() will not
				 * have the correct memory address for the name.
				 */

				relationNameList = (List *) linitial(dropStmt->objects);
				relationNameListLength = list_length(relationNameList);

				switch (relationNameListLength)
				{
					case 1:
					{
						relationNameValue = linitial(relationNameList);
						break;
					}

					case 2:
					{
						relationSchemaNameValue = linitial(relationNameList);
						relationNameValue = lsecond(relationNameList);
						break;
					}

					case 3:
					{
						relationSchemaNameValue = lsecond(relationNameList);
						relationNameValue = lthird(relationNameList);
						break;
					}

					default:
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("improper relation name: \"%s\"",
											   NameListToString(relationNameList))));
						break;
					}
				}

				/* prefix with schema name if it is not added already */
				if (relationSchemaNameValue == NULL)
				{
					Value *schemaNameValue = makeString(pstrdup(schemaName));
					relationNameList = lcons(schemaNameValue, relationNameList);
				}

				relationName = &(relationNameValue->val.str);
				AppendShardIdToName(relationName, shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe object type in drop statement"),
								  errdetail("Object type: %u", (uint32) objectType)));
			}

			break;
		}


		case T_GrantStmt:
		{
			GrantStmt *grantStmt = (GrantStmt *) parseTree;

			if (grantStmt->targtype == ACL_TARGET_OBJECT &&
				grantStmt->objtype == ACL_OBJECT_RELATION)
			{
				ListCell *lc;

				foreach(lc, grantStmt->objects)
				{
					RangeVar *relation = (RangeVar *) lfirst(lc);
					char **relationName = &(relation->relname);
					char **relationSchemaName = &(relation->schemaname);

					/* prefix with schema name if it is not added already */
					SetSchemaNameIfNotExist(relationSchemaName, schemaName);

					AppendShardIdToName(relationName, shardId);
				}
			}
			break;
		}

		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parseTree;
			char **relationName = &(indexStmt->relation->relname);
			char **indexName = &(indexStmt->idxname);
			char **relationSchemaName = &(indexStmt->relation->schemaname);

			/*
			 * Concurrent index statements cannot run within a transaction block.
			 * Therefore, we do not support them.
			 */
			if (indexStmt->concurrent)
			{
				ereport(ERROR, (errmsg("cannot extend name for concurrent index")));
			}

			/*
			 * In the regular DDL execution code path (for non-sharded tables),
			 * if the index statement results from a table creation command, the
			 * indexName may be null. For sharded tables however, we intercept
			 * that code path and explicitly set the index name. Therefore, the
			 * index name in here cannot be null.
			 */
			if ((*indexName) == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for null index name")));
			}

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);
			AppendShardIdToName(indexName, shardId);
			break;
		}

		case T_ReindexStmt:
		{
			ReindexStmt *reindexStmt = (ReindexStmt *) parseTree;

			ReindexObjectType objectType = reindexStmt->kind;
			if (objectType == REINDEX_OBJECT_TABLE || objectType == REINDEX_OBJECT_INDEX)
			{
				char **objectName = &(reindexStmt->relation->relname);
				char **objectSchemaName = &(reindexStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(objectName, shardId);
			}
			else if (objectType == REINDEX_OBJECT_DATABASE)
			{
				ereport(ERROR, (errmsg("cannot extend name for multi-relation reindex")));
			}
			else
			{
				ereport(ERROR, (errmsg("invalid object type in reindex statement"),
								errdetail("Object type: %u", (uint32) objectType)));
			}

			break;
		}

		case T_RenameStmt:
		{
			RenameStmt *renameStmt = (RenameStmt *) parseTree;
			ObjectType objectType = renameStmt->renameType;

			if (objectType == OBJECT_TABLE || objectType == OBJECT_INDEX)
			{
				char **oldRelationName = &(renameStmt->relation->relname);
				char **newRelationName = &(renameStmt->newname);
				char **objectSchemaName = &(renameStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(oldRelationName, shardId);
				AppendShardIdToName(newRelationName, shardId);
			}
			else if (objectType == OBJECT_COLUMN || objectType == OBJECT_TRIGGER)
			{
				char **relationName = &(renameStmt->relation->relname);
				char **objectSchemaName = &(renameStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(relationName, shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe object type in rename statement"),
								  errdetail("Object type: %u", (uint32) objectType)));
			}

			break;
		}

		case T_TruncateStmt:
		{
			/*
			 * We currently do not support truncate statements. This is
			 * primarily because truncates allow implicit modifications to
			 * sequences through table column dependencies. As we have not
			 * determined our dependency model for sequences, we error here.
			 */
			ereport(ERROR, (errmsg("cannot extend name for truncate statement")));
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg("unsafe statement type in name extension"),
							  errdetail("Statement type: %u", (uint32) nodeType)));
			break;
		}
	}
}


/*
 * TypeAddIndexConstraint checks if the alter table command adds a constraint
 * and if that constraint also results in an index creation.
 */
static bool
TypeAddIndexConstraint(const AlterTableCmd *command)
{
	if (command->subtype == AT_AddConstraint)
	{
		if (IsA(command->def, Constraint))
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_PRIMARY ||
				constraint->contype == CONSTR_UNIQUE)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * TypeDropIndexConstraint checks if the alter table command drops a constraint
 * and if that constraint also results in an index drop. Note that drop
 * constraints do not have access to constraint type information; this is in
 * contrast with add constraint commands. This function therefore performs
 * additional system catalog lookups to determine if the drop constraint is
 * associated with an index.
 */
static bool
TypeDropIndexConstraint(const AlterTableCmd *command,
						const RangeVar *relation, uint64 shardId)
{
	Relation pgConstraint = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	char *searchedConstraintName = NULL;
	bool indexConstraint = false;
	Oid relationId = InvalidOid;
	bool failOK = true;

	if (command->subtype != AT_DropConstraint)
	{
		return false;
	}

	/*
	 * At this stage, our only option is performing a relationId lookup. We
	 * first find the relationId, and then scan the pg_constraints system
	 * catalog using this relationId. Finally, we check if the passed in
	 * constraint is for a primary key or unique index.
	 */
	relationId = RangeVarGetRelid(relation, NoLock, failOK);
	if (!OidIsValid(relationId))
	{
		/* overlook this error, it should be signaled later in the pipeline */
		return false;
	}

	searchedConstraintName = pnstrdup(command->name, NAMEDATALEN);
	AppendShardIdToName(&searchedConstraintName, shardId);

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	scanDescriptor = systable_beginscan(pgConstraint,
										ConstraintRelidIndexId, true, /* indexOK */
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = NameStr(constraintForm->conname);

		if (strncmp(constraintName, searchedConstraintName, NAMEDATALEN) == 0)
		{
			/* we found the constraint, now check if it is for an index */
			if (constraintForm->contype == CONSTRAINT_PRIMARY ||
				constraintForm->contype == CONSTRAINT_UNIQUE)
			{
				indexConstraint = true;
			}

			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	pfree(searchedConstraintName);

	return indexConstraint;
}


/*
 * AppendShardIdToConstraintName extends given constraint name with given
 * shardId. Note that we only extend constraint names if they correspond to
 * indexes, and the caller should verify that index correspondence before
 * calling this function.
 */
static void
AppendShardIdToConstraintName(AlterTableCmd *command, uint64 shardId)
{
	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		char **constraintName = &(constraint->conname);
		AppendShardIdToName(constraintName, shardId);
	}
	else if (command->subtype == AT_DropConstraint)
	{
		char **constraintName = &(command->name);
		AppendShardIdToName(constraintName, shardId);
	}
}


/*
 * SetSchemaNameIfNotExist function checks whether schemaName is set and if it is not set
 * it sets its value to given newSchemaName.
 */
static void
SetSchemaNameIfNotExist(char **schemaName, char *newSchemaName)
{
	if ((*schemaName) == NULL)
	{
		*schemaName = pstrdup(newSchemaName);
	}
}


/*
 * AppendShardIdToName appends shardId to the given name. The function takes in
 * the name's address in order to reallocate memory for the name in the same
 * memory context the name was originally created in.
 */
void
AppendShardIdToName(char **name, uint64 shardId)
{
	char extendedName[NAMEDATALEN];
	uint32 extendedNameLength = 0;

	snprintf(extendedName, NAMEDATALEN, "%s%c" UINT64_FORMAT,
			 (*name), SHARD_NAME_SEPARATOR, shardId);

	/*
	 * Parser should have already checked that the table name has enough space
	 * reserved for appending shardIds. Nonetheless, we perform an additional
	 * check here to verify that the appended name does not overflow.
	 */
	extendedNameLength = strlen(extendedName) + 1;
	if (extendedNameLength >= NAMEDATALEN)
	{
		ereport(ERROR, (errmsg("shard name too long to extend: \"%s\"", (*name))));
	}

	(*name) = (char *) repalloc((*name), extendedNameLength);
	snprintf((*name), extendedNameLength, "%s", extendedName);
}


/*
 * AppendShardIdToStringInfo appends shardId to the given name, represented
 * by a StringInfo.
 */
void
AppendShardIdToStringInfo(StringInfo name, uint64 shardId)
{
	appendStringInfo(name, "%c" UINT64_FORMAT, SHARD_NAME_SEPARATOR, shardId);
}
