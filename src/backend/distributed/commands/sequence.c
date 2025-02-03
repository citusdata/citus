/*-------------------------------------------------------------------------
 *
 * sequence.c
 *     This file contains implementation of CREATE and ALTER SEQUENCE
 *     statement functions to run in a distributed setting
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_attrdef.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/commands/sequence.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_create_or_replace.h"

/* Local functions forward declarations for helper functions */
static bool OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId);
static Oid SequenceUsedInDistributedTable(const ObjectAddress *sequenceAddress, char
										  depType);
static List * FilterDistributedSequences(GrantStmt *stmt);


/*
 * ErrorIfUnsupportedSeqStmt errors out if the provided create sequence
 * statement specifies a distributed table in its OWNED BY clause.
 */
void
ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt)
{
	Oid ownedByTableId = InvalidOid;

	/* create is easy: just prohibit any distributed OWNED BY */
	if (OptionsSpecifyOwnedBy(createSeqStmt->options, &ownedByTableId))
	{
		if (IsCitusTable(ownedByTableId))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create sequences that specify a distributed "
								   "table in their OWNED BY option"),
							errhint("Use a sequence in a distributed table by specifying "
									"a serial column type before creating any shards.")));
		}
	}
}


/*
 * ErrorIfDistributedAlterSeqOwnedBy errors out if the provided alter sequence
 * statement attempts to change the owned by property of a distributed sequence
 * or attempt to change a local sequence to be owned by a distributed table.
 */
void
ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt)
{
	Oid sequenceId = RangeVarGetRelid(alterSeqStmt->sequence, AccessShareLock,
									  alterSeqStmt->missing_ok);
	Oid ownedByTableId = InvalidOid;
	Oid newOwnedByTableId = InvalidOid;
	int32 ownedByColumnId = 0;
	bool hasDistributedOwner = false;

	/* alter statement referenced nonexistent sequence; return */
	if (sequenceId == InvalidOid)
	{
		return;
	}

	bool sequenceOwned = sequenceIsOwned(sequenceId, DEPENDENCY_AUTO, &ownedByTableId,
										 &ownedByColumnId);
	if (!sequenceOwned)
	{
		sequenceOwned = sequenceIsOwned(sequenceId, DEPENDENCY_INTERNAL, &ownedByTableId,
										&ownedByColumnId);
	}

	/* see whether the sequence is already owned by a distributed table */
	if (sequenceOwned)
	{
		hasDistributedOwner = IsCitusTable(ownedByTableId);
	}

	if (OptionsSpecifyOwnedBy(alterSeqStmt->options, &newOwnedByTableId))
	{
		/* if a distributed sequence tries to change owner, error */
		if (hasDistributedOwner && ownedByTableId != newOwnedByTableId)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot alter OWNED BY option of a sequence "
								   "already owned by a distributed table")));
		}
	}
}


/*
 * OptionsSpecifyOwnedBy processes the options list of either a CREATE or ALTER
 * SEQUENCE command, extracting the first OWNED BY option it encounters. The
 * identifier for the specified table is placed in the Oid out parameter before
 * returning true. Returns false if no such option is found. Still returns true
 * for OWNED BY NONE, but leaves the out paramter set to InvalidOid.
 */
static bool
OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId)
{
	DefElem *defElem = NULL;
	foreach_declared_ptr(defElem, optionList)
	{
		if (strcmp(defElem->defname, "owned_by") == 0)
		{
			List *ownedByNames = defGetQualifiedName(defElem);
			int nameCount = list_length(ownedByNames);

			/* if only one name is present, this is OWNED BY NONE */
			if (nameCount == 1)
			{
				*ownedByTableId = InvalidOid;
				return true;
			}
			else
			{
				/*
				 * Otherwise, we have a list of schema, table, column, which we
				 * need to truncate to simply the schema and table to determine
				 * the relevant relation identifier.
				 */
				List *relNameList = list_truncate(list_copy(ownedByNames), nameCount - 1);
				RangeVar *rangeVar = makeRangeVarFromNameList(relNameList);
				bool failOK = true;

				*ownedByTableId = RangeVarGetRelid(rangeVar, NoLock, failOK);
				return true;
			}
		}
	}

	return false;
}


/*
 * ExtractDefaultColumnsAndOwnedSequences finds each column of relation with
 * relationId that has a DEFAULT expression and each sequence owned by such
 * columns (if any). Then, appends the column name and id of the owned sequence
 * -that the column defaults- to the lists passed as NIL initially.
 */
void
ExtractDefaultColumnsAndOwnedSequences(Oid relationId, List **columnNameList,
									   List **ownedSequenceIdList)
{
	Assert(*columnNameList == NIL && *ownedSequenceIdList == NIL);

	Relation relation = relation_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attisdropped ||
			attributeForm->attgenerated == ATTRIBUTE_GENERATED_STORED)
		{
			/* skip dropped columns and columns with GENERATED AS ALWAYS expressions */
			continue;
		}

		char *columnName = NameStr(attributeForm->attname);
		List *columnOwnedSequences =
			getOwnedSequences_internal(relationId, attributeIndex + 1, DEPENDENCY_AUTO);

		if (attributeForm->atthasdef && list_length(columnOwnedSequences) == 0)
		{
			/*
			 * Even if there are no owned sequences, the code path still
			 * expects the columnName to be filled such that it can DROP
			 * DEFAULT for the existing nextval('seq') columns.
			 */
			*ownedSequenceIdList = lappend_oid(*ownedSequenceIdList, InvalidOid);
			*columnNameList = lappend(*columnNameList, columnName);

			continue;
		}

		Oid ownedSequenceId = InvalidOid;
		foreach_declared_oid(ownedSequenceId, columnOwnedSequences)
		{
			/*
			 * A column might have multiple sequences one via OWNED BY one another
			 * via bigserial/default nextval.
			 */
			*ownedSequenceIdList = lappend_oid(*ownedSequenceIdList, ownedSequenceId);
			*columnNameList = lappend(*columnNameList, columnName);
		}
	}

	relation_close(relation, NoLock);
}


/*
 * ColumnDefaultsToNextVal returns true if the column with attrNumber
 * has a default expression that contains nextval().
 */
bool
ColumnDefaultsToNextVal(Oid relationId, AttrNumber attrNumber)
{
	Assert(AttributeNumberIsValid(attrNumber));

	Relation relation = RelationIdGetRelation(relationId);
	Node *defExpr = build_column_default(relation, attrNumber);
	RelationClose(relation);

	if (defExpr == NULL)
	{
		/* column doesn't have a DEFAULT expression */
		return false;
	}

	return contain_nextval_expression_walker(defExpr, NULL);
}


/*
 * PreprocessDropSequenceStmt gets called during the planning phase of a DROP SEQUENCE statement
 * and returns a list of DDLJob's that will drop any distributed sequences from the
 * workers.
 *
 * The DropStmt could have multiple objects to drop, the list of objects will be filtered
 * to only keep the distributed sequences for deletion on the workers. Non-distributed
 * sequences will still be dropped locally but not on the workers.
 */
List *
PreprocessDropSequenceStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);
	List *distributedSequencesList = NIL;
	List *distributedSequenceAddresses = NIL;

	Assert(stmt->removeType == OBJECT_SEQUENCE);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, sequences cascading
		 * from an extension should therefore not be propagated here.
		 */
		return NIL;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return NIL;
	}

	/*
	 * Our statements need to be fully qualified so we can drop them from the right schema
	 * on the workers
	 */
	QualifyTreeNode((Node *) stmt);

	/*
	 * iterate over all sequences to be dropped and filter to keep only distributed
	 * sequences.
	 */
	List *deletingSequencesList = stmt->objects;
	List *objectNameList = NULL;
	foreach_declared_ptr(objectNameList, deletingSequencesList)
	{
		RangeVar *seq = makeRangeVarFromNameList(objectNameList);

		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);
		if (!IsAnyObjectDistributed(list_make1(sequenceAddress)))
		{
			continue;
		}

		/* collect information for all distributed sequences */
		distributedSequenceAddresses = lappend(distributedSequenceAddresses,
											   sequenceAddress);
		distributedSequencesList = lappend(distributedSequencesList, objectNameList);
	}

	if (list_length(distributedSequencesList) <= 0)
	{
		/* no distributed functions to drop */
		return NIL;
	}

	/*
	 * managing sequences can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * sequences, so we block the call.
	 */
	EnsureCoordinator();

	/* remove the entries for the distributed objects on dropping */
	ObjectAddress *address = NULL;
	foreach_declared_ptr(address, distributedSequenceAddresses)
	{
		UnmarkObjectDistributed(address);
	}

	/*
	 * Swap the list of objects before deparsing and restore the old list after. This
	 * ensures we only have distributed sequences in the deparsed drop statement.
	 */
	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedSequencesList;
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * SequenceDropStmtObjectAddress returns list of object addresses in the drop sequence
 * statement.
 */
List *
SequenceDropStmtObjectAddress(Node *stmt, bool missing_ok, bool isPostprocess)
{
	DropStmt *dropSeqStmt = castNode(DropStmt, stmt);

	List *objectAddresses = NIL;

	List *droppingSequencesList = dropSeqStmt->objects;
	List *objectNameList = NULL;
	foreach_declared_ptr(objectNameList, droppingSequencesList)
	{
		RangeVar *seq = makeRangeVarFromNameList(objectNameList);

		Oid seqOid = RangeVarGetRelid(seq, AccessShareLock, missing_ok);

		ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*objectAddress, SequenceRelationId, seqOid);
		objectAddresses = lappend(objectAddresses, objectAddress);
	}

	return objectAddresses;
}


/*
 * PreprocessRenameSequenceStmt is called when the user is renaming a sequence. The invocation
 * happens before the statement is applied locally.
 *
 * As the sequence already exists we have access to the ObjectAddress, this is used to
 * check if it is distributed. If so the rename is executed on all the workers to keep the
 * types in sync across the cluster.
 */
List *
PreprocessRenameSequenceStmt(Node *node, const char *queryString, ProcessUtilityContext
							 processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SEQUENCE);

	List *addresses = GetObjectAddressListFromParseTree((Node *) stmt,
														stmt->missing_ok, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	EnsureCoordinator();
	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION, (void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * RenameSequenceStmtObjectAddress returns the ObjectAddress of the sequence that is the
 * subject of the RenameStmt.
 */
List *
RenameSequenceStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, missing_ok);
	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);

	return list_make1(sequenceAddress);
}


/*
 * PreprocessAlterSequenceStmt gets called during the planning phase of an ALTER SEQUENCE statement
 * of one of the following forms:
 * ALTER SEQUENCE [ IF EXISTS ] name
 *  [ AS data_type ]
 *  [ INCREMENT [ BY ] increment ]
 *  [ MINVALUE minvalue | NO MINVALUE ] [ MAXVALUE maxvalue | NO MAXVALUE ]
 *  [ START [ WITH ] start ]
 *  [ RESTART [ [ WITH ] restart ] ]
 *  [ CACHE cache ] [ [ NO ] CYCLE ]
 *  [ OWNED BY { table_name.column_name | NONE } ]
 *
 * For distributed sequences, this operation will not be allowed for now.
 * The reason is that we change sequence parameters when distributing it, so we don't want to
 * touch those parameters for now.
 */
List *
PreprocessAlterSequenceStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	AlterSeqStmt *stmt = castNode(AlterSeqStmt, node);

	List *addresses = GetObjectAddressListFromParseTree((Node *) stmt,
														stmt->missing_ok, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	/* We have already asserted that we have exactly 1 address in the addresses. */
	ObjectAddress *address = linitial(addresses);

	/* error out if the sequence is distributed */
	if (IsAnyObjectDistributed(addresses) || SequenceUsedInDistributedTable(address,
																			DEPENDENCY_INTERNAL))
	{
		ereport(ERROR, (errmsg(
							"Altering a distributed sequence is currently not supported.")));
	}

	/*
	 * error out if the sequence is used in a distributed table
	 * and this is an ALTER SEQUENCE .. AS .. statement
	 */
	Oid citusTableId = SequenceUsedInDistributedTable(address, DEPENDENCY_AUTO);
	if (citusTableId != InvalidOid)
	{
		List *options = stmt->options;
		DefElem *defel = NULL;
		foreach_declared_ptr(defel, options)
		{
			if (strcmp(defel->defname, "as") == 0)
			{
				if (IsCitusTableType(citusTableId, CITUS_LOCAL_TABLE))
				{
					ereport(ERROR, (errmsg(
										"Altering a sequence used in a local table that"
										" is added to metadata is currently not supported.")));
				}
				ereport(ERROR, (errmsg(
									"Altering a sequence used in a distributed"
									" table is currently not supported.")));
			}
		}
	}

	return NIL;
}


/*
 * SequenceUsedInDistributedTable returns true if the argument sequence
 * is used as the default value of a column in a distributed table.
 * Returns false otherwise
 * See DependencyType for the possible values of depType.
 * We use DEPENDENCY_INTERNAL for sequences created by identity column.
 * DEPENDENCY_AUTO for regular sequences.
 */
static Oid
SequenceUsedInDistributedTable(const ObjectAddress *sequenceAddress, char depType)
{
	Oid relationId;
	List *relations = GetDependentRelationsWithSequence(sequenceAddress->objectId,
														depType);
	foreach_declared_oid(relationId, relations)
	{
		if (IsCitusTable(relationId))
		{
			return relationId;
		}
	}

	return InvalidOid;
}


/*
 * AlterSequenceStmtObjectAddress returns the ObjectAddress of the sequence that is the
 * subject of the AlterSeqStmt.
 */
List *
AlterSequenceStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterSeqStmt *stmt = castNode(AlterSeqStmt, node);

	RangeVar *sequence = stmt->sequence;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, stmt->missing_ok);
	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);

	return list_make1(sequenceAddress);
}


/*
 * PreprocessAlterSequenceSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers.
 */
List *
PreprocessAlterSequenceSchemaStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);

	List *addresses = GetObjectAddressListFromParseTree((Node *) stmt,
														stmt->missing_ok, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	EnsureCoordinator();
	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION, (void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * AlterSequenceSchemaStmtObjectAddress returns the ObjectAddress of the sequence that is
 * the subject of the AlterObjectSchemaStmt.
 */
List *
AlterSequenceSchemaStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, true);

	if (seqOid == InvalidOid)
	{
		/*
		 * couldn't find the sequence, might have already been moved to the new schema, we
		 * construct a new sequence name that uses the new schema to search in.
		 */
		const char *newSchemaName = stmt->newschema;
		Oid newSchemaOid = get_namespace_oid(newSchemaName, true);
		seqOid = get_relname_relid(sequence->relname, newSchemaOid);

		if (!missing_ok && seqOid == InvalidOid)
		{
			/*
			 * if the sequence is still invalid we couldn't find the sequence, error with the same
			 * message postgres would error with if missing_ok is false (not ok to miss)
			 */
			const char *quotedSequenceName =
				quote_qualified_identifier(sequence->schemaname, sequence->relname);

			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
							errmsg("relation \"%s\" does not exist",
								   quotedSequenceName)));
		}
	}

	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);

	return list_make1(sequenceAddress);
}


/*
 * PostprocessAlterSequenceSchemaStmt is executed after the change has been applied locally,
 * we can now use the new dependencies of the sequence to ensure all its dependencies
 * exist on the workers before we apply the commands remotely.
 */
List *
PostprocessAlterSequenceSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);
	List *addresses = GetObjectAddressListFromParseTree((Node *) stmt,
														stmt->missing_ok, true);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureAllObjectDependenciesExistOnAllNodes(addresses);

	return NIL;
}


/*
 * PreprocessAlterSequenceOwnerStmt is called for change of ownership of sequences before the
 * ownership is changed on the local instance.
 *
 * If the sequence for which the owner is changed is distributed we execute the change on
 * all the workers to keep the type in sync across the cluster.
 */
List *
PreprocessAlterSequenceOwnerStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	List *sequenceAddresses = GetObjectAddressListFromParseTree((Node *) stmt, false,
																false);

	/*  the code-path only supports a single object */
	Assert(list_length(sequenceAddresses) == 1);

	if (!ShouldPropagateAnyObject(sequenceAddresses))
	{
		return NIL;
	}

	EnsureCoordinator();
	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION, (void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * AlterSequenceOwnerStmtObjectAddress returns the ObjectAddress of the sequence that is the
 * subject of the AlterOwnerStmt.
 */
List *
AlterSequenceOwnerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, missing_ok);
	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);

	return list_make1(sequenceAddress);
}


/*
 * PostprocessAlterSequenceOwnerStmt is executed after the change has been applied locally,
 * we can now use the new dependencies of the sequence to ensure all its dependencies
 * exist on the workers before we apply the commands remotely.
 */
List *
PostprocessAlterSequenceOwnerStmt(Node *node, const char *queryString)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	List *sequenceAddresses = GetObjectAddressListFromParseTree((Node *) stmt, false,
																true);

	/*  the code-path only supports a single object */
	Assert(list_length(sequenceAddresses) == 1);

	if (!ShouldPropagateAnyObject(sequenceAddresses))
	{
		return NIL;
	}

	/* dependencies have changed (owner) let's ensure they exist */
	EnsureAllObjectDependenciesExistOnAllNodes(sequenceAddresses);

	return NIL;
}


/*
 * PreprocessAlterSequencePersistenceStmt is called for change of persistence
 * of sequences before the persistence is changed on the local instance.
 *
 * If the sequence for which the persistence is changed is distributed, we execute
 * the change on all the workers to keep the type in sync across the cluster.
 */
List *
PreprocessAlterSequencePersistenceStmt(Node *node, const char *queryString,
									   ProcessUtilityContext processUtilityContext)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	List *sequenceAddresses = GetObjectAddressListFromParseTree((Node *) stmt, false,
																false);

	/*  the code-path only supports a single object */
	Assert(list_length(sequenceAddresses) == 1);

	if (!ShouldPropagateAnyObject(sequenceAddresses))
	{
		return NIL;
	}

	EnsureCoordinator();
	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION, (void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * AlterSequencePersistenceStmtObjectAddress returns the ObjectAddress of the
 * sequence that is the subject of the AlterPersistenceStmt.
 */
List *
AlterSequencePersistenceStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, missing_ok);
	ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*sequenceAddress, RelationRelationId, seqOid);

	return list_make1(sequenceAddress);
}


/*
 * PreprocessSequenceAlterTableStmt is called for change of persistence or owner
 * of sequences before the persistence/owner is changed on the local instance.
 *
 * Altering persistence or owner are the only ALTER commands of a sequence
 * that may pass through an AlterTableStmt as well
 */
List *
PreprocessSequenceAlterTableStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	ListCell *cmdCell = NULL;
	foreach(cmdCell, stmt->cmds)
	{
		AlterTableCmd *cmd = castNode(AlterTableCmd, lfirst(cmdCell));
		switch (cmd->subtype)
		{
			case AT_ChangeOwner:
			{
				return PreprocessAlterSequenceOwnerStmt(node,
														queryString,
														processUtilityContext);
			}

			case AT_SetLogged:
			{
				return PreprocessAlterSequencePersistenceStmt(node,
															  queryString,
															  processUtilityContext);
			}

			case AT_SetUnLogged:
			{
				return PreprocessAlterSequencePersistenceStmt(node,
															  queryString,
															  processUtilityContext);
			}

			default:
			{
				/* normally we shouldn't ever reach this */
				ereport(ERROR, (errmsg("unsupported subtype for alter sequence command"),
								errdetail("sub command type: %d",
										  cmd->subtype)));
			}
		}
	}
	return NIL;
}


/*
 * PreprocessGrantOnSequenceStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on distributed sequences.
 */
List *
PreprocessGrantOnSequenceStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, sequences cascading
		 * from an extension should therefore not be propagated here.
		 */
		return NIL;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return NIL;
	}

	List *distributedSequences = FilterDistributedSequences(stmt);

	if (list_length(distributedSequences) == 0)
	{
		return NIL;
	}

	EnsureCoordinator();

	GrantStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedSequences;

	/*
	 * if the original command was targeting schemas, we have expanded to the distributed
	 * sequences in these schemas through FilterDistributedSequences.
	 */
	stmtCopy->targtype = ACL_TARGET_OBJECT;

	QualifyTreeNode((Node *) stmtCopy);

	char *sql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION, (void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_METADATA_NODES, commands);
}


/*
 * PostprocessGrantOnSequenceStmt makes sure dependencies of each
 * distributed sequence in the statement exist on all nodes
 */
List *
PostprocessGrantOnSequenceStmt(Node *node, const char *queryString)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	List *distributedSequences = FilterDistributedSequences(stmt);

	if (list_length(distributedSequences) == 0)
	{
		return NIL;
	}

	EnsureCoordinator();

	RangeVar *sequence = NULL;
	foreach_declared_ptr(sequence, distributedSequences)
	{
		ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
		Oid sequenceOid = RangeVarGetRelid(sequence, NoLock, false);
		ObjectAddressSet(*sequenceAddress, RelationRelationId, sequenceOid);
		EnsureAllObjectDependenciesExistOnAllNodes(list_make1(sequenceAddress));
	}
	return NIL;
}


/*
 * GenerateBackupNameForSequenceCollision generates a new sequence name for an existing
 * sequence. The name is generated in such a way that the new name doesn't overlap with
 * an existing relation by adding a suffix with incrementing number after the new name.
 */
char *
GenerateBackupNameForSequenceCollision(const ObjectAddress *address)
{
	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	int count = 0;
	char *namespaceName = get_namespace_name(get_rel_namespace(address->objectId));
	Oid schemaId = get_namespace_oid(namespaceName, false);

	char *baseName = get_rel_name(address->objectId);
	int baseLength = strlen(baseName);

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

		Oid newRelationId = get_relname_relid(newName, schemaId);
		if (newRelationId == InvalidOid)
		{
			return newName;
		}

		count++;
	}
}


/*
 * FilterDistributedSequences determines and returns a list of distributed sequences
 * RangeVar-s from given grant statement.
 * - If the stmt's targtype is ACL_TARGET_OBJECT, i.e. of the form GRANT ON SEQUENCE ...
 *   it returns the distributed sequences in the list of sequences in the statement
 * - If targtype is ACL_TARGET_ALL_IN_SCHEMA, i.e. GRANT ON ALL SEQUENCES IN SCHEMA ...
 *   it expands the ALL IN SCHEMA to the actual sequences, and returns the distributed
 *   sequences from those.
 */
static List *
FilterDistributedSequences(GrantStmt *stmt)
{
	bool grantOnSequenceCommand = (stmt->targtype == ACL_TARGET_OBJECT &&
								   stmt->objtype == OBJECT_SEQUENCE);
	bool grantOnAllSequencesInSchemaCommand = (stmt->targtype ==
											   ACL_TARGET_ALL_IN_SCHEMA &&
											   stmt->objtype == OBJECT_SEQUENCE);

	/* we are only interested in sequence level grants */
	if (!grantOnSequenceCommand && !grantOnAllSequencesInSchemaCommand)
	{
		return NIL;
	}

	List *grantSequenceList = NIL;

	if (grantOnAllSequencesInSchemaCommand)
	{
		/* iterate over all namespace names provided to get their oid's */
		List *namespaceOidList = NIL;
		String *namespaceValue = NULL;
		foreach_declared_ptr(namespaceValue, stmt->objects)
		{
			char *nspname = strVal(namespaceValue);
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		/*
		 * iterate over all distributed sequences to filter the ones
		 * that belong to one of the namespaces from above
		 */
		List *distributedSequenceList = DistributedSequenceList();
		ObjectAddress *sequenceAddress = NULL;
		foreach_declared_ptr(sequenceAddress, distributedSequenceList)
		{
			Oid namespaceOid = get_rel_namespace(sequenceAddress->objectId);

			/*
			 * if this distributed sequence's schema is one of the schemas
			 * specified in the GRANT .. ALL SEQUENCES IN SCHEMA ..
			 * add it to the list
			 */
			if (list_member_oid(namespaceOidList, namespaceOid))
			{
				RangeVar *distributedSequence = makeRangeVar(get_namespace_name(
																 namespaceOid),
															 get_rel_name(
																 sequenceAddress->objectId),
															 -1);
				grantSequenceList = lappend(grantSequenceList, distributedSequence);
			}
		}
	}
	else
	{
		bool missing_ok = false;
		RangeVar *sequenceRangeVar = NULL;
		foreach_declared_ptr(sequenceRangeVar, stmt->objects)
		{
			Oid sequenceOid = RangeVarGetRelid(sequenceRangeVar, NoLock, missing_ok);
			ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
			ObjectAddressSet(*sequenceAddress, RelationRelationId, sequenceOid);

			/*
			 * if this sequence from GRANT .. ON SEQUENCE .. is a distributed
			 * sequence, add it to the list
			 */
			if (IsAnyObjectDistributed(list_make1(sequenceAddress)))
			{
				grantSequenceList = lappend(grantSequenceList, sequenceRangeVar);
			}
		}
	}

	return grantSequenceList;
}


/*
 * RenameExistingSequenceWithDifferentTypeIfExists renames the sequence's type if
 * that sequence exists and the desired sequence type is different than its type.
 */
void
RenameExistingSequenceWithDifferentTypeIfExists(RangeVar *sequence, Oid desiredSeqTypeId)
{
	Oid sequenceOid;
	RangeVarGetAndCheckCreationNamespace(sequence, NoLock, &sequenceOid);

	if (OidIsValid(sequenceOid))
	{
		Form_pg_sequence pgSequenceForm = pg_get_sequencedef(sequenceOid);
		if (pgSequenceForm->seqtypid != desiredSeqTypeId)
		{
			ObjectAddress sequenceAddress = { 0 };
			ObjectAddressSet(sequenceAddress, RelationRelationId, sequenceOid);

			char *newName = GenerateBackupNameForCollision(&sequenceAddress);

			RenameStmt *renameStmt = CreateRenameStatement(&sequenceAddress, newName);
			const char *sqlRenameStmt = DeparseTreeNode((Node *) renameStmt);
			ProcessUtilityParseTree((Node *) renameStmt, sqlRenameStmt,
									PROCESS_UTILITY_QUERY,
									NULL, None_Receiver, NULL);

			CommandCounterIncrement();
		}
	}
}
