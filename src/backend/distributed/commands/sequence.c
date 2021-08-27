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

#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/commands/sequence.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* Local functions forward declarations for helper functions */
static bool OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId);
static Oid SequenceUsedInDistributedTable(const ObjectAddress *sequenceAddress);


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
	foreach_ptr(defElem, optionList)
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
		if (attributeForm->attisdropped || !attributeForm->atthasdef)
		{
			/*
			 * If this column has already been dropped or it has no DEFAULT
			 * definition, skip it.
			 */
			continue;
		}

		if (attributeForm->attgenerated == ATTRIBUTE_GENERATED_STORED)
		{
			/* skip columns with GENERATED AS ALWAYS expressions */
			continue;
		}

		char *columnName = NameStr(attributeForm->attname);
		*columnNameList = lappend(*columnNameList, columnName);

		List *columnOwnedSequences =
			GetSequencesOwnedByColumn(relationId, attributeIndex + 1);

		Oid ownedSequenceId = InvalidOid;
		if (list_length(columnOwnedSequences) != 0)
		{
			/*
			 * A column might only own one sequence. We intentionally use
			 * GetSequencesOwnedByColumn macro and pick initial oid from the
			 * list instead of using getOwnedSequence. This is both because
			 * getOwnedSequence is removed in pg13 and is also because it
			 * errors out if column does not have any sequences.
			 */
			Assert(list_length(columnOwnedSequences) == 1);
			ownedSequenceId = linitial_oid(columnOwnedSequences);
		}

		*ownedSequenceIdList = lappend_oid(*ownedSequenceIdList, ownedSequenceId);
	}

	relation_close(relation, NoLock);
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
	List *deletingSequencesList = stmt->objects;
	List *distributedSequencesList = NIL;
	List *distributedSequenceAddresses = NIL;

	Assert(stmt->removeType == OBJECT_SEQUENCE);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, sequences cascading
		 * from an extension should therefor not be propagated here.
		 */
		return NIL;
	}

	if (!EnableDependencyCreation)
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
	List *objectNameList = NULL;
	foreach_ptr(objectNameList, deletingSequencesList)
	{
		RangeVar *seq = makeRangeVarFromNameList(objectNameList);

		Oid seqOid = RangeVarGetRelid(seq, NoLock, stmt->missing_ok);

		ObjectAddress sequenceAddress = { 0 };
		ObjectAddressSet(sequenceAddress, RelationRelationId, seqOid);

		if (!IsObjectDistributed(&sequenceAddress))
		{
			continue;
		}

		/* collect information for all distributed sequences */
		ObjectAddress *addressp = palloc(sizeof(ObjectAddress));
		*addressp = sequenceAddress;
		distributedSequenceAddresses = lappend(distributedSequenceAddresses, addressp);
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
	foreach_ptr(address, distributedSequenceAddresses)
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

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);

	if (!ShouldPropagateObject(&address))
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
ObjectAddress
RenameSequenceStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, missing_ok);
	ObjectAddress sequenceAddress = { 0 };
	ObjectAddressSet(sequenceAddress, RelationRelationId, seqOid);

	return sequenceAddress;
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

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);

	/* error out if the sequence is distributed */
	if (IsObjectDistributed(&address))
	{
		ereport(ERROR, (errmsg(
							"Altering a distributed sequence is currently not supported.")));
	}

	/*
	 * error out if the sequence is used in a distributed table
	 * and this is an ALTER SEQUENCE .. AS .. statement
	 */
	Oid citusTableId = SequenceUsedInDistributedTable(&address);
	if (citusTableId != InvalidOid)
	{
		List *options = stmt->options;
		DefElem *defel = NULL;
		foreach_ptr(defel, options)
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
 */
static Oid
SequenceUsedInDistributedTable(const ObjectAddress *sequenceAddress)
{
	List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
	Oid citusTableId = InvalidOid;
	foreach_oid(citusTableId, citusTableIdList)
	{
		List *attnumList = NIL;
		List *dependentSequenceList = NIL;
		GetDependentSequencesWithRelation(citusTableId, &attnumList,
										  &dependentSequenceList, 0);
		Oid currentSeqOid = InvalidOid;
		foreach_oid(currentSeqOid, dependentSequenceList)
		{
			/*
			 * This sequence is used in a distributed table
			 */
			if (currentSeqOid == sequenceAddress->objectId)
			{
				return citusTableId;
			}
		}
	}
	return InvalidOid;
}


/*
 * AlterSequenceStmtObjectAddress returns the ObjectAddress of the sequence that is the
 * subject of the AlterSeqStmt.
 */
ObjectAddress
AlterSequenceStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterSeqStmt *stmt = castNode(AlterSeqStmt, node);

	RangeVar *sequence = stmt->sequence;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, stmt->missing_ok);
	ObjectAddress sequenceAddress = { 0 };
	ObjectAddressSet(sequenceAddress, RelationRelationId, seqOid);

	return sequenceAddress;
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

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);
	if (!ShouldPropagateObject(&address))
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
ObjectAddress
AlterSequenceSchemaStmtObjectAddress(Node *node, bool missing_ok)
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

	ObjectAddress sequenceAddress = { 0 };
	ObjectAddressSet(sequenceAddress, RelationRelationId, seqOid);

	return sequenceAddress;
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
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_SEQUENCE);

	ObjectAddress sequenceAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&sequenceAddress))
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
ObjectAddress
AlterSequenceOwnerStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_SEQUENCE);

	RangeVar *sequence = stmt->relation;
	Oid seqOid = RangeVarGetRelid(sequence, NoLock, missing_ok);
	ObjectAddress sequenceAddress = { 0 };
	ObjectAddressSet(sequenceAddress, RelationRelationId, seqOid);

	return sequenceAddress;
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
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_SEQUENCE);

	ObjectAddress sequenceAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&sequenceAddress))
	{
		return NIL;
	}

	/* dependencies have changed (owner) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&sequenceAddress);

	return NIL;
}
