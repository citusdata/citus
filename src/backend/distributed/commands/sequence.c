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
#include "distributed/commands.h"
#include "distributed/commands/sequence.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "nodes/parsenodes.h"

/* Local functions forward declarations for helper functions */
static bool OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId);


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
		else if (!hasDistributedOwner && IsCitusTable(newOwnedByTableId))
		{
			/* and don't let local sequences get a distributed OWNED BY */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot associate an existing sequence with a "
								   "distributed table"),
							errhint("Use a sequence in a distributed table by specifying "
									"a serial column type before creating any shards.")));
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
