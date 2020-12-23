/*-------------------------------------------------------------------------
 *
 * cstore_reader.c
 *
 * This file contains function definitions for reading cstore files. This
 * includes the logic for reading file level metadata, reading row stripes,
 * and skipping unrelated row chunks and columns.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#else
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#endif
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "columnar/cstore.h"
#include "columnar/cstore_version_compat.h"

/* static function declarations */
static StripeBuffers * LoadFilteredStripeBuffers(Relation relation,
												 StripeMetadata *stripeMetadata,
												 TupleDesc tupleDescriptor,
												 List *projectedColumnList,
												 List *whereClauseList);
static void ReadStripeNextRow(StripeBuffers *stripeBuffers, List *projectedColumnList,
							  uint64 chunkIndex, uint64 chunkRowIndex,
							  ChunkData *chunkData, Datum *columnValues,
							  bool *columnNulls);
static ColumnBuffers * LoadColumnBuffers(Relation relation,
										 ColumnChunkSkipNode *chunkSkipNodeArray,
										 uint32 chunkCount, uint64 stripeOffset,
										 Form_pg_attribute attributeForm);
static bool * SelectedChunkMask(StripeSkipList *stripeSkipList,
								List *projectedColumnList, List *whereClauseList);
static List * BuildRestrictInfoList(List *whereClauseList);
static Node * BuildBaseConstraint(Var *variable);
static OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue);
static StripeSkipList * SelectedChunkSkipList(StripeSkipList *stripeSkipList,
											  bool *projectedColumnMask,
											  bool *selectedChunkMask);
static uint32 StripeSkipListRowCount(StripeSkipList *stripeSkipList);
static bool * ProjectedColumnMask(uint32 columnCount, List *projectedColumnList);
static void DeserializeBoolArray(StringInfo boolArrayBuffer, bool *boolArray,
								 uint32 boolArrayLength);
static void DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray,
								  uint32 datumCount, bool datumTypeByValue,
								  int datumTypeLength, char datumTypeAlign,
								  Datum *datumArray);
static ChunkData * DeserializeChunkData(StripeBuffers *stripeBuffers, uint64 chunkIndex,
										uint32 rowCount, TupleDesc tupleDescriptor,
										List *projectedColumnList);
static Datum ColumnDefaultValue(TupleConstr *tupleConstraints,
								Form_pg_attribute attributeForm);

/*
 * CStoreBeginRead initializes a cstore read operation. This function returns a
 * read handle that's used during reading rows and finishing the read operation.
 */
TableReadState *
CStoreBeginRead(Relation relation, TupleDesc tupleDescriptor,
				List *projectedColumnList, List *whereClauseList)
{
	List *stripeList = StripesForRelfilenode(relation->rd_node);

	/*
	 * We allocate all stripe specific data in the stripeReadContext, and reset
	 * this memory context before loading a new stripe. This is to avoid memory
	 * leaks.
	 */
	MemoryContext stripeReadContext = AllocSetContextCreate(CurrentMemoryContext,
															"Stripe Read Memory Context",
															ALLOCSET_DEFAULT_SIZES);

	TableReadState *readState = palloc0(sizeof(TableReadState));
	readState->relation = relation;
	readState->stripeList = stripeList;
	readState->projectedColumnList = projectedColumnList;
	readState->whereClauseList = whereClauseList;
	readState->stripeBuffers = NULL;
	readState->readStripeCount = 0;
	readState->stripeReadRowCount = 0;
	readState->tupleDescriptor = tupleDescriptor;
	readState->stripeReadContext = stripeReadContext;
	readState->chunkData = NULL;
	readState->deserializedChunkIndex = -1;

	return readState;
}


/*
 * CStoreReadNextRow tries to read a row from the cstore file. On success, it sets
 * column values and nulls, and returns true. If there are no more rows to read,
 * the function returns false.
 */
bool
CStoreReadNextRow(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
	StripeMetadata *stripeMetadata = readState->currentStripeMetadata;
	MemoryContext oldContext = NULL;

	/*
	 * If no stripes are loaded, load the next non-empty stripe. Note that when
	 * loading stripes, we skip over chunks whose contents can be filtered with
	 * the query's restriction qualifiers. So, even when a stripe is physically
	 * not empty, we may end up loading it as an empty stripe.
	 */
	while (readState->stripeBuffers == NULL)
	{
		List *stripeMetadataList = readState->stripeList;
		uint32 stripeCount = list_length(stripeMetadataList);

		/* if we have read all stripes, return false */
		if (readState->readStripeCount == stripeCount)
		{
			return false;
		}

		oldContext = MemoryContextSwitchTo(readState->stripeReadContext);
		MemoryContextReset(readState->stripeReadContext);
		readState->chunkData = NULL;

		stripeMetadata = list_nth(stripeMetadataList, readState->readStripeCount);
		StripeBuffers *stripeBuffers = LoadFilteredStripeBuffers(readState->relation,
																 stripeMetadata,
																 readState->
																 tupleDescriptor,
																 readState->
																 projectedColumnList,
																 readState->
																 whereClauseList);
		readState->readStripeCount++;
		readState->currentStripeMetadata = stripeMetadata;

		MemoryContextSwitchTo(oldContext);

		if (stripeBuffers->rowCount != 0)
		{
			readState->stripeBuffers = stripeBuffers;
			readState->stripeReadRowCount = 0;
			readState->deserializedChunkIndex = -1;
			break;
		}
	}

	uint32 chunkIndex = readState->stripeReadRowCount / stripeMetadata->chunkRowCount;
	uint32 chunkRowIndex = readState->stripeReadRowCount % stripeMetadata->chunkRowCount;

	if (chunkIndex != readState->deserializedChunkIndex)
	{
		uint32 chunkRowCount = 0;

		uint32 stripeRowCount = stripeMetadata->rowCount;
		uint32 lastChunkIndex = stripeRowCount / stripeMetadata->chunkRowCount;
		if (chunkIndex == lastChunkIndex)
		{
			chunkRowCount = stripeRowCount % stripeMetadata->chunkRowCount;
		}
		else
		{
			chunkRowCount = stripeMetadata->chunkRowCount;
		}

		oldContext = MemoryContextSwitchTo(readState->stripeReadContext);

		FreeChunkData(readState->chunkData);
		readState->chunkData =
			DeserializeChunkData(readState->stripeBuffers, chunkIndex,
								 chunkRowCount, readState->tupleDescriptor,
								 readState->projectedColumnList);

		MemoryContextSwitchTo(oldContext);

		readState->deserializedChunkIndex = chunkIndex;
	}

	ReadStripeNextRow(readState->stripeBuffers, readState->projectedColumnList,
					  chunkIndex, chunkRowIndex, readState->chunkData,
					  columnValues, columnNulls);

	/*
	 * If we finished reading the current stripe, set stripe data to NULL. That
	 * way, we will load a new stripe the next time this function gets called.
	 */
	readState->stripeReadRowCount++;
	if (readState->stripeReadRowCount == readState->stripeBuffers->rowCount)
	{
		readState->stripeBuffers = NULL;
	}

	return true;
}


/*
 * CStoreRescan clears the position where we were scanning so that the next read starts at
 * the beginning again
 */
void
CStoreRescan(TableReadState *readState)
{
	readState->stripeBuffers = NULL;
	readState->readStripeCount = 0;
	readState->stripeReadRowCount = 0;
}


/* Finishes a cstore read operation. */
void
CStoreEndRead(TableReadState *readState)
{
	MemoryContextDelete(readState->stripeReadContext);
	list_free_deep(readState->stripeList);
	pfree(readState);
}


/*
 * CreateEmptyChunkDataArray creates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 */
ChunkData *
CreateEmptyChunkData(uint32 columnCount, bool *columnMask, uint32 chunkRowCount)
{
	uint32 columnIndex = 0;

	ChunkData *chunkData = palloc0(sizeof(ChunkData));
	chunkData->existsArray = palloc0(columnCount * sizeof(bool *));
	chunkData->valueArray = palloc0(columnCount * sizeof(Datum *));
	chunkData->valueBufferArray = palloc0(columnCount * sizeof(StringInfo));
	chunkData->columnCount = columnCount;
	chunkData->rowCount = chunkRowCount;

	/* allocate chunk memory for deserialized data */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		if (columnMask[columnIndex])
		{
			chunkData->existsArray[columnIndex] = palloc0(chunkRowCount * sizeof(bool));
			chunkData->valueArray[columnIndex] = palloc0(chunkRowCount * sizeof(Datum));
			chunkData->valueBufferArray[columnIndex] = NULL;
		}
	}

	return chunkData;
}


/*
 * FreeChunkData deallocates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 * ColumnChunkData->serializedValueBuffer lives in memory read/write context
 * so it is deallocated automatically when the context is deleted.
 */
void
FreeChunkData(ChunkData *chunkData)
{
	uint32 columnIndex = 0;

	if (chunkData == NULL)
	{
		return;
	}

	for (columnIndex = 0; columnIndex < chunkData->columnCount; columnIndex++)
	{
		if (chunkData->existsArray[columnIndex] != NULL)
		{
			pfree(chunkData->existsArray[columnIndex]);
		}

		if (chunkData->valueArray[columnIndex] != NULL)
		{
			pfree(chunkData->valueArray[columnIndex]);
		}
	}

	pfree(chunkData->existsArray);
	pfree(chunkData->valueArray);
	pfree(chunkData);
}


/* CStoreTableRowCount returns the exact row count of a table using skiplists */
uint64
CStoreTableRowCount(Relation relation)
{
	ListCell *stripeMetadataCell = NULL;
	uint64 totalRowCount = 0;
	List *stripeList = StripesForRelfilenode(relation->rd_node);

	foreach(stripeMetadataCell, stripeList)
	{
		StripeMetadata *stripeMetadata = (StripeMetadata *) lfirst(stripeMetadataCell);
		totalRowCount += stripeMetadata->rowCount;
	}

	return totalRowCount;
}


/*
 * LoadFilteredStripeBuffers reads serialized stripe data from the given file.
 * The function skips over chunks whose rows are refuted by restriction qualifiers,
 * and only loads columns that are projected in the query.
 */
static StripeBuffers *
LoadFilteredStripeBuffers(Relation relation, StripeMetadata *stripeMetadata,
						  TupleDesc tupleDescriptor, List *projectedColumnList,
						  List *whereClauseList)
{
	uint32 columnIndex = 0;
	uint32 columnCount = tupleDescriptor->natts;

	bool *projectedColumnMask = ProjectedColumnMask(columnCount, projectedColumnList);

	StripeSkipList *stripeSkipList = ReadStripeSkipList(relation->rd_node,
														stripeMetadata->id,
														tupleDescriptor,
														stripeMetadata->chunkCount);

	bool *selectedChunkMask = SelectedChunkMask(stripeSkipList, projectedColumnList,
												whereClauseList);

	StripeSkipList *selectedChunkSkipList =
		SelectedChunkSkipList(stripeSkipList, projectedColumnMask,
							  selectedChunkMask);

	/* load column data for projected columns */
	ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));

	for (columnIndex = 0; columnIndex < stripeMetadata->columnCount; columnIndex++)
	{
		if (projectedColumnMask[columnIndex])
		{
			ColumnChunkSkipNode *chunkSkipNode =
				selectedChunkSkipList->chunkSkipNodeArray[columnIndex];
			Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
			uint32 chunkCount = selectedChunkSkipList->chunkCount;

			ColumnBuffers *columnBuffers = LoadColumnBuffers(relation, chunkSkipNode,
															 chunkCount,
															 stripeMetadata->fileOffset,
															 attributeForm);

			columnBuffersArray[columnIndex] = columnBuffers;
		}
	}

	StripeBuffers *stripeBuffers = palloc0(sizeof(StripeBuffers));
	stripeBuffers->columnCount = columnCount;
	stripeBuffers->rowCount = StripeSkipListRowCount(selectedChunkSkipList);
	stripeBuffers->columnBuffersArray = columnBuffersArray;

	return stripeBuffers;
}


/*
 * ReadStripeNextRow reads the next row from the given stripe, finds the projected
 * column values within this row, and accordingly sets the column values and nulls.
 * Note that this function sets the values for all non-projected columns to null.
 */
static void
ReadStripeNextRow(StripeBuffers *stripeBuffers, List *projectedColumnList,
				  uint64 chunkIndex, uint64 chunkRowIndex,
				  ChunkData *chunkData, Datum *columnValues,
				  bool *columnNulls)
{
	ListCell *projectedColumnCell = NULL;

	/* set all columns to null by default */
	memset(columnNulls, 1, stripeBuffers->columnCount * sizeof(bool));

	foreach(projectedColumnCell, projectedColumnList)
	{
		Var *projectedColumn = lfirst(projectedColumnCell);
		uint32 columnIndex = projectedColumn->varattno - 1;

		if (chunkData->existsArray[columnIndex][chunkRowIndex])
		{
			columnValues[columnIndex] = chunkData->valueArray[columnIndex][chunkRowIndex];
			columnNulls[columnIndex] = false;
		}
	}
}


/*
 * LoadColumnBuffers reads serialized column data from the given file. These
 * column data are laid out as sequential chunks in the file; and chunk positions
 * and lengths are retrieved from the column chunk skip node array.
 */
static ColumnBuffers *
LoadColumnBuffers(Relation relation, ColumnChunkSkipNode *chunkSkipNodeArray,
				  uint32 chunkCount, uint64 stripeOffset,
				  Form_pg_attribute attributeForm)
{
	uint32 chunkIndex = 0;
	ColumnChunkBuffers **chunkBuffersArray =
		palloc0(chunkCount * sizeof(ColumnChunkBuffers *));

	for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
	{
		chunkBuffersArray[chunkIndex] = palloc0(sizeof(ColumnChunkBuffers));
	}

	/*
	 * We first read the "exists" chunks. We don't read "values" array here,
	 * because "exists" chunks are stored sequentially on disk, and we want to
	 * minimize disk seeks.
	 */
	for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
	{
		ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];
		uint64 existsOffset = stripeOffset + chunkSkipNode->existsChunkOffset;
		StringInfo rawExistsBuffer = ReadFromSmgr(relation, existsOffset,
												  chunkSkipNode->existsLength);

		chunkBuffersArray[chunkIndex]->existsBuffer = rawExistsBuffer;
	}

	/* then read "values" chunks, which are also stored sequentially on disk */
	for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
	{
		ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];
		CompressionType compressionType = chunkSkipNode->valueCompressionType;
		uint64 valueOffset = stripeOffset + chunkSkipNode->valueChunkOffset;
		StringInfo rawValueBuffer = ReadFromSmgr(relation, valueOffset,
												 chunkSkipNode->valueLength);

		chunkBuffersArray[chunkIndex]->valueBuffer = rawValueBuffer;
		chunkBuffersArray[chunkIndex]->valueCompressionType = compressionType;
		chunkBuffersArray[chunkIndex]->decompressedValueSize =
			chunkSkipNode->decompressedValueSize;
	}

	ColumnBuffers *columnBuffers = palloc0(sizeof(ColumnBuffers));
	columnBuffers->chunkBuffersArray = chunkBuffersArray;

	return columnBuffers;
}


/*
 * SelectedChunkMask walks over each column's chunks and checks if a chunk can
 * be filtered without reading its data. The filtering happens when all rows in
 * the chunk can be refuted by the given qualifier conditions.
 */
static bool *
SelectedChunkMask(StripeSkipList *stripeSkipList, List *projectedColumnList,
				  List *whereClauseList)
{
	ListCell *columnCell = NULL;
	uint32 chunkIndex = 0;
	List *restrictInfoList = BuildRestrictInfoList(whereClauseList);

	bool *selectedChunkMask = palloc0(stripeSkipList->chunkCount * sizeof(bool));
	memset(selectedChunkMask, true, stripeSkipList->chunkCount * sizeof(bool));

	foreach(columnCell, projectedColumnList)
	{
		Var *column = lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;

		/* if this column's data type doesn't have a comparator, skip it */
		FmgrInfo *comparisonFunction = GetFunctionInfoOrNull(column->vartype,
															 BTREE_AM_OID,
															 BTORDER_PROC);
		if (comparisonFunction == NULL)
		{
			continue;
		}

		Node *baseConstraint = BuildBaseConstraint(column);
		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			bool predicateRefuted = false;
			ColumnChunkSkipNode *chunkSkipNodeArray =
				stripeSkipList->chunkSkipNodeArray[columnIndex];
			ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];

			/*
			 * A column chunk with comparable data type can miss min/max values
			 * if all values in the chunk are NULL.
			 */
			if (!chunkSkipNode->hasMinMax)
			{
				continue;
			}

			UpdateConstraint(baseConstraint, chunkSkipNode->minimumValue,
							 chunkSkipNode->maximumValue);

			List *constraintList = list_make1(baseConstraint);
#if (PG_VERSION_NUM >= 100000)
			predicateRefuted = predicate_refuted_by(constraintList, restrictInfoList,
													false);
#else
			predicateRefuted = predicate_refuted_by(constraintList, restrictInfoList);
#endif
			if (predicateRefuted)
			{
				selectedChunkMask[chunkIndex] = false;
			}
		}
	}

	return selectedChunkMask;
}


/*
 * GetFunctionInfoOrNull first resolves the operator for the given data type,
 * access method, and support procedure. The function then uses the resolved
 * operator's identifier to fill in a function manager object, and returns
 * this object. This function is based on a similar function from CitusDB's code.
 */
FmgrInfo *
GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId, int16 procedureId)
{
	FmgrInfo *functionInfo = NULL;

	/* get default operator class from pg_opclass for datum type */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);
	if (operatorClassId == InvalidOid)
	{
		return NULL;
	}

	Oid operatorFamilyId = get_opclass_family(operatorClassId);
	if (operatorFamilyId == InvalidOid)
	{
		return NULL;
	}

	Oid operatorId = get_opfamily_proc(operatorFamilyId, typeId, typeId, procedureId);
	if (operatorId != InvalidOid)
	{
		functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

		/* fill in the FmgrInfo struct using the operatorId */
		fmgr_info(operatorId, functionInfo);
	}

	return functionInfo;
}


/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. The function is copied from CitusDB's shard pruning
 * logic.
 */
static List *
BuildRestrictInfoList(List *whereClauseList)
{
	List *restrictInfoList = NIL;

	ListCell *qualCell = NULL;
	foreach(qualCell, whereClauseList)
	{
		Node *qualNode = (Node *) lfirst(qualCell);

		RestrictInfo *restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (var <= max && var >= min), where
 * min and max values represent a chunk's min and max values. These chunk
 * values are filled in after the constraint is built. This function is based
 * on a similar function from CitusDB's shard pruning logic.
 */
static Node *
BuildBaseConstraint(Var *variable)
{
	OpExpr *lessThanExpr = MakeOpExpression(variable, BTLessEqualStrategyNumber);
	OpExpr *greaterThanExpr = MakeOpExpression(variable, BTGreaterEqualStrategyNumber);

	Node *baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number. The function is copied from CitusDB's shard pruning logic.
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;

	/* Load the operator from system catalogs */
	Oid operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	Const *constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	OpExpr *expression = (OpExpr *) make_opclause(operatorId,
												  InvalidOid, /* no result type yet */
												  false, /* no return set */
												  (Expr *) variable,
												  (Expr *) constantValue,
												  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * GetOperatorByType returns operator Oid for the given type, access method,
 * and strategy number. Note that this function incorrectly errors out when
 * the given type doesn't have its own operator but can use another compatible
 * type's default operator. The function is copied from CitusDB's shard pruning
 * logic.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
}


/*
 * UpdateConstraint updates the base constraint with the given min/max values.
 * The function is copied from CitusDB's shard pruning logic.
 */
static void
UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr);
	Node *maxNode = get_rightop((Expr *) lessThanExpr);

	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	Const *minConstant = (Const *) minNode;
	Const *maxConstant = (Const *) maxNode;

	minConstant->constvalue = minValue;
	maxConstant->constvalue = maxValue;

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = true;
	maxConstant->constbyval = true;
}


/*
 * SelectedChunkSkipList constructs a new StripeSkipList in which the
 * non-selected chunks are removed from the given stripeSkipList.
 */
static StripeSkipList *
SelectedChunkSkipList(StripeSkipList *stripeSkipList, bool *projectedColumnMask,
					  bool *selectedChunkMask)
{
	uint32 selectedChunkCount = 0;
	uint32 chunkIndex = 0;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
	{
		if (selectedChunkMask[chunkIndex])
		{
			selectedChunkCount++;
		}
	}

	ColumnChunkSkipNode **selectedChunkSkipNodeArray = palloc0(columnCount *
															   sizeof(ColumnChunkSkipNode
																	  *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 selectedChunkIndex = 0;
		bool firstColumn = columnIndex == 0;

		/* first column's chunk skip node is always read */
		if (!projectedColumnMask[columnIndex] && !firstColumn)
		{
			selectedChunkSkipNodeArray[columnIndex] = NULL;
			continue;
		}

		Assert(stripeSkipList->chunkSkipNodeArray[columnIndex] != NULL);

		selectedChunkSkipNodeArray[columnIndex] = palloc0(selectedChunkCount *
														  sizeof(ColumnChunkSkipNode));

		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			if (selectedChunkMask[chunkIndex])
			{
				selectedChunkSkipNodeArray[columnIndex][selectedChunkIndex] =
					stripeSkipList->chunkSkipNodeArray[columnIndex][chunkIndex];
				selectedChunkIndex++;
			}
		}
	}

	StripeSkipList *SelectedChunkSkipList = palloc0(sizeof(StripeSkipList));
	SelectedChunkSkipList->chunkSkipNodeArray = selectedChunkSkipNodeArray;
	SelectedChunkSkipList->chunkCount = selectedChunkCount;
	SelectedChunkSkipList->columnCount = stripeSkipList->columnCount;

	return SelectedChunkSkipList;
}


/*
 * StripeSkipListRowCount counts the number of rows in the given stripeSkipList.
 * To do this, the function finds the first column, and sums up row counts across
 * all chunks for that column.
 */
static uint32
StripeSkipListRowCount(StripeSkipList *stripeSkipList)
{
	uint32 stripeSkipListRowCount = 0;
	uint32 chunkIndex = 0;
	ColumnChunkSkipNode *firstColumnSkipNodeArray =
		stripeSkipList->chunkSkipNodeArray[0];

	for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
	{
		uint32 chunkRowCount = firstColumnSkipNodeArray[chunkIndex].rowCount;
		stripeSkipListRowCount += chunkRowCount;
	}

	return stripeSkipListRowCount;
}


/*
 * ProjectedColumnMask returns a boolean array in which the projected columns
 * from the projected column list are marked as true.
 */
static bool *
ProjectedColumnMask(uint32 columnCount, List *projectedColumnList)
{
	bool *projectedColumnMask = palloc0(columnCount * sizeof(bool));
	ListCell *columnCell = NULL;

	foreach(columnCell, projectedColumnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;
		projectedColumnMask[columnIndex] = true;
	}

	return projectedColumnMask;
}


/*
 * DeserializeBoolArray reads an array of bits from the given buffer and stores
 * it in provided bool array.
 */
static void
DeserializeBoolArray(StringInfo boolArrayBuffer, bool *boolArray,
					 uint32 boolArrayLength)
{
	uint32 boolArrayIndex = 0;

	uint32 maximumBoolCount = boolArrayBuffer->len * 8;
	if (boolArrayLength > maximumBoolCount)
	{
		ereport(ERROR, (errmsg("insufficient data for reading boolean array")));
	}

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		uint32 byteIndex = boolArrayIndex / 8;
		uint32 bitIndex = boolArrayIndex % 8;
		uint8 bitmask = (1 << bitIndex);

		uint8 shiftedBit = (boolArrayBuffer->data[byteIndex] & bitmask);
		if (shiftedBit == 0)
		{
			boolArray[boolArrayIndex] = false;
		}
		else
		{
			boolArray[boolArrayIndex] = true;
		}
	}
}


/*
 * DeserializeDatumArray reads an array of datums from the given buffer and stores
 * them in provided datumArray. If a value is marked as false in the exists array,
 * the function assumes that the datum isn't in the buffer, and simply skips it.
 */
static void
DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray, uint32 datumCount,
					  bool datumTypeByValue, int datumTypeLength,
					  char datumTypeAlign, Datum *datumArray)
{
	uint32 datumIndex = 0;
	uint32 currentDatumDataOffset = 0;

	for (datumIndex = 0; datumIndex < datumCount; datumIndex++)
	{
		if (!existsArray[datumIndex])
		{
			continue;
		}

		char *currentDatumDataPointer = datumBuffer->data + currentDatumDataOffset;

		datumArray[datumIndex] = fetch_att(currentDatumDataPointer, datumTypeByValue,
										   datumTypeLength);
		currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
													 datumTypeLength,
													 currentDatumDataPointer);
		currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
												   datumTypeAlign);

		if (currentDatumDataOffset > datumBuffer->len)
		{
			ereport(ERROR, (errmsg("insufficient data left in datum buffer")));
		}
	}
}


/*
 * DeserializeChunkData deserializes requested data chunk for all columns and
 * stores in chunkDataArray. It uncompresses serialized data if necessary. The
 * function also deallocates data buffers used for previous chunk, and compressed
 * data buffers for the current chunk which will not be needed again. If a column
 * data is not present serialized buffer, then default value (or null) is used
 * to fill value array.
 */
static ChunkData *
DeserializeChunkData(StripeBuffers *stripeBuffers, uint64 chunkIndex,
					 uint32 rowCount, TupleDesc tupleDescriptor,
					 List *projectedColumnList)
{
	int columnIndex = 0;
	bool *columnMask = ProjectedColumnMask(tupleDescriptor->natts, projectedColumnList);
	ChunkData *chunkData = CreateEmptyChunkData(tupleDescriptor->natts, columnMask,
												rowCount);

	for (columnIndex = 0; columnIndex < stripeBuffers->columnCount; columnIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		bool columnAdded = false;

		if (columnBuffers == NULL && columnMask[columnIndex])
		{
			columnAdded = true;
		}

		if (columnBuffers != NULL)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];

			/* decompress and deserialize current chunk's data */
			StringInfo valueBuffer =
				DecompressBuffer(chunkBuffers->valueBuffer,
								 chunkBuffers->valueCompressionType,
								 chunkBuffers->decompressedValueSize);

			if (chunkBuffers->valueCompressionType != COMPRESSION_NONE)
			{
				/* compressed data is not needed anymore */
				pfree(chunkBuffers->valueBuffer->data);
				pfree(chunkBuffers->valueBuffer);
			}

			DeserializeBoolArray(chunkBuffers->existsBuffer,
								 chunkData->existsArray[columnIndex],
								 rowCount);
			DeserializeDatumArray(valueBuffer, chunkData->existsArray[columnIndex],
								  rowCount, attributeForm->attbyval,
								  attributeForm->attlen, attributeForm->attalign,
								  chunkData->valueArray[columnIndex]);

			/* store current chunk's data buffer to be freed at next chunk read */
			chunkData->valueBufferArray[columnIndex] = valueBuffer;
		}
		else if (columnAdded)
		{
			/*
			 * This is a column that was added after creation of this stripe.
			 * So we use either the default value or NULL.
			 */
			if (attributeForm->atthasdef)
			{
				int rowIndex = 0;

				Datum defaultValue = ColumnDefaultValue(tupleDescriptor->constr,
														attributeForm);

				for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
				{
					chunkData->existsArray[columnIndex][rowIndex] = true;
					chunkData->valueArray[columnIndex][rowIndex] = defaultValue;
				}
			}
			else
			{
				memset(chunkData->existsArray[columnIndex], false,
					   rowCount * sizeof(bool));
			}
		}
	}

	return chunkData;
}


/*
 * ColumnDefaultValue returns default value for given column. Only const values
 * are supported. The function errors on any other default value expressions.
 */
static Datum
ColumnDefaultValue(TupleConstr *tupleConstraints, Form_pg_attribute attributeForm)
{
	Node *defaultValueNode = NULL;
	int defValIndex = 0;

	for (defValIndex = 0; defValIndex < tupleConstraints->num_defval; defValIndex++)
	{
		AttrDefault attrDefault = tupleConstraints->defval[defValIndex];
		if (attrDefault.adnum == attributeForm->attnum)
		{
			defaultValueNode = stringToNode(attrDefault.adbin);
			break;
		}
	}

	Assert(defaultValueNode != NULL);

	/* try reducing the default value node to a const node */
	defaultValueNode = eval_const_expressions(NULL, defaultValueNode);
	if (IsA(defaultValueNode, Const))
	{
		Const *constNode = (Const *) defaultValueNode;
		return constNode->constvalue;
	}
	else
	{
		const char *columnName = NameStr(attributeForm->attname);
		ereport(ERROR, (errmsg("unsupported default value for column \"%s\"", columnName),
						errhint("Expression is either mutable or "
								"does not evaluate to constant value")));
	}
}


StringInfo
ReadFromSmgr(Relation rel, uint64 offset, uint32 size)
{
	StringInfo resultBuffer = makeStringInfo();
	uint64 read = 0;

	enlargeStringInfo(resultBuffer, size);
	resultBuffer->len = size;

	while (read < size)
	{
		SmgrAddr addr = logical_to_smgr(offset + read);

		Buffer buffer = ReadBuffer(rel, addr.blockno);
		Page page = BufferGetPage(buffer);
		PageHeader phdr = (PageHeader) page;

		uint32 to_read = Min(size - read, phdr->pd_upper - addr.offset);
		memcpy_s(resultBuffer->data + read, size - read, page + addr.offset, to_read);
		ReleaseBuffer(buffer);
		read += to_read;
	}

	return resultBuffer;
}
