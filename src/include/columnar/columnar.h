/*-------------------------------------------------------------------------
 *
 * columnar.h
 *
 * Type and function declarations for Columnar
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_H
#define COLUMNAR_H
#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/bufpage.h"
#include "storage/lockdefs.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "columnar/columnar_compression.h"
#include "columnar/columnar_metadata.h"

/* Defines for valid option names */
#define OPTION_NAME_COMPRESSION_TYPE "compression"
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_limit"
#define OPTION_NAME_CHUNK_ROW_COUNT "chunk_group_row_limit"

/* Limits for option parameters */
#define STRIPE_ROW_COUNT_MINIMUM 1000
#define STRIPE_ROW_COUNT_MAXIMUM 10000000
#define CHUNK_ROW_COUNT_MINIMUM 1000
#define CHUNK_ROW_COUNT_MAXIMUM 100000
#define COMPRESSION_LEVEL_MIN 1
#define COMPRESSION_LEVEL_MAX 19

/* Columnar file signature */
#define COLUMNAR_VERSION_MAJOR 2
#define COLUMNAR_VERSION_MINOR 0

/* miscellaneous defines */
#define COLUMNAR_TUPLE_COST_MULTIPLIER 10
#define COLUMNAR_POSTSCRIPT_SIZE_LENGTH 1
#define COLUMNAR_POSTSCRIPT_SIZE_MAX 256
#define COLUMNAR_BYTES_PER_PAGE (BLCKSZ - SizeOfPageHeaderData)

/*
 * ColumnarOptions holds the option values to be used when reading or writing
 * a columnar table. To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values specified above.
 */
typedef struct ColumnarOptions
{
	uint64 stripeRowCount;
	uint32 chunkRowCount;
	CompressionType compressionType;
	int compressionLevel;
} ColumnarOptions;


/*
 * ColumnarTableDDLContext holds the instance variable for the TableDDLCommandFunction
 * instance described below.
 */
typedef struct ColumnarTableDDLContext
{
	char *schemaName;
	char *relationName;
	ColumnarOptions options;
} ColumnarTableDDLContext;


/* ColumnChunkSkipNode contains statistics for a ColumnChunkData. */
typedef struct ColumnChunkSkipNode
{
	/* statistics about values of a column chunk */
	bool hasMinMax;
	Datum minimumValue;
	Datum maximumValue;
	uint64 rowCount;

	/*
	 * Offsets and sizes of value and exists streams in the column data.
	 * These enable us to skip reading suppressed row chunks, and start reading
	 * a chunk without reading previous chunks.
	 */
	uint64 valueChunkOffset;
	uint64 valueLength;
	uint64 existsChunkOffset;
	uint64 existsLength;

	/*
	 * This is used for (1) determining destination size when decompressing,
	 * (2) calculating compression rates when logging stats.
	 */
	uint64 decompressedValueSize;

	CompressionType valueCompressionType;
	int valueCompressionLevel;
} ColumnChunkSkipNode;


/*
 * StripeSkipList can be used for skipping row chunks. It contains a column chunk
 * skip node for each chunk of each column. chunkSkipNodeArray[column][chunk]
 * is the entry for the specified column chunk.
 */
typedef struct StripeSkipList
{
	ColumnChunkSkipNode **chunkSkipNodeArray;
	uint32 *chunkGroupRowCounts;
	uint32 columnCount;
	uint32 chunkCount;
} StripeSkipList;


/*
 * ChunkData represents a chunk of data for multiple columns. valueArray stores
 * the values of data, and existsArray stores whether a value is present.
 * valueBuffer is used to store (uncompressed) serialized values
 * referenced by Datum's in valueArray. It is only used for by-reference Datum's.
 * There is a one-to-one correspondence between valueArray and existsArray.
 */
typedef struct ChunkData
{
	uint32 rowCount;
	uint32 columnCount;

	/*
	 * Following are indexed by [column][row]. If a column is not projected,
	 * then existsArray[column] and valueArray[column] are NULL.
	 */
	bool **existsArray;
	Datum **valueArray;

	/* valueBuffer keeps actual data for type-by-reference datums from valueArray. */
	StringInfo *valueBufferArray;
} ChunkData;


/*
 * ColumnChunkBuffers represents a chunk of serialized data in a column.
 * valueBuffer stores the serialized values of data, and existsBuffer stores
 * serialized value of presence information. valueCompressionType contains
 * compression type if valueBuffer is compressed. Finally rowCount has
 * the number of rows in this chunk.
 */
typedef struct ColumnChunkBuffers
{
	StringInfo existsBuffer;
	StringInfo valueBuffer;
	CompressionType valueCompressionType;
	uint64 decompressedValueSize;
} ColumnChunkBuffers;


/*
 * ColumnBuffers represents data buffers for a column in a row stripe. Each
 * column is made of multiple column chunks.
 */
typedef struct ColumnBuffers
{
	ColumnChunkBuffers **chunkBuffersArray;
} ColumnBuffers;


/* StripeBuffers represents data for a row stripe. */
typedef struct StripeBuffers
{
	uint32 columnCount;
	uint32 rowCount;
	ColumnBuffers **columnBuffersArray;

	uint32 *selectedChunkGroupRowCounts;
} StripeBuffers;


/* ColumnarReadState represents state of a columnar scan. */
struct ColumnarReadState;
typedef struct ColumnarReadState ColumnarReadState;


/* ColumnarWriteState represents state of a columnar write operation. */
struct ColumnarWriteState;
typedef struct ColumnarWriteState ColumnarWriteState;

extern int columnar_compression;
extern int columnar_stripe_row_limit;
extern int columnar_chunk_group_row_limit;
extern int columnar_compression_level;

extern void columnar_init_gucs(void);

extern CompressionType ParseCompressionType(const char *compressionTypeString);

/* Function declarations for writing to a columnar table */
extern ColumnarWriteState * ColumnarBeginWrite(RelFileNode relfilenode,
											   ColumnarOptions options,
											   TupleDesc tupleDescriptor);
extern uint64 ColumnarWriteRow(ColumnarWriteState *state, Datum *columnValues,
							   bool *columnNulls);
extern void ColumnarFlushPendingWrites(ColumnarWriteState *state);
extern void ColumnarEndWrite(ColumnarWriteState *state);
extern bool ContainsPendingWrites(ColumnarWriteState *state);
extern MemoryContext ColumnarWritePerTupleContext(ColumnarWriteState *state);

/* Function declarations for reading from columnar table */
extern ColumnarReadState * ColumnarBeginRead(Relation relation,
											 TupleDesc tupleDescriptor,
											 List *projectedColumnList,
											 List *qualConditions,
											 MemoryContext scanContext);
extern bool ColumnarReadNextRow(ColumnarReadState *state, Datum *columnValues,
								bool *columnNulls, uint64 *rowNumber);
extern void ColumnarRescan(ColumnarReadState *readState);
extern bool ColumnarReadRowByRowNumber(ColumnarReadState *readState,
									   uint64 rowNumber, Datum *columnValues,
									   bool *columnNulls, Snapshot snapshot);
extern void ColumnarEndRead(ColumnarReadState *state);
extern void ColumnarResetRead(ColumnarReadState *readState);
extern int64 ColumnarReadChunkGroupsFiltered(ColumnarReadState *state);

/* Function declarations for common functions */
extern FmgrInfo * GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId,
										int16 procedureId);
extern ChunkData * CreateEmptyChunkData(uint32 columnCount, bool *columnMask,
										uint32 chunkGroupRowCount);
extern void FreeChunkData(ChunkData *chunkData);
extern uint64 ColumnarTableRowCount(Relation relation);
extern const char * CompressionTypeStr(CompressionType type);

/* columnar_metadata_tables.c */
extern void InitColumnarOptions(Oid regclass);
extern void SetColumnarOptions(Oid regclass, ColumnarOptions *options);
extern bool DeleteColumnarTableOptions(Oid regclass, bool missingOk);
extern bool ReadColumnarOptions(Oid regclass, ColumnarOptions *options);
extern bool IsColumnarTableAmTable(Oid relationId);

/* columnar_metadata_tables.c */
extern void DeleteMetadataRows(RelFileNode relfilenode);
extern uint64 ColumnarMetadataNewStorageId(void);
extern uint64 GetHighestUsedAddress(RelFileNode relfilenode);
extern StripeMetadata ReserveStripe(Relation rel, uint64 size,
									uint64 rowCount, uint64 columnCount,
									uint64 chunkCount, uint64 chunkGroupRowCount,
									uint64 stripeFirstRowNumber);
extern void SaveStripeSkipList(RelFileNode relfilenode, uint64 stripe,
							   StripeSkipList *stripeSkipList,
							   TupleDesc tupleDescriptor);
extern void SaveChunkGroups(RelFileNode relfilenode, uint64 stripe,
							List *chunkGroupRowCounts);
extern StripeSkipList * ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe,
										   TupleDesc tupleDescriptor,
										   uint32 chunkCount);
extern StripeMetadata * FindNextStripeByRowNumber(Relation relation, uint64 rowNumber,
												  Snapshot snapshot);
extern StripeMetadata * FindStripeByRowNumber(Relation relation, uint64 rowNumber,
											  Snapshot snapshot);
extern uint64 StripeGetHighestRowNumber(StripeMetadata *stripeMetadata);
extern StripeMetadata * FindStripeWithHighestRowNumber(Relation relation,
													   Snapshot snapshot);
extern Datum columnar_relation_storageid(PG_FUNCTION_ARGS);


/* write_state_management.c */
extern ColumnarWriteState * columnar_init_write_state(Relation relation, TupleDesc
													  tupdesc,
													  SubTransactionId currentSubXid);
extern void FlushWriteStateForRelfilenode(Oid relfilenode, SubTransactionId
										  currentSubXid);
extern void FlushWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId
									  parentSubXid);
extern void DiscardWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId
										parentSubXid);
extern void MarkRelfilenodeDropped(Oid relfilenode, SubTransactionId currentSubXid);
extern void NonTransactionDropWriteState(Oid relfilenode);
extern bool PendingWritesInUpperTransactions(Oid relfilenode,
											 SubTransactionId currentSubXid);
extern MemoryContext GetWriteContextForDebug(void);


#endif /* COLUMNAR_H */
