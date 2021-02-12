/*-------------------------------------------------------------------------
 *
 * columnar.h
 *
 * Type and function declarations for Columnar
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
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
#define COLUMNAR_VERSION_MAJOR 1
#define COLUMNAR_VERSION_MINOR 7

/* miscellaneous defines */
#define COLUMNAR_TUPLE_COST_MULTIPLIER 10
#define COLUMNAR_POSTSCRIPT_SIZE_LENGTH 1
#define COLUMNAR_POSTSCRIPT_SIZE_MAX 256
#define COLUMNAR_BYTES_PER_PAGE (BLCKSZ - SizeOfPageHeaderData)

/* Enumaration for columnar table's compression method */
typedef enum
{
	COMPRESSION_TYPE_INVALID = -1,
	COMPRESSION_NONE = 0,
	COMPRESSION_PG_LZ = 1,
	COMPRESSION_LZ4 = 2,
	COMPRESSION_ZSTD = 3,

	COMPRESSION_COUNT
} CompressionType;


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


/*
 * StripeMetadata represents information about a stripe. This information is
 * stored in the metadata table "columnar.stripe".
 */
typedef struct StripeMetadata
{
	uint64 fileOffset;
	uint64 dataLength;
	uint32 columnCount;
	uint32 chunkCount;
	uint32 chunkRowCount;
	uint64 rowCount;
	uint64 id;
} StripeMetadata;


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
	uint32 *chunkRowCounts;
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


/* TableReadState represents state of a columnar scan. */
struct TableReadState;
typedef struct TableReadState TableReadState;


/* TableWriteState represents state of a columnar write operation. */
struct TableWriteState;
typedef struct TableWriteState TableWriteState;

extern int columnar_compression;
extern int columnar_stripe_row_limit;
extern int columnar_chunk_group_row_limit;
extern int columnar_compression_level;

extern void columnar_init_gucs(void);

extern CompressionType ParseCompressionType(const char *compressionTypeString);

/* Function declarations for writing to a columnar table */
extern TableWriteState * ColumnarBeginWrite(RelFileNode relfilenode,
											ColumnarOptions options,
											TupleDesc tupleDescriptor);
extern void ColumnarWriteRow(TableWriteState *state, Datum *columnValues,
							 bool *columnNulls);
extern void ColumnarFlushPendingWrites(TableWriteState *state);
extern void ColumnarEndWrite(TableWriteState *state);
extern bool ContainsPendingWrites(TableWriteState *state);
extern MemoryContext ColumnarWritePerTupleContext(TableWriteState *state);

/* Function declarations for reading from columnar table */
extern TableReadState * ColumnarBeginRead(Relation relation,
										  TupleDesc tupleDescriptor,
										  List *projectedColumnList,
										  List *qualConditions);
extern bool ColumnarReadNextRow(TableReadState *state, Datum *columnValues,
								bool *columnNulls);
extern void ColumnarRescan(TableReadState *readState);
extern void ColumnarEndRead(TableReadState *state);
extern int64 ColumnarReadChunkGroupsFiltered(TableReadState *state);

/* Function declarations for common functions */
extern FmgrInfo * GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId,
										int16 procedureId);
extern ChunkData * CreateEmptyChunkData(uint32 columnCount, bool *columnMask,
										uint32 chunkRowCount);
extern void FreeChunkData(ChunkData *chunkData);
extern uint64 ColumnarTableRowCount(Relation relation);
extern bool CompressBuffer(StringInfo inputBuffer,
						   StringInfo outputBuffer,
						   CompressionType compressionType,
						   int compressionLevel);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType,
								   uint64 decompressedSize);
extern const char * CompressionTypeStr(CompressionType type);

/* columnar_metadata_tables.c */
extern void InitColumnarOptions(Oid regclass);
extern void SetColumnarOptions(Oid regclass, ColumnarOptions *options);
extern bool DeleteColumnarTableOptions(Oid regclass, bool missingOk);
extern bool ReadColumnarOptions(Oid regclass, ColumnarOptions *options);
extern void WriteToSmgr(Relation relation, uint64 logicalOffset,
						char *data, uint32 dataLength);
extern StringInfo ReadFromSmgr(Relation rel, uint64 offset, uint32 size);
extern bool IsColumnarTableAmTable(Oid relationId);

/* columnar_metadata_tables.c */
extern void DeleteMetadataRows(RelFileNode relfilenode);
extern List * StripesForRelfilenode(RelFileNode relfilenode);
extern uint64 GetHighestUsedAddress(RelFileNode relfilenode);
extern StripeMetadata ReserveStripe(Relation rel, uint64 size,
									uint64 rowCount, uint64 columnCount,
									uint64 chunkCount, uint64 chunkRowCount);
extern void SaveStripeSkipList(RelFileNode relfilenode, uint64 stripe,
							   StripeSkipList *stripeSkipList,
							   TupleDesc tupleDescriptor);
extern void SaveChunkGroups(RelFileNode relfilenode, uint64 stripe,
							List *chunkGroupRowCounts);
extern StripeSkipList * ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe,
										   TupleDesc tupleDescriptor,
										   uint32 chunkCount);
extern Datum columnar_relation_storageid(PG_FUNCTION_ARGS);


/* write_state_management.c */
extern TableWriteState * columnar_init_write_state(Relation relation, TupleDesc
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

typedef struct SmgrAddr
{
	BlockNumber blockno;
	uint32 offset;
} SmgrAddr;

/*
 * Map logical offsets (as tracked in the metadata) to a physical page and
 * offset where the data is kept.
 */
static inline SmgrAddr
logical_to_smgr(uint64 logicalOffset)
{
	SmgrAddr addr;

	addr.blockno = logicalOffset / COLUMNAR_BYTES_PER_PAGE;
	addr.offset = SizeOfPageHeaderData + (logicalOffset % COLUMNAR_BYTES_PER_PAGE);

	return addr;
}


/*
 * Map a physical page adnd offset address to a logical address.
 */
static inline uint64
smgr_to_logical(SmgrAddr addr)
{
	return COLUMNAR_BYTES_PER_PAGE * addr.blockno + addr.offset - SizeOfPageHeaderData;
}


/*
 * Get the first usable address of next block.
 */
static inline SmgrAddr
next_block_start(SmgrAddr addr)
{
	SmgrAddr result = {
		.blockno = addr.blockno + 1,
		.offset = SizeOfPageHeaderData
	};

	return result;
}


#endif /* COLUMNAR_H */
