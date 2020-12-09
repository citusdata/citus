/*-------------------------------------------------------------------------
 *
 * cstore.h
 *
 * Type and function declarations for CStore
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_H
#define CSTORE_H
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
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_count"
#define OPTION_NAME_CHUNK_ROW_COUNT "chunk_row_count"

/* Limits for option parameters */
#define STRIPE_ROW_COUNT_MINIMUM 1000
#define STRIPE_ROW_COUNT_MAXIMUM 10000000
#define CHUNK_ROW_COUNT_MINIMUM 1000
#define CHUNK_ROW_COUNT_MAXIMUM 100000
#define COMPRESSION_LEVEL_MIN 1
#define COMPRESSION_LEVEL_MAX 19

/* String representations of compression types */
#define COMPRESSION_STRING_NONE "none"
#define COMPRESSION_STRING_PG_LZ "pglz"

/* CStore file signature */
#define CSTORE_MAGIC_NUMBER "citus_cstore"
#define CSTORE_VERSION_MAJOR 1
#define CSTORE_VERSION_MINOR 7

/* miscellaneous defines */
#define CSTORE_TUPLE_COST_MULTIPLIER 10
#define CSTORE_POSTSCRIPT_SIZE_LENGTH 1
#define CSTORE_POSTSCRIPT_SIZE_MAX 256
#define CSTORE_BYTES_PER_PAGE (BLCKSZ - SizeOfPageHeaderData)

/* Enumaration for cstore file's compression method */
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
 * a cstore file. To resolve these values, we first check foreign table's options,
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
 * StripeMetadata represents information about a stripe. This information is
 * stored in the cstore file's footer.
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


/* DataFileMetadata represents the metadata of a cstore file. */
typedef struct DataFileMetadata
{
	List *stripeMetadataList;
} DataFileMetadata;


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


/* StripeBuffers represents data for a row stripe in a cstore file. */
typedef struct StripeBuffers
{
	uint32 columnCount;
	uint32 rowCount;
	ColumnBuffers **columnBuffersArray;
} StripeBuffers;


/* TableReadState represents state of a cstore file read operation. */
typedef struct TableReadState
{
	List *stripeList;
	StripeMetadata *currentStripeMetadata;
	TupleDesc tupleDescriptor;
	Relation relation;

	/*
	 * List of Var pointers for columns in the query. We use this both for
	 * getting vector of projected columns, and also when we want to build
	 * base constraint to find selected row chunks.
	 */
	List *projectedColumnList;

	List *whereClauseList;
	MemoryContext stripeReadContext;
	StripeBuffers *stripeBuffers;
	uint32 readStripeCount;
	uint64 stripeReadRowCount;
	ChunkData *chunkData;
	int32 deserializedChunkIndex;
} TableReadState;


/* TableWriteState represents state of a cstore file write operation. */
typedef struct TableWriteState
{
	TupleDesc tupleDescriptor;
	FmgrInfo **comparisonFunctionArray;
	RelFileNode relfilenode;

	MemoryContext stripeWriteContext;
	MemoryContext perTupleContext;
	StripeBuffers *stripeBuffers;
	StripeSkipList *stripeSkipList;
	ColumnarOptions options;
	ChunkData *chunkData;

	/*
	 * compressionBuffer buffer is used as temporary storage during
	 * data value compression operation. It is kept here to minimize
	 * memory allocations. It lives in stripeWriteContext and gets
	 * deallocated when memory context is reset.
	 */
	StringInfo compressionBuffer;
} TableWriteState;

extern int cstore_compression;
extern int cstore_stripe_row_count;
extern int cstore_chunk_row_count;
extern int columnar_compression_level;

extern void cstore_init(void);

extern CompressionType ParseCompressionType(const char *compressionTypeString);

/* Function declarations for writing to a cstore file */
extern TableWriteState * CStoreBeginWrite(RelFileNode relfilenode,
										  ColumnarOptions options,
										  TupleDesc tupleDescriptor);
extern void CStoreWriteRow(TableWriteState *state, Datum *columnValues,
						   bool *columnNulls);
extern void CStoreFlushPendingWrites(TableWriteState *state);
extern void CStoreEndWrite(TableWriteState *state);
extern bool ContainsPendingWrites(TableWriteState *state);

/* Function declarations for reading from a cstore file */
extern TableReadState * CStoreBeginRead(Relation relation,
										TupleDesc tupleDescriptor,
										List *projectedColumnList, List *qualConditions);
extern bool CStoreReadFinished(TableReadState *state);
extern bool CStoreReadNextRow(TableReadState *state, Datum *columnValues,
							  bool *columnNulls);
extern void CStoreRescan(TableReadState *readState);
extern void CStoreEndRead(TableReadState *state);

/* Function declarations for common functions */
extern FmgrInfo * GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId,
										int16 procedureId);
extern ChunkData * CreateEmptyChunkData(uint32 columnCount, bool *columnMask,
										uint32 chunkRowCount);
extern void FreeChunkData(ChunkData *chunkData);
extern uint64 CStoreTableRowCount(Relation relation);
extern bool CompressBuffer(StringInfo inputBuffer,
						   StringInfo outputBuffer,
						   CompressionType compressionType,
						   int compressionLevel);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType,
								   uint64 decompressedSize);
extern const char * CompressionTypeStr(CompressionType type);

/* cstore_metadata_tables.c */
extern void InitColumnarOptions(Oid regclass);
extern void SetColumnarOptions(Oid regclass, ColumnarOptions *options);
extern bool DeleteColumnarTableOptions(Oid regclass, bool missingOk);
extern bool ReadColumnarOptions(Oid regclass, ColumnarOptions *options);
extern void WriteToSmgr(Relation relation, uint64 logicalOffset,
						char *data, uint32 dataLength);
extern StringInfo ReadFromSmgr(Relation rel, uint64 offset, uint32 size);
extern bool IsCStoreTableAmTable(Oid relationId);

/* cstore_metadata_tables.c */
extern void DeleteMetadataRows(RelFileNode relfilenode);
extern List * StripesForRelfilenode(RelFileNode relfilenode);
extern uint64 GetHighestUsedAddress(RelFileNode relfilenode);
extern StripeMetadata ReserveStripe(Relation rel, uint64 size,
									uint64 rowCount, uint64 columnCount,
									uint64 chunkCount, uint64 chunkRowCount);
extern void SaveStripeSkipList(RelFileNode relfilenode, uint64 stripe,
							   StripeSkipList *stripeSkipList,
							   TupleDesc tupleDescriptor);
extern StripeSkipList * ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe,
										   TupleDesc tupleDescriptor,
										   uint32 chunkCount);
extern Datum columnar_relation_storageid(PG_FUNCTION_ARGS);


/* write_state_management.c */
extern TableWriteState * cstore_init_write_state(Relation relation, TupleDesc
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

	addr.blockno = logicalOffset / CSTORE_BYTES_PER_PAGE;
	addr.offset = SizeOfPageHeaderData + (logicalOffset % CSTORE_BYTES_PER_PAGE);

	return addr;
}


/*
 * Map a physical page adnd offset address to a logical address.
 */
static inline uint64
smgr_to_logical(SmgrAddr addr)
{
	return CSTORE_BYTES_PER_PAGE * addr.blockno + addr.offset - SizeOfPageHeaderData;
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


#endif /* CSTORE_H */
