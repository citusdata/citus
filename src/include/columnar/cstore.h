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
#include "utils/relcache.h"
#include "utils/snapmgr.h"

/* Defines for valid option names */
#define OPTION_NAME_COMPRESSION_TYPE "compression"
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_count"
#define OPTION_NAME_BLOCK_ROW_COUNT "block_row_count"

/* Limits for option parameters */
#define STRIPE_ROW_COUNT_MINIMUM 1000
#define STRIPE_ROW_COUNT_MAXIMUM 10000000
#define BLOCK_ROW_COUNT_MINIMUM 1000
#define BLOCK_ROW_COUNT_MAXIMUM 100000

/* String representations of compression types */
#define COMPRESSION_STRING_NONE "none"
#define COMPRESSION_STRING_PG_LZ "pglz"

/* CStore file signature */
#define CSTORE_MAGIC_NUMBER "citus_cstore"
#define CSTORE_VERSION_MAJOR 1
#define CSTORE_VERSION_MINOR 7

/* miscellaneous defines */
#define CSTORE_FDW_NAME "cstore_fdw"
#define CSTORE_TUPLE_COST_MULTIPLIER 10
#define CSTORE_POSTSCRIPT_SIZE_LENGTH 1
#define CSTORE_POSTSCRIPT_SIZE_MAX 256

/* Enumaration for cstore file's compression method */
typedef enum
{
	COMPRESSION_TYPE_INVALID = -1,
	COMPRESSION_NONE = 0,
	COMPRESSION_PG_LZ = 1,

	COMPRESSION_COUNT
} CompressionType;


/*
 * CStoreFdwOptions holds the option values to be used when reading or writing
 * a cstore file. To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values specified above.
 */
typedef struct CStoreOptions
{
	CompressionType compressionType;
	uint64 stripeRowCount;
	uint32 blockRowCount;
} CStoreOptions;


/*
 * StripeMetadata represents information about a stripe. This information is
 * stored in the cstore file's footer.
 */
typedef struct StripeMetadata
{
	uint64 fileOffset;
	uint64 dataLength;
	uint32 columnCount;
	uint32 blockCount;
	uint32 blockRowCount;
	uint64 rowCount;
	uint64 id;
} StripeMetadata;


/* DataFileMetadata represents the metadata of a cstore file. */
typedef struct DataFileMetadata
{
	List *stripeMetadataList;
	uint64 blockRowCount;
	uint64 stripeRowCount;
	CompressionType compression;
} DataFileMetadata;


/* ColumnBlockSkipNode contains statistics for a ColumnBlockData. */
typedef struct ColumnBlockSkipNode
{
	/* statistics about values of a column block */
	bool hasMinMax;
	Datum minimumValue;
	Datum maximumValue;
	uint64 rowCount;

	/*
	 * Offsets and sizes of value and exists streams in the column data.
	 * These enable us to skip reading suppressed row blocks, and start reading
	 * a block without reading previous blocks.
	 */
	uint64 valueBlockOffset;
	uint64 valueLength;
	uint64 existsBlockOffset;
	uint64 existsLength;

	CompressionType valueCompressionType;
} ColumnBlockSkipNode;


/*
 * StripeSkipList can be used for skipping row blocks. It contains a column block
 * skip node for each block of each column. blockSkipNodeArray[column][block]
 * is the entry for the specified column block.
 */
typedef struct StripeSkipList
{
	ColumnBlockSkipNode **blockSkipNodeArray;
	uint32 columnCount;
	uint32 blockCount;
} StripeSkipList;


/*
 * BlockData represents a block of data for multiple columns. valueArray stores
 * the values of data, and existsArray stores whether a value is present.
 * valueBuffer is used to store (uncompressed) serialized values
 * referenced by Datum's in valueArray. It is only used for by-reference Datum's.
 * There is a one-to-one correspondence between valueArray and existsArray.
 */
typedef struct BlockData
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
} BlockData;


/*
 * ColumnBlockBuffers represents a block of serialized data in a column.
 * valueBuffer stores the serialized values of data, and existsBuffer stores
 * serialized value of presence information. valueCompressionType contains
 * compression type if valueBuffer is compressed. Finally rowCount has
 * the number of rows in this block.
 */
typedef struct ColumnBlockBuffers
{
	StringInfo existsBuffer;
	StringInfo valueBuffer;
	CompressionType valueCompressionType;
} ColumnBlockBuffers;


/*
 * ColumnBuffers represents data buffers for a column in a row stripe. Each
 * column is made of multiple column blocks.
 */
typedef struct ColumnBuffers
{
	ColumnBlockBuffers **blockBuffersArray;
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
	DataFileMetadata *datafileMetadata;
	StripeMetadata *currentStripeMetadata;
	TupleDesc tupleDescriptor;
	Relation relation;

	/*
	 * List of Var pointers for columns in the query. We use this both for
	 * getting vector of projected columns, and also when we want to build
	 * base constraint to find selected row blocks.
	 */
	List *projectedColumnList;

	List *whereClauseList;
	MemoryContext stripeReadContext;
	StripeBuffers *stripeBuffers;
	uint32 readStripeCount;
	uint64 stripeReadRowCount;
	BlockData *blockData;
	int32 deserializedBlockIndex;
} TableReadState;


/* TableWriteState represents state of a cstore file write operation. */
typedef struct TableWriteState
{
	CompressionType compressionType;
	TupleDesc tupleDescriptor;
	FmgrInfo **comparisonFunctionArray;
	Relation relation;

	MemoryContext stripeWriteContext;
	StripeBuffers *stripeBuffers;
	StripeSkipList *stripeSkipList;
	uint32 stripeMaxRowCount;
	uint32 blockRowCount;
	BlockData *blockData;

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
extern int cstore_block_row_count;

extern void cstore_init(void);

extern CompressionType ParseCompressionType(const char *compressionTypeString);

/* Function declarations for writing to a cstore file */
extern TableWriteState * CStoreBeginWrite(Relation relation,
										  CompressionType compressionType,
										  uint64 stripeMaxRowCount,
										  uint32 blockRowCount,
										  TupleDesc tupleDescriptor);
extern void CStoreWriteRow(TableWriteState *state, Datum *columnValues,
						   bool *columnNulls);
extern void CStoreEndWrite(TableWriteState *state);

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
extern BlockData * CreateEmptyBlockData(uint32 columnCount, bool *columnMask,
										uint32 blockRowCount);
extern void FreeBlockData(BlockData *blockData);
extern uint64 CStoreTableRowCount(Relation relation);
extern bool CompressBuffer(StringInfo inputBuffer, StringInfo outputBuffer,
						   CompressionType compressionType);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType);
extern char * CompressionTypeStr(CompressionType type);

/* cstore_metadata_tables.c */
extern void DeleteDataFileMetadataRowIfExists(Oid relfilenode);
extern void InitCStoreDataFileMetadata(Oid relfilenode, int blockRowCount, int
									   stripeRowCount, CompressionType compression);
extern void UpdateCStoreDataFileMetadata(Oid relfilenode, int blockRowCount, int
										 stripeRowCount, CompressionType compression);
extern DataFileMetadata * ReadDataFileMetadata(Oid relfilenode, bool missingOk);
extern uint64 GetHighestUsedAddress(Oid relfilenode);
extern StripeMetadata ReserveStripe(Relation rel, uint64 size,
									uint64 rowCount, uint64 columnCount,
									uint64 blockCount, uint64 blockRowCount);
extern void SaveStripeSkipList(Oid relfilenode, uint64 stripe,
							   StripeSkipList *stripeSkipList,
							   TupleDesc tupleDescriptor);
extern StripeSkipList * ReadStripeSkipList(Oid relfilenode, uint64 stripe,
										   TupleDesc tupleDescriptor,
										   uint32 blockCount);

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
	uint64 bytes_per_page = BLCKSZ - SizeOfPageHeaderData;
	SmgrAddr addr;

	addr.blockno = logicalOffset / bytes_per_page;
	addr.offset = SizeOfPageHeaderData + (logicalOffset % bytes_per_page);

	return addr;
}


/*
 * Map a physical page adnd offset address to a logical address.
 */
static inline uint64
smgr_to_logical(SmgrAddr addr)
{
	uint64 bytes_per_page = BLCKSZ - SizeOfPageHeaderData;
	return bytes_per_page * addr.blockno + addr.offset - SizeOfPageHeaderData;
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
