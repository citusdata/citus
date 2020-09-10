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

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/relcache.h"

/* Defines for valid option names */
#define OPTION_NAME_FILENAME "filename"
#define OPTION_NAME_COMPRESSION_TYPE "compression"
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_count"
#define OPTION_NAME_BLOCK_ROW_COUNT "block_row_count"

/* Default values for option parameters */
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_NONE
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_BLOCK_ROW_COUNT 10000

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
	char *filename;
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
	uint64 skipListLength;
	uint64 dataLength;
	uint64 footerLength;
	uint64 id;
} StripeMetadata;


/* TableMetadata represents the metadata of a cstore file. */
typedef struct TableMetadata
{
	List *stripeMetadataList;
	uint64 blockRowCount;
} TableMetadata;


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
 * ColumnBlockData represents a block of data in a column. valueArray stores
 * the values of data, and existsArray stores whether a value is present.
 * valueBuffer is used to store (uncompressed) serialized values
 * referenced by Datum's in valueArray. It is only used for by-reference Datum's.
 * There is a one-to-one correspondence between valueArray and existsArray.
 */
typedef struct ColumnBlockData
{
	bool *existsArray;
	Datum *valueArray;

	/* valueBuffer keeps actual data for type-by-reference datums from valueArray. */
	StringInfo valueBuffer;
} ColumnBlockData;


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


/*
 * StripeFooter represents a stripe's footer. In this footer, we keep three
 * arrays of sizes. The number of elements in each of the arrays is equal
 * to the number of columns.
 */
typedef struct StripeFooter
{
	uint32 columnCount;
	uint64 *skipListSizeArray;
	uint64 *existsSizeArray;
	uint64 *valueSizeArray;
} StripeFooter;


/* TableReadState represents state of a cstore file read operation. */
typedef struct TableReadState
{
	Oid relationId;

	FILE *tableFile;
	TableMetadata *tableMetadata;
	TupleDesc tupleDescriptor;

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
	ColumnBlockData **blockDataArray;
	int32 deserializedBlockIndex;
} TableReadState;


/* TableWriteState represents state of a cstore file write operation. */
typedef struct TableWriteState
{
	Oid relationId;
	FILE *tableFile;
	TableMetadata *tableMetadata;
	CompressionType compressionType;
	TupleDesc tupleDescriptor;
	FmgrInfo **comparisonFunctionArray;
	uint64 currentFileOffset;
	Relation relation;

	MemoryContext stripeWriteContext;
	uint64 currentStripeId;
	StripeBuffers *stripeBuffers;
	StripeSkipList *stripeSkipList;
	uint32 stripeMaxRowCount;
	ColumnBlockData **blockDataArray;

	/*
	 * compressionBuffer buffer is used as temporary storage during
	 * data value compression operation. It is kept here to minimize
	 * memory allocations. It lives in stripeWriteContext and gets
	 * deallocated when memory context is reset.
	 */
	StringInfo compressionBuffer;
} TableWriteState;

extern CompressionType ParseCompressionType(const char *compressionTypeString);
extern void InitializeCStoreTableFile(Oid relationId, Relation relation,
									  CStoreOptions *cstoreOptions);
extern void CreateCStoreDatabaseDirectory(Oid databaseOid);
extern void RemoveCStoreDatabaseDirectory(Oid databaseOid);
extern void DeleteCStoreTableFiles(char *filename);

/* Function declarations for writing to a cstore file */
extern TableWriteState * CStoreBeginWrite(Oid relationId,
										  const char *filename,
										  CompressionType compressionType,
										  uint64 stripeMaxRowCount,
										  uint32 blockRowCount,
										  TupleDesc tupleDescriptor);
extern void CStoreWriteRow(TableWriteState *state, Datum *columnValues,
						   bool *columnNulls);
extern void CStoreEndWrite(TableWriteState *state);

/* Function declarations for reading from a cstore file */
extern TableReadState * CStoreBeginRead(Oid relationId, const char *filename,
										TupleDesc tupleDescriptor,
										List *projectedColumnList, List *qualConditions);
extern bool CStoreReadFinished(TableReadState *state);
extern bool CStoreReadNextRow(TableReadState *state, Datum *columnValues,
							  bool *columnNulls);
extern void CStoreEndRead(TableReadState *state);

/* Function declarations for common functions */
extern FmgrInfo * GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId,
										int16 procedureId);
extern ColumnBlockData ** CreateEmptyBlockDataArray(uint32 columnCount, bool *columnMask,
													uint32 blockRowCount);
extern void FreeColumnBlockDataArray(ColumnBlockData **blockDataArray,
									 uint32 columnCount);
extern uint64 CStoreTableRowCount(Oid relid, const char *filename);
extern bool CompressBuffer(StringInfo inputBuffer, StringInfo outputBuffer,
						   CompressionType compressionType);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType);

/* cstore_metadata_tables.c */
extern void SaveStripeFooter(Oid relid, uint64 stripe, StripeFooter *footer);
extern StripeFooter * ReadStripeFooter(Oid relid, uint64 stripe, int relationColumnCount);
extern void InitCStoreTableMetadata(Oid relid, int blockRowCount);
extern void InsertStripeMetadataRow(Oid relid, StripeMetadata *stripe);
extern TableMetadata * ReadTableMetadata(Oid relid);

#endif /* CSTORE_H */
