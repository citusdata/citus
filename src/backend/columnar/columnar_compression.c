/*-------------------------------------------------------------------------
 *
 * columnar_compression.c
 *
 * This file contains compression/decompression functions definitions
 * used for columnar.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_lzcompress.h"
#include "lib/stringinfo.h"

#include "citus_version.h"
#include "pg_version_constants.h"

#include "columnar/columnar_compression.h"

#if HAVE_CITUS_LIBLZ4
#include <lz4.h>
#endif

#if PG_VERSION_NUM >= PG_VERSION_16
#include "varatt.h"
#endif

#if HAVE_LIBZSTD
#include <zstd.h>
#endif

/*
 *	The information at the start of the compressed data. This decription is taken
 *	from pg_lzcompress in pre-9.5 version of PostgreSQL.
 */
typedef struct ColumnarCompressHeader
{
	int32 vl_len_;              /* varlena header (do not touch directly!) */
	int32 rawsize;
} ColumnarCompressHeader;

/*
 * Utilities for manipulation of header information for compressed data
 */

#define COLUMNAR_COMPRESS_HDRSZ ((int32) sizeof(ColumnarCompressHeader))
#define COLUMNAR_COMPRESS_RAWSIZE(ptr) (((ColumnarCompressHeader *) (ptr))->rawsize)
#define COLUMNAR_COMPRESS_RAWDATA(ptr) (((char *) (ptr)) + COLUMNAR_COMPRESS_HDRSZ)
#define COLUMNAR_COMPRESS_SET_RAWSIZE(ptr, \
									  len) (((ColumnarCompressHeader *) (ptr))->rawsize = \
												(len))


/*
 * CompressBuffer compresses the given buffer with the given compression type
 * outputBuffer enlarged to contain compressed data. The function returns true
 * if compression is done, returns false if compression is not done.
 * outputBuffer is valid only if the function returns true.
 */
bool
CompressBuffer(StringInfo inputBuffer,
			   StringInfo outputBuffer,
			   CompressionType compressionType,
			   int compressionLevel)
{
	switch (compressionType)
	{
#if HAVE_CITUS_LIBLZ4
		case COMPRESSION_LZ4:
		{
			int maximumLength = LZ4_compressBound(inputBuffer->len);

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			int compressedSize = LZ4_compress_default(inputBuffer->data,
													  outputBuffer->data,
													  inputBuffer->len, maximumLength);
			if (compressedSize <= 0)
			{
				elog(DEBUG1,
					 "failure in LZ4_compress_default, input size=%d, output size=%d",
					 inputBuffer->len, maximumLength);
				return false;
			}

			elog(DEBUG1, "compressed %d bytes to %d bytes", inputBuffer->len,
				 compressedSize);

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

#if HAVE_LIBZSTD
		case COMPRESSION_ZSTD:
		{
			int maximumLength = ZSTD_compressBound(inputBuffer->len);

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			size_t compressedSize = ZSTD_compress(outputBuffer->data,
												  outputBuffer->maxlen,
												  inputBuffer->data,
												  inputBuffer->len,
												  compressionLevel);

			if (ZSTD_isError(compressedSize))
			{
				ereport(WARNING, (errmsg("zstd compression failed"),
								  (errdetail("%s", ZSTD_getErrorName(compressedSize)))));
				return false;
			}

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

		case COMPRESSION_PG_LZ:
		{
			uint64 maximumLength = PGLZ_MAX_OUTPUT(inputBuffer->len) +
								   COLUMNAR_COMPRESS_HDRSZ;
			bool compressionResult = false;

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			int32 compressedByteCount = pglz_compress((const char *) inputBuffer->data,
													  inputBuffer->len,
													  COLUMNAR_COMPRESS_RAWDATA(
														  outputBuffer->data),
													  PGLZ_strategy_always);
			if (compressedByteCount >= 0)
			{
				COLUMNAR_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
				SET_VARSIZE_COMPRESSED(outputBuffer->data,
									   compressedByteCount + COLUMNAR_COMPRESS_HDRSZ);
				compressionResult = true;
			}

			if (compressionResult)
			{
				outputBuffer->len = VARSIZE(outputBuffer->data);
			}

			return compressionResult;
		}

		default:
		{
			return false;
		}
	}
}


/*
 * DecompressBuffer decompresses the given buffer with the given compression
 * type. This function returns the buffer as-is when no compression is applied.
 */
StringInfo
DecompressBuffer(StringInfo buffer,
				 CompressionType compressionType,
				 uint64 decompressedSize)
{
	switch (compressionType)
	{
		case COMPRESSION_NONE:
		{
			return buffer;
		}

#if HAVE_CITUS_LIBLZ4
		case COMPRESSION_LZ4:
		{
			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			int lz4DecompressSize = LZ4_decompress_safe(buffer->data,
														decompressedBuffer->data,
														buffer->len,
														decompressedSize);

			if (lz4DecompressSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("Expected %lu bytes, but received %d bytes",
										  decompressedSize, lz4DecompressSize)));
			}

			decompressedBuffer->len = decompressedSize;

			return decompressedBuffer;
		}
#endif

#if HAVE_LIBZSTD
		case COMPRESSION_ZSTD:
		{
			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			size_t zstdDecompressSize = ZSTD_decompress(decompressedBuffer->data,
														decompressedSize,
														buffer->data,
														buffer->len);
			if (ZSTD_isError(zstdDecompressSize))
			{
				ereport(ERROR, (errmsg("zstd decompression failed"),
								(errdetail("%s", ZSTD_getErrorName(
											   zstdDecompressSize)))));
			}

			if (zstdDecompressSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("unexpected decompressed size"),
								errdetail("Expected %ld, received %ld", decompressedSize,
										  zstdDecompressSize)));
			}

			decompressedBuffer->len = decompressedSize;

			return decompressedBuffer;
		}
#endif

		case COMPRESSION_PG_LZ:
		{
			uint32 compressedDataSize = VARSIZE(buffer->data) - COLUMNAR_COMPRESS_HDRSZ;
			uint32 decompressedDataSize = COLUMNAR_COMPRESS_RAWSIZE(buffer->data);

			if (compressedDataSize + COLUMNAR_COMPRESS_HDRSZ != buffer->len)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("Expected %u bytes, but received %u bytes",
										  compressedDataSize, buffer->len)));
			}

			char *decompressedData = palloc0(decompressedDataSize);

			int32 decompressedByteCount = pglz_decompress(COLUMNAR_COMPRESS_RAWDATA(
															  buffer->data),
														  compressedDataSize,
														  decompressedData,
														  decompressedDataSize, true);

			if (decompressedByteCount < 0)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("compressed data is corrupted")));
			}

			StringInfo decompressedBuffer = palloc0(sizeof(StringInfoData));
			decompressedBuffer->data = decompressedData;
			decompressedBuffer->len = decompressedDataSize;
			decompressedBuffer->maxlen = decompressedDataSize;

			return decompressedBuffer;
		}

		default:
		{
			ereport(ERROR, (errmsg("unexpected compression type: %d", compressionType)));
		}
	}
}
