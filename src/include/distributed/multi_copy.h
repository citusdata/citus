/*-------------------------------------------------------------------------
 *
 * multi_copy.h
 *    Declarations for public functions and variables used in COPY for
 *    distributed tables.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_COPY_H
#define MULTI_COPY_H


#include "nodes/parsenodes.h"


/* config variable managed via guc.c */
extern int CopyTransactionManager;


static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/*
 * A smaller version of copy.c's CopyStateData, trimmed to the elements
 * necessary to copy out results. While it'd be a bit nicer to share code,
 * it'd require changing core postgres code.
 */
typedef struct CopyOutStateData
{
	StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
	                             * dest == COPY_NEW_FE in COPY FROM */
	int file_encoding;          /* file or remote side's character encoding */
	bool need_transcoding;              /* file encoding diff from server? */
	bool binary;                /* binary format? */
	char *null_print;           /* NULL marker string (server encoding!) */
	char *null_print_client;            /* same converted to file encoding */
	char *delim;                /* column delimiter (must be 1 byte) */

	MemoryContext rowcontext;   /* per-row evaluation context */
} CopyOutStateData;

typedef struct CopyOutStateData *CopyOutState;


/* function declarations for copying into a distributed table */
extern FmgrInfo * ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat);
extern void BuildCopyRowData(Datum *valueArray, bool *isNullArray, TupleDesc
							 rowDescriptor,
							 CopyOutState rowOutputState,
							 FmgrInfo *columnOutputFunctions);
extern void BuildCopyBinaryHeaders(CopyOutState headerOutputState);
extern void BuildCopyBinaryFooters(CopyOutState footerOutputState);
extern void CitusCopyFrom(CopyStmt *copyStatement, char *completionTag);


#endif /* MULTI_COPY_H */
