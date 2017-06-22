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


#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"


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

/* struct type to keep both hostname and port */
typedef struct NodeAddress
{
	char *nodeName;
	int32 nodePort;
} NodeAddress;

/* CopyDestReceiver can be used to stream results into a distributed table */
typedef struct CitusCopyDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* relation and columns to which to copy */
	Oid distributedRelationId;
	List *columnNameList;
	int partitionColumnIndex;

	/* distributed table metadata */
	DistTableCacheEntry *tableMetadata;

	/* open relation handle */
	Relation distributedRelation;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/* MemoryContext for DestReceiver session */
	MemoryContext memoryContext;

	/* template for COPY statement to send to workers */
	CopyStmt *copyStatement;

	/* cached shard metadata for pruning */
	HTAB *shardConnectionHash;
	bool stopOnFailure;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

	/* number of tuples sent */
	int64 tuplesSent;
} CitusCopyDestReceiver;


/* function declarations for copying into a distributed table */
extern CitusCopyDestReceiver * CreateCitusCopyDestReceiver(Oid relationId,
														   List *columnNameList,
														   EState *executorState,
														   bool stopOnFailure);
extern FmgrInfo * ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat);
extern void AppendCopyRowData(Datum *valueArray, bool *isNullArray,
							  TupleDesc rowDescriptor,
							  CopyOutState rowOutputState,
							  FmgrInfo *columnOutputFunctions);
extern void AppendCopyBinaryHeaders(CopyOutState headerOutputState);
extern void AppendCopyBinaryFooters(CopyOutState footerOutputState);
extern void CitusCopyFrom(CopyStmt *copyStatement, char *completionTag);
extern bool IsCopyFromWorker(CopyStmt *copyStatement);
extern NodeAddress * MasterNodeAddress(CopyStmt *copyStatement);


#endif /* MULTI_COPY_H */
