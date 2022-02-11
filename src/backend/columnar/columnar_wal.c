/*-------------------------------------------------------------------------
 *
 * columnar_wal.c
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bufmask.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/rmgr.h"
#include "access/xlog_internal.h"
#include "catalog/catalog.h"
#include "columnar/columnar_wal.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "safe_lib.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

#define RM_COLUMNAR_ID 241
#define RM_COLUMNAR_NAME "columnar"

#define XLOG_COLUMNAR_PAGE_OVERWRITE 0
#define XLOG_COLUMNAR_PAGE_APPEND 1
#define XLOG_COLUMNAR_INSERT 2

typedef struct xl_columnar_insert
{
	RelFileNode		target_node;
	OffsetNumber	offnum;     /* inserted tuple's offset */
	uint16			t_infomask2;
	uint16			t_infomask;
	uint8			flags;
	uint8			t_hoff;
} xl_columnar_insert;

#define SizeOfColumnarInsert    (offsetof(xl_columnar_insert, flags) + sizeof(uint8))

static void columnar_redo_page_overwrite(XLogReaderState *record);
static void columnar_redo_page_append(XLogReaderState *record);
static void columnar_redo_insert(XLogReaderState *record);

static void columnar_redo(XLogReaderState *record);
static void columnar_desc(StringInfo buf, XLogReaderState *record);
static const char *columnar_identify(uint8 info);
static void columnar_startup(void);
static void columnar_cleanup(void);
static void columnar_mask(char *pagedata, BlockNumber blkno);
static void columnar_decode(struct LogicalDecodingContext *ctx,
							struct XLogRecordBuffer *buf);

static RmgrData ColumnarRmgr = {
	.rm_name = RM_COLUMNAR_NAME,
	.rm_redo = columnar_redo,
	.rm_desc = columnar_desc,
	.rm_identify = columnar_identify,
	.rm_startup = columnar_startup,
	.rm_cleanup = columnar_cleanup,
	.rm_mask = columnar_mask,
	.rm_decode = columnar_decode
};

void
columnar_wal_init()
{
	RegisterCustomRmgr(RM_COLUMNAR_ID, &ColumnarRmgr);
}

void
columnar_wal_page_overwrite(Relation rel, BlockNumber blockno, char *buf,
							uint32 len)
{
	XLogRecPtr recptr;
	Buffer buffer = ReadBuffer(rel, blockno);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	START_CRIT_SECTION();

	Page page = BufferGetPage(buffer);

	PageHeader phdr = (PageHeader) page;
	PageInit(page, BLCKSZ, 0);

	if (len > phdr->pd_upper - phdr->pd_lower)
	{
		elog(ERROR,
			 "write of columnar data of length %d exceeds page size on block %d of relation %d",
			 len, blockno, rel->rd_id);
	}

	memcpy_s(page + phdr->pd_lower, phdr->pd_upper - phdr->pd_lower, buf, len);
	phdr->pd_lower += len;

	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
	recptr = XLogInsert(RM_COLUMNAR_ID, XLOG_COLUMNAR_PAGE_OVERWRITE);
	PageSetLSN(page, recptr);

	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}

void
columnar_wal_page_append(Relation rel, BlockNumber blockno, uint32 offset,
						 char *buf, uint32 len)
{
	XLogRecPtr recptr;
	Buffer buffer = ReadBuffer(rel, blockno);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	START_CRIT_SECTION();

	Page page = BufferGetPage(buffer);

	PageHeader phdr = (PageHeader) page;
	if (PageIsNew(page))
	{
		PageInit(page, BLCKSZ, 0);
	}

	if (offset != phdr->pd_lower)
	{
		elog(ERROR,
			 "append of columnar data at offset %d does not match pd_lower %d of block %d relation %d",
			 offset, phdr->pd_lower, blockno, rel->rd_id);
	}

	if (len > phdr->pd_upper - offset)
	{
		elog(ERROR,
			 "append of columnar data of length %d to offset %d exceeds free space of block %d relation %d",
			 len, offset, blockno, rel->rd_id);
	}

	memcpy_s(page + phdr->pd_lower, phdr->pd_upper - phdr->pd_lower, buf, len);
	phdr->pd_lower += len;

	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
	recptr = XLogInsert(RM_COLUMNAR_ID, XLOG_COLUMNAR_PAGE_APPEND);
	PageSetLSN(page, recptr);

	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}

void
columnar_wal_insert(Relation rel, TupleTableSlot *slot, int options)
{
	bool        shouldFree = true;
	HeapTuple   heaptup = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	xl_columnar_insert xlrec;

	if (options & HEAP_INSERT_NO_LOGICAL)
	{
		return;
	}

	memcpy(&xlrec.target_node, &rel->rd_node, sizeof(RelFileNode));

	xlrec.flags = 0;
	xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;

	if (options & HEAP_INSERT_SPECULATIVE)
	{
		xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;
	}

	if (IsToastRelation(rel))
	{
		xlrec.flags |= XLH_INSERT_ON_TOAST_RELATION;
	}

	xlrec.t_infomask2 = heaptup->t_data->t_infomask2;
	xlrec.t_infomask = heaptup->t_data->t_infomask;
	xlrec.t_hoff = heaptup->t_data->t_hoff;

	XLogBeginInsert();

	XLogRegisterData((char *) &xlrec, SizeOfColumnarInsert);
	XLogRegisterData((char *) heaptup->t_data + SizeofHeapTupleHeader,
					 heaptup->t_len - SizeofHeapTupleHeader);

	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	/* not associated with any buffer, so ignore returned LSN */
	(void) XLogInsert(RM_COLUMNAR_ID, XLOG_COLUMNAR_INSERT);

	if (shouldFree)
	{
		pfree(heaptup);
	}
}

/*
 * Full page images are always forced, so this is a no-op.
 */
static void
columnar_redo_page_overwrite(XLogReaderState *record)
{
	return;
}

/*
 * Full page images are always forced, so this is a no-op.
 */
static void
columnar_redo_page_append(XLogReaderState *record)
{
	return;
}

/*
 * Columnar insert records are only for logical decoding. Nothing needs to
 * be done for redo.
 */
static void
columnar_redo_insert(XLogReaderState *record)
{
	return;
}

static void
columnar_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_COLUMNAR_PAGE_OVERWRITE:
			columnar_redo_page_overwrite(record);
			break;
		case XLOG_COLUMNAR_PAGE_APPEND:
			columnar_redo_page_append(record);
			break;
		case XLOG_COLUMNAR_INSERT:
			columnar_redo_insert(record);
			break;
		default:
			elog(PANIC, "columnar_redo: unknown op code %u", info);
	}
}

static void
columnar_desc(StringInfo buf, XLogReaderState *record)
{

}

static const char *
columnar_identify(uint8 info)
{
	return "";
}

static void
columnar_startup(void)
{
	elog(LOG, "columnar_rm_startup()");
}

static void
columnar_cleanup(void)
{
	elog(LOG, "columnar_rm_cleanup()");
}

static void
columnar_mask(char *pagedata, BlockNumber blkno)
{
	mask_page_lsn_and_checksum(pagedata);

	mask_unused_space(pagedata);
}

static void
columnar_decode(struct LogicalDecodingContext *ctx,
				struct XLogRecordBuffer *buf)
{
	Size		datalen;
	char	   *tupledata;
	Size		tuplelen;
	XLogReaderState *r = buf->record;
	xl_columnar_insert *xlrec;
	ReorderBufferChange *change;

	xlrec = (xl_columnar_insert *) XLogRecGetData(r);

	/*
	 * Ignore insert records without new tuples (this does happen when
	 * raw_heap_insert marks the TOAST record as HEAP_INSERT_NO_LOGICAL).
	 */
	if (!(xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE))
		return;

	/* only interested in our database */
	if (xlrec->target_node.dbNode != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */

	if (ctx->callbacks.filter_by_origin_cb != NULL &&
		!filter_by_origin_cb_wrapper(ctx, XLogRecGetOrigin(r)))
	{
		return;
	}

	change = ReorderBufferGetChange(ctx->reorder);
	if (!(xlrec->flags & XLH_INSERT_IS_SPECULATIVE))
		change->action = REORDER_BUFFER_CHANGE_INSERT;
	else
		change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT;
	change->origin_id = XLogRecGetOrigin(r);

	memcpy(&change->data.tp.relnode, &xlrec->target_node,
		   sizeof(RelFileNode));

	datalen = XLogRecGetDataLen(r);
	tupledata = XLogRecGetData(r) + SizeOfColumnarInsert;
	tuplelen = datalen - SizeOfColumnarInsert;

	change->data.tp.newtuple =
		ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

	ReorderBufferTupleBuf *newtuple = change->data.tp.newtuple;
	newtuple->tuple.t_len = tuplelen + SizeofHeapTupleHeader;
	HeapTupleHeader header = newtuple->tuple.t_data;
	ItemPointerSetInvalid(&newtuple->tuple.t_self);
	newtuple->tuple.t_tableOid = InvalidOid;
	memset(header, 0, SizeofHeapTupleHeader);
	memcpy(newtuple->tuple.t_data + SizeofHeapTupleHeader,
		   tupledata, tuplelen);

	header->t_infomask = xlrec->t_infomask;
	header->t_infomask2 = xlrec->t_infomask2;
	header->t_hoff = xlrec->t_hoff;

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change,
							 xlrec->flags & XLH_INSERT_ON_TOAST_RELATION);
}


