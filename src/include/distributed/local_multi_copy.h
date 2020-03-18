
#ifndef LOCAL_MULTI_COPY
#define LOCAL_MULTI_COPY

/*
 * LOCAL_COPY_FLUSH_THRESHOLD is the threshold for local copy to be flushed.
 * There will be one buffer for each local placement, when the buffer size
 * exceeds this threshold, it will be flushed.
 */
#define LOCAL_COPY_FLUSH_THRESHOLD (1 * 512 * 1024)

extern void WriteTupleToLocalShard(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
								   int64
								   shardId,
								   CopyOutState localCopyOutState);
extern void FinishLocalCopyToShard(CitusCopyDestReceiver *copyDest, int64 shardId,
								   CopyOutState localCopyOutState);

#endif /* LOCAL_MULTI_COPY */
