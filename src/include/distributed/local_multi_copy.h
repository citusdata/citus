
#ifndef LOCAL_MULTI_COPY
#define LOCAL_MULTI_COPY

/*
 * LocalCopyFlushThresholdByte is the threshold for local copy to be flushed.
 * There will be one buffer for each local placement, when the buffer size
 * exceeds this threshold, it will be flushed.
 *
 * Managed via GUC, the default is 512 kB.
 */
extern int LocalCopyFlushThresholdByte;

extern void WriteTupleToLocalShard(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
								   int64
								   shardId,
								   CopyOutState localCopyOutState);
extern void WriteTupleToLocalFile(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
								  int64 shardId, CopyOutState localFileCopyOutState,
								  FileCompat *fileCompat);
extern void FinishLocalCopyToShard(CitusCopyDestReceiver *copyDest, int64 shardId,
								   CopyOutState localCopyOutState);
extern void FinishLocalCopyToFile(CopyOutState localFileCopyOutState,
								  FileCompat *fileCompat);

#endif /* LOCAL_MULTI_COPY */
