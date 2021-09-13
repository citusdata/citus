/*-------------------------------------------------------------------------
 *
 * columnar_metadata.h
 *
 * Type and function declarations for Columnar metadata.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_METADATA_H
#define COLUMNAR_METADATA_H

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
	uint32 chunkGroupRowCount;
	uint64 rowCount;
	uint64 id;
	uint64 firstRowNumber;

	/* see StripeWriteState */
	bool aborted;

	/*
	 * If write operation is in-progress (i.e. StripeWriteState returned
	 * STRIPE_WRITE_IN_PROGRESS), then insertedByCurrentXact is used to
	 * distinguish whether it's being written by current transaction or
	 * not.
	 */
	bool insertedByCurrentXact;
} StripeMetadata;

/*
 * EmptyStripeReservation represents information for an empty stripe
 * reservation.
 */
typedef struct EmptyStripeReservation
{
	uint64 stripeId;
	uint64 stripeFirstRowNumber;
} EmptyStripeReservation;

extern List * StripesForRelfilenode(RelFileNode relfilenode);
extern void ColumnarStorageUpdateIfNeeded(Relation rel, bool isUpgrade);

#endif /* COLUMNAR_METADATA_H */
