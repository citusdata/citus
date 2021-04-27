/*-------------------------------------------------------------------------
 *
 * columnar_storage.h
 *
 * Type and function declarations for storage of columnar data in blocks.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_STORAGE_H
#define COLUMNAR_STORAGE_H

#include "postgres.h"

#include "storage/smgr.h"
#include "utils/rel.h"


#define COLUMNAR_INVALID_ROW_NUMBER 0
#define COLUMNAR_FIRST_ROW_NUMBER 1


/*
 * Logical offsets never fall on the first two physical pages. See
 * comments in columnar_storage.c.
 */
#define ColumnarInvalidLogicalOffset 0
#define ColumnarFirstLogicalOffset ((BLCKSZ - SizeOfPageHeaderData) * 2)
#define ColumnarLogicalOffsetIsValid(X) ((X) >= ColumnarFirstLogicalOffset)


extern void ColumnarStorageInit(SMgrRelation srel, uint64 storageId);
extern bool ColumnarStorageIsCurrent(Relation rel);
extern void ColumnarStorageUpdateCurrent(Relation rel, bool upgrade,
										 uint64 reservedStripeId,
										 uint64 reservedRowNumber,
										 uint64 reservedOffset);

extern uint64 ColumnarStorageGetVersionMajor(Relation rel, bool force);
extern uint64 ColumnarStorageGetVersionMinor(Relation rel, bool force);
extern uint64 ColumnarStorageGetStorageId(Relation rel, bool force);
extern uint64 ColumnarStorageGetReservedStripeId(Relation rel, bool force);
extern uint64 ColumnarStorageGetReservedRowNumber(Relation rel, bool force);
extern uint64 ColumnarStorageGetReservedOffset(Relation rel, bool force);

extern uint64 ColumnarStorageReserveData(Relation rel, uint64 amount);
extern uint64 ColumnarStorageReserveStripe(Relation rel, uint64 nrows,
										   uint64 *firstRowNumber);

extern void ColumnarStorageRead(Relation rel, uint64 logicalOffset,
								char *data, uint32 amount);
extern void ColumnarStorageWrite(Relation rel, uint64 logicalOffset,
								 char *data, uint32 amount);
extern bool ColumnarStorageTruncate(Relation rel, uint64 newDataReservation);

#endif /* COLUMNAR_STORAGE_H */
