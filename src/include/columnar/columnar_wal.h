/*-------------------------------------------------------------------------
 *
 * columnar_wal.h
 *
 * Type and function declarations for Columnar
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_WAL_H
#define COLUMNAR_WAL_H

extern void columnar_wal_init(void);
extern void columnar_wal_page_overwrite(Relation rel, BlockNumber blockno,
										char *buf, uint32 len);
extern void columnar_wal_page_append(Relation rel, BlockNumber blockno,
									 uint32 offset, char *buf, uint32 len);
extern void columnar_wal_insert(Relation rel, TupleTableSlot *slot,
								int options);

#endif /* COLUMNAR_WAL_H */
