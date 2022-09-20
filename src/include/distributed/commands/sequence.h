/*-------------------------------------------------------------------------
 *
 * sequence.h
 *	  Functions for dealing with sequences
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_SEQUENCE_H
#define CITUS_SEQUENCE_H

#include "access/attnum.h"
#include "nodes/pg_list.h"


extern bool ColumnDefaultsToNextVal(Oid relationId, AttrNumber attrNumber);
extern void ExtractDefaultColumnsAndOwnedSequences(Oid relationId,
												   List **columnNameList,
												   List **ownedSequenceIdList);


#endif /* CITUS_SEQUENCE_H */
