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

#include "nodes/pg_list.h"


extern void ExtractColumnsOwningSequences(Oid relationId, List **columnNameList,
										  List **ownedSequenceIdList);


#endif /* CITUS_SEQUENCE_H */
