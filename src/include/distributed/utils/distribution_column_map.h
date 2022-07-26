/*-------------------------------------------------------------------------
 *
 * distribution_column_map.h
 *	  Declarations for a relation OID to distribution column hash.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTION_COLUMN_HASH_H
#define DISTRIBUTION_COLUMN_HASH_H

#include "postgres.h"

#include "nodes/primnodes.h"
#include "utils/hsearch.h"


typedef HTAB DistributionColumnMap;


extern DistributionColumnMap * CreateDistributionColumnMap(void);
extern void AddDistributionColumnForRelation(DistributionColumnMap *distributionColumns,
											 Oid relationId,
											 char *distributionColumnName);
extern Var * GetDistributionColumnFromMap(DistributionColumnMap *distributionColumnMap,
										  Oid relationId);
extern Var * GetDistributionColumnWithOverrides(Oid relationId,
												DistributionColumnMap *overrides);

#endif   /* DISTRIBUTION_COLUMN_HASH_H */
