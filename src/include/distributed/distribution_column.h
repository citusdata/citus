/*-------------------------------------------------------------------------
 *
 * distribution_column.h
 *	  Type and function declarations used for handling the distribution
 *    column of distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTION_COLUMN_H
#define DISTRIBUTION_COLUMN_H


#include "utils/rel.h"


/* Remaining metadata utility functions  */
extern Var * FindColumnWithNameOnTargetRelation(Oid sourceRelationId,
												char *sourceColumnName,
												Oid targetRelationId);
extern Var * BuildDistributionKeyFromColumnName(Relation distributedRelation,
												char *columnName);
extern char * ColumnToColumnName(Oid relationId, char *columnNodeString);

#endif   /* DISTRIBUTION_COLUMN_H */
