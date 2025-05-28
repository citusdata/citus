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
extern Var * BuildDistributionKeyFromColumnName(Oid relationId,
												char *columnName,
												LOCKMODE lockMode);
extern char * ColumnToColumnName(Oid relationId, Node *columnNode);
extern Oid ColumnTypeIdForRelationColumnName(Oid relationId, char *columnName);
extern void EnsureValidDistributionColumn(Oid relationId, char *columnName);
extern AttrNumber GetAttrNumForMatchingColumn(RangeTblEntry *rteTarget, Oid relid, Var *var);

#endif   /* DISTRIBUTION_COLUMN_H */
