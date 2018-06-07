/*-------------------------------------------------------------------------
 * foreign_constraint.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FOREIGN_CONSTRAINT_H
#define FOREIGN_CONSTRAINT_H

#include "postgres.h"
#include "postgres_ext.h"
#include "utils/relcache.h"
#include "nodes/primnodes.h"

extern bool ConstraintIsAForeignKeyToReferenceTable(char *constraintName,
													Oid leftRelationId);
extern void ErrorIfUnsupportedForeignConstraint(Relation relation, char
												distributionMethod,
												Var *distributionColumn, uint32
												colocationId);
extern bool ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid
													  relationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationId);
extern bool TableReferenced(Oid relationId);

#endif
