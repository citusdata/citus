/*
 * multi_partitioning_utils.h
 *	  Utility functions declarations for declarative partitioning
 *
 * Copyright (c) 2017, Citus Data, Inc.
 */
#ifndef MULTI_PARTITIONING_UTILS_H_
#define MULTI_PARTITIONING_UTILS_H_


#include "nodes/pg_list.h"


extern bool PartitionedTable(Oid relationId);
extern bool PartitionTable(Oid relationId);
extern bool IsChildTable(Oid relationId);
extern bool IsParentTable(Oid relationId);
extern List * PartitionList(Oid parentRelationId);
extern Oid PartitionParentOid(Oid partitionOid);
extern char * GenerateDetachPartitionCommand(Oid partitionTableId);
extern char * GenerateAlterTableAttachPartitionCommand(Oid partitionTableId);
extern char * GeneratePartitioningInformation(Oid tableId);


#endif /* MULTI_PARTITIONING_UTILS_H_ */
