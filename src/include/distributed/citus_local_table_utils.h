/*-------------------------------------------------------------------------
 *
 * citus_local_table_utils.h
 *
 * Declarations for public utility functions related with citus local
 * tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_LOCAL_TABLE_UTILS_H_
#define CITUS_LOCAL_TABLE_UTILS_H_

#include "postgres.h"

extern bool IsCitusLocalTable(Oid relationId);

#endif /* CITUS_LOCAL_TABLE_UTILS_H_ */
