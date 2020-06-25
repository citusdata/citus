/*-------------------------------------------------------------------------
 *
 * create_citus_local_table.h
 *
 * Declarations for public utility functions related with citus local
 * tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CREATE_CITUS_LOCAL_TABLE_H_
#define CREATE_CITUS_LOCAL_TABLE_H_

#include "postgres.h"

extern bool IsCitusLocalTable(Oid relationId);

#endif /* CREATE_CITUS_LOCAL_TABLE_H_ */
