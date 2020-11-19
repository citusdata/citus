/*-------------------------------------------------------------------------
 *
 * cstore_fdw.h
 *
 * Type and function declarations for CStore foreign data wrapper.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_FDW_H
#define CSTORE_FDW_H

#include "postgres.h"

#include "fmgr.h"

void cstore_fdw_init(void);
void cstore_fdw_finish(void);

/* event trigger function declarations */
extern Datum cstore_ddl_event_end_trigger(PG_FUNCTION_ARGS);

/* Function declarations for utility UDFs */
extern Datum cstore_table_size(PG_FUNCTION_ARGS);
extern Datum cstore_clean_table_resources(PG_FUNCTION_ARGS);

/* Function declarations for foreign data wrapper */
extern Datum cstore_fdw_handler(PG_FUNCTION_ARGS);
extern Datum cstore_fdw_validator(PG_FUNCTION_ARGS);

#endif   /* CSTORE_FDW_H */
