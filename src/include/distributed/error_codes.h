/*-------------------------------------------------------------------------
 *
 * error_codes.h
 *   Error codes that are specific to Citus
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_ERROR_CODES_H
#define CITUS_ERROR_CODES_H


#include "utils/elog.h"


#define ERRCODE_CITUS_INTERMEDIATE_RESULT_NOT_FOUND MAKE_SQLSTATE('C', 'I', 'I', 'N', 'F')


#endif /* CITUS_ERROR_CODES_H */
