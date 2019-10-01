/*-------------------------------------------------------------------------
 *
 * argutils.h
 *
 * Macros to help with argument parsing in UDFs.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

/*
 * PG_ENSURE_ARGNOTNULL ensures that a UDF argument is not NULL and throws an
 * error otherwise. This is useful for non STRICT UDFs where only some
 * arguments are allowed to be NULL.
 */
#define PG_ENSURE_ARGNOTNULL(argIndex, argName) \
	if (PG_ARGISNULL(argIndex)) \
	{ \
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), \
						errmsg("%s cannot be NULL", argName))); \
	}

/*
 * PG_GETARG_TEXT_TO_CSTRING is the same as PG_GETARG_TEXT_P, but instead of
 * text* it returns char*. Just like most other PG_GETARG_* macros this assumes
 * the argument is not NULL.
 */
#define PG_GETARG_TEXT_TO_CSTRING(argIndex) \
	text_to_cstring(PG_GETARG_TEXT_P(argIndex))

/*
 * PG_GETARG_TEXT_TO_CSTRING_OR_NULL is the same as PG_GETARG_TEXT_TO_CSTRING,
 * but it supports the case where the argument is NULL. In this case it will
 * return a NULL pointer.
 */
#define PG_GETARG_TEXT_TO_CSTRING_OR_NULL(argIndex) \
	PG_ARGISNULL(argIndex) ? NULL : PG_GETARG_TEXT_TO_CSTRING(argIndex)

/*
 * PG_GETARG_NAME_OR_NULL is the same as PG_GETARG_NAME, but it supports the
 * case where the argument is NULL. In this case it will return a NULL pointer.
 */
#define PG_GETARG_NAME_OR_NULL(argIndex) \
	PG_ARGISNULL(argIndex) ? NULL : PG_GETARG_NAME(argIndex)

/*
 * PG_GETARG_FLOAT4_OR is the same as PG_GETARG_FLOAT4, but it supports the
 * case where the argument is NULL. In that case it will return the provided
 * fallback.
 */
#define PG_GETARG_FLOAT4_OR_DEFAULT(argIndex, fallback) \
	PG_ARGISNULL(argIndex) ? (fallback) : PG_GETARG_FLOAT4(argIndex)
