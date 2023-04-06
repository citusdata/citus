/*-------------------------------------------------------------------------
 *
 * jsonbutils.h
 *
 * Declarations for public utility functions related to jsonb.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CITUS_JSONBUTILS_H
#define CITUS_JSONBUTILS_H

#include "postgres.h"

bool ExtractFieldJsonbDatum(Datum jsonbDoc, const char *fieldName, Datum *result);
text * ExtractFieldTextP(Datum jsonbDoc, const char *fieldName);
bool ExtractFieldBoolean(Datum jsonbDoc, const char *fieldName, bool defaultValue);
int32 ExtractFieldInt32(Datum jsonbDoc, const char *fieldName, int32 defaultValue);

#endif /* CITUS_JSONBUTILS_H */
