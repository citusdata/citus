#include "postgres.h"

#include "fmgr.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"

#include "pg_version_compat.h"

#include "distributed/jsonbutils.h"
#include "distributed/metadata_cache.h"


/*
 * ExtractFieldJsonb gets value of fieldName from jsonbDoc and puts it
 * into result. If not found, returns false. Otherwise, returns true.
 * The field is returned as a Text* Datum if as_text is true, or a Jsonb*
 * Datum if as_text is false.
 */
static bool
ExtractFieldJsonb(Datum jsonbDoc, const char *fieldName, Datum *result, bool as_text)
{
	Datum pathArray[1] = { CStringGetTextDatum(fieldName) };
	bool pathNulls[1] = { false };
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;
	int dimensions[1] = { 1 };
	int lowerbounds[1] = { 1 };

	get_typlenbyvalalign(TEXTOID, &typeLength, &typeByValue, &typeAlignment);

	ArrayType *pathArrayObject = construct_md_array(pathArray, pathNulls, 1, dimensions,
													lowerbounds, TEXTOID, typeLength,
													typeByValue, typeAlignment);
	Datum pathDatum = PointerGetDatum(pathArrayObject);

	FmgrInfo fmgrInfo;

	if (as_text)
	{
		fmgr_info(JsonbExtractPathTextFuncId(), &fmgrInfo);
	}
	else
	{
		fmgr_info(JsonbExtractPathFuncId(), &fmgrInfo);
	}

	LOCAL_FCINFO(functionCallInfo, 2);
	InitFunctionCallInfoData(*functionCallInfo, &fmgrInfo, 2, DEFAULT_COLLATION_OID, NULL,
							 NULL);

	fcSetArg(functionCallInfo, 0, jsonbDoc);
	fcSetArg(functionCallInfo, 1, pathDatum);

	*result = FunctionCallInvoke(functionCallInfo);
	return !functionCallInfo->isnull;
}


/*
 * ExtractFieldBoolean gets value of fieldName from jsonbDoc, or returns
 * defaultValue if it doesn't exist.
 */
bool
ExtractFieldBoolean(Datum jsonbDoc, const char *fieldName, bool defaultValue)
{
	Datum jsonbDatum = 0;
	bool found = ExtractFieldJsonb(jsonbDoc, fieldName, &jsonbDatum, false);
	if (!found)
	{
		return defaultValue;
	}

	Datum boolDatum = DirectFunctionCall1(jsonb_bool, jsonbDatum);
	return DatumGetBool(boolDatum);
}


/*
 * ExtractFieldInt32 gets value of fieldName from jsonbDoc, or returns
 * defaultValue if it doesn't exist.
 */
int32
ExtractFieldInt32(Datum jsonbDoc, const char *fieldName, int32 defaultValue)
{
	Datum jsonbDatum = 0;
	bool found = ExtractFieldJsonb(jsonbDoc, fieldName, &jsonbDatum, false);
	if (!found)
	{
		return defaultValue;
	}

	Datum int32Datum = DirectFunctionCall1(jsonb_int4, jsonbDatum);
	return DatumGetInt32(int32Datum);
}


/*
 * ExtractFieldTextP gets value of fieldName as text* from jsonbDoc, or
 * returns NULL if it doesn't exist.
 */
text *
ExtractFieldTextP(Datum jsonbDoc, const char *fieldName)
{
	Datum jsonbDatum = 0;

	bool found = ExtractFieldJsonb(jsonbDoc, fieldName, &jsonbDatum, true);
	if (!found)
	{
		return NULL;
	}

	return DatumGetTextP(jsonbDatum);
}


/*
 * ExtractFieldJsonbDatum gets value of fieldName from jsonbDoc and puts it
 * into result. If not found, returns false. Otherwise, returns true.
 */
bool
ExtractFieldJsonbDatum(Datum jsonbDoc, const char *fieldName, Datum *result)
{
	return ExtractFieldJsonb(jsonbDoc, fieldName, result, false);
}
