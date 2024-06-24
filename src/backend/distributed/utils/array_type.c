/*-------------------------------------------------------------------------
 *
 * array_type.c
 *
 * Utility functions for dealing with array types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_version_compat.h"

#include "distributed/utils/array_type.h"


/*
 * DeconstructArrayObject takes in a single dimensional array, and deserializes
 * this array's members into an array of datum objects. The function then
 * returns this datum array.
 */
Datum *
DeconstructArrayObject(ArrayType *arrayObject)
{
	Datum *datumArray = NULL;
	bool *datumArrayNulls = NULL;
	int datumArrayLength = 0;

	bool typeByVal = false;
	char typeAlign = 0;
	int16 typeLength = 0;

	bool arrayHasNull = ARR_HASNULL(arrayObject);
	if (arrayHasNull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("worker array object cannot contain null values")));
	}

	Oid typeId = ARR_ELEMTYPE(arrayObject);
	get_typlenbyvalalign(typeId, &typeLength, &typeByVal, &typeAlign);

	deconstruct_array(arrayObject, typeId, typeLength, typeByVal, typeAlign,
					  &datumArray, &datumArrayNulls, &datumArrayLength);

	return datumArray;
}


/*
 * ArrayObjectCount takes in a single dimensional array, and returns the number
 * of elements in this array.
 */
int32
ArrayObjectCount(ArrayType *arrayObject)
{
	int32 dimensionCount = ARR_NDIM(arrayObject);
	int32 *dimensionLengthArray = ARR_DIMS(arrayObject);

	if (dimensionCount == 0)
	{
		return 0;
	}

	/* we currently allow split point arrays to have only one subarray */
	Assert(dimensionCount == 1);

	int32 arrayLength = ArrayGetNItems(dimensionCount, dimensionLengthArray);
	if (arrayLength <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						errmsg("worker array object cannot be empty")));
	}

	return arrayLength;
}


/*
 * DatumArrayToArrayType converts the provided Datum array (of the specified
 * length and type) into an ArrayType suitable for returning from a UDF.
 */
ArrayType *
DatumArrayToArrayType(Datum *datumArray, int datumCount, Oid datumTypeId)
{
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	ArrayType *arrayObject = construct_array(datumArray, datumCount, datumTypeId,
											 typeLength, typeByValue, typeAlignment);

	return arrayObject;
}


/*
 * Converts ArrayType to List.
 */
List *
IntegerArrayTypeToList(ArrayType *arrayObject)
{
	List *list = NULL;
	Datum *datumObjectArray = DeconstructArrayObject(arrayObject);
	int arrayObjectCount = ArrayObjectCount(arrayObject);

	for (int index = 0; index < arrayObjectCount; index++)
	{
		int32 intObject = DatumGetInt32(datumObjectArray[index]);
		list = lappend_int(list, intObject);
	}

	return list;
}


/*
 * Converts Text ArrayType to Integer List.
 */
extern List *
TextArrayTypeToIntegerList(ArrayType *arrayObject)
{
	List *list = NULL;
	Datum *datumObjectArray = DeconstructArrayObject(arrayObject);
	int arrayObjectCount = ArrayObjectCount(arrayObject);

	for (int index = 0; index < arrayObjectCount; index++)
	{
		char *intAsStr = text_to_cstring(DatumGetTextP(datumObjectArray[index]));
		list = lappend_int(list, pg_strtoint32(intAsStr));
	}

	return list;
}


/*
 * IntArrayToDatum
 *
 * Convert an integer array to the datum int array format
 * (currently used for nodes_involved in pg_dist_background_task)
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the int_array is empty.
 */
Datum
IntArrayToDatum(uint32 int_array_size, int int_array[])
{
	if (int_array_size == 0)
	{
		return PointerGetDatum(NULL);
	}

	ArrayBuildState *astate = NULL;
	for (int i = 0; i < int_array_size; i++)
	{
		Datum dvalue = Int32GetDatum(int_array[i]);
		bool disnull = false;
		Oid element_type = INT4OID;
		astate = accumArrayResult(astate, dvalue, disnull, element_type,
								  CurrentMemoryContext);
	}

	return makeArrayResult(astate, CurrentMemoryContext);
}
