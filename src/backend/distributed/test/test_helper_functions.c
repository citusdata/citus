/*-------------------------------------------------------------------------
 *
 * test/src/test_helper_functions.c
 *
 * This file contains helper functions used in many Citus tests.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <string.h>

#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "utils/array.h"
#include "utils/lsyscache.h"


/*
 * DatumArrayToArrayType converts the provided Datum array (of the specified
 * length and type) into an ArrayType suitable for returning from a UDF.
 */
ArrayType *
DatumArrayToArrayType(Datum *datumArray, int datumCount, Oid datumTypeId)
{
	ArrayType *arrayObject = NULL;
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	arrayObject = construct_array(datumArray, datumCount, datumTypeId,
								  typeLength, typeByValue, typeAlignment);

	return arrayObject;
}
