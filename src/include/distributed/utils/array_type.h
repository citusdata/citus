/*-------------------------------------------------------------------------
 *
 * array_type.h
 *	  Utility functions for dealing with array types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_ARRAY_TYPE_H
#define CITUS_ARRAY_TYPE_H

#include "postgres.h"

#include "utils/array.h"


extern Datum * DeconstructArrayObject(ArrayType *arrayObject);
extern int32 ArrayObjectCount(ArrayType *arrayObject);
extern ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);
extern List * IntegerArrayTypeToList(ArrayType *arrayObject);
extern List * TextArrayTypeToIntegerList(ArrayType *arrayObject);
extern Datum IntArrayToDatum(uint32 int_array_size, int int_array[]);

#endif   /* CITUS_ARRAY_TYPE_H */
