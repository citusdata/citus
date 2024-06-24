/*-------------------------------------------------------------------------
 *
 * executor_util_tuples.c
 *
 * Utility functions for handling tuples during remote execution.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "utils/lsyscache.h"

#include "distributed/executor_util.h"


/*
 * TupleDescGetAttBinaryInMetadata - Build an AttInMetadata structure based on
 * the supplied TupleDesc. AttInMetadata can be used in conjunction with
 * fmStringInfos containing binary encoded types to produce a properly formed
 * tuple.
 *
 * NOTE: This function is a copy of the PG function TupleDescGetAttInMetadata,
 * except that it uses getTypeBinaryInputInfo instead of getTypeInputInfo.
 */
AttInMetadata *
TupleDescGetAttBinaryInMetadata(TupleDesc tupdesc)
{
	int natts = tupdesc->natts;
	int i;
	Oid atttypeid;
	Oid attinfuncid;

	AttInMetadata *attinmeta = (AttInMetadata *) palloc(sizeof(AttInMetadata));

	/* "Bless" the tupledesc so that we can make rowtype datums with it */
	attinmeta->tupdesc = BlessTupleDesc(tupdesc);

	/*
	 * Gather info needed later to call the "in" function for each attribute
	 */
	FmgrInfo *attinfuncinfo = (FmgrInfo *) palloc0(natts * sizeof(FmgrInfo));
	Oid *attioparams = (Oid *) palloc0(natts * sizeof(Oid));
	int32 *atttypmods = (int32 *) palloc0(natts * sizeof(int32));

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Ignore dropped attributes */
		if (!att->attisdropped)
		{
			atttypeid = att->atttypid;
			getTypeBinaryInputInfo(atttypeid, &attinfuncid, &attioparams[i]);
			fmgr_info(attinfuncid, &attinfuncinfo[i]);
			atttypmods[i] = att->atttypmod;
		}
	}
	attinmeta->attinfuncs = attinfuncinfo;
	attinmeta->attioparams = attioparams;
	attinmeta->atttypmods = atttypmods;

	return attinmeta;
}


/*
 * BuildTupleFromBytes - build a HeapTuple given user data in binary form.
 * values is an array of StringInfos, one for each attribute of the return
 * tuple. A NULL StringInfo pointer indicates we want to create a NULL field.
 *
 * NOTE: This function is a copy of the PG function BuildTupleFromCStrings,
 * except that it uses ReceiveFunctionCall instead of InputFunctionCall.
 */
HeapTuple
BuildTupleFromBytes(AttInMetadata *attinmeta, fmStringInfo *values)
{
	TupleDesc tupdesc = attinmeta->tupdesc;
	int natts = tupdesc->natts;
	int i;

	Datum *dvalues = (Datum *) palloc(natts * sizeof(Datum));
	bool *nulls = (bool *) palloc(natts * sizeof(bool));

	/*
	 * Call the "in" function for each non-dropped attribute, even for nulls,
	 * to support domains.
	 */
	for (i = 0; i < natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			/* Non-dropped attributes */
			dvalues[i] = ReceiveFunctionCall(&attinmeta->attinfuncs[i],
											 values[i],
											 attinmeta->attioparams[i],
											 attinmeta->atttypmods[i]);
			if (values[i] != NULL)
			{
				nulls[i] = false;
			}
			else
			{
				nulls[i] = true;
			}
		}
		else
		{
			/* Handle dropped attributes by setting to NULL */
			dvalues[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	/*
	 * Form a tuple
	 */
	HeapTuple tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	/*
	 * Release locally palloc'd space.  XXX would probably be good to pfree
	 * values of pass-by-reference datums, as well.
	 */
	pfree(dvalues);
	pfree(nulls);

	return tuple;
}
