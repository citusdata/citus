#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "utils/fmgr.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(stypebox_serialize);
PG_FUNCTION_INFO_V1(stypebox_deserialize);
PG_FUNCTION_INFO_V1(stypebox_combine);
PG_FUNCTION_INFO_V1(worker_partial_agg_sfunc);
PG_FUNCTION_INFO_V1(worker_partial_agg_ffunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_sfunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_ffunc);

typedef struct StypeBox {
	Datum value;
	Oid agg;
	bool value_null;
} StypeBox;

static Form_pg_aggregate get_aggform(Oid aggfnoid);
static Form_pg_proc get_procform(Oid aggfnoid);

static Form_pg_aggregate
get_aggform(Oid aggfnoid)
{
	/* Fetch the pg_aggregate row */
	HeapTuple tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for aggregate %u",
			 aggfnoid);
	return (Form_pg_aggregate) GETSTRUCT(tuple);
}

static Form_pg_proc
get_procform(Oid fnoid)
{
	Form_pg_proc = SearchSysCache1(PROCID, ObjectIdGetDatum(fnoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u",
			 fnoid);
	return (Form_pg_proc) GETSTRUCT(tuple);
}
/*
 * (box) -> bytea
 * return bytes(box.agg.oid, box.agg.serial(box.value))
 */
Datum
stypebox_serialize(PG_FUNCTION_ARGS)
{
	StypeBox *box = PG_GETARG_POINTER(0);
	Form_pg_aggregate aggform = get_aggform(box->agg);
	// TODO return null if box null?
	byteap *valbytes = DatumGetByteaPP(DirectFunctionCall1(aggform->serialfunc, box->value));
	byteap *realbytes = palloc(VARSIZE(valbytes) + sizeof(Oid));
	SET_VARSIZE(realbytes, VARSIZE(valbytes) + sizeof(Oid));
	memcpy(VARDATA(realbytes), &box->agg, sizeof(Oid));
	memcpy(VARDATA(realbytes) + sizeof(Oid), VARDATA(valbytes), VARSIZE(valbytes) - VARHDRSZ);
	pfree(valbytes); // TODO I get to free this right?
	PG_RETURN_BYTEA_P(valbytes);
}

/*
 * (bytea, internal) -> box
 * box->agg = readagg(bytea)
 * box->value = agg.deserial(readrest(bytea))
 * return box
 */
Datum
stypebox_deserialize(PG_FUNCTION_ARGS)
{
	StypeBox *box;
	byteap *bytes = PG_GETARG_BYTEA_PP(0);
	byteap *inner_bytes = PG_GETARG_BYTEA_P_SLICE(0, sizeof(Oid), VARSIZE(bytes) - sizeof(Oid))
	Oid agg;
	Form_pg_aggregate aggform;
	memcpy(&agg, VARDATA(bytes), sizeof(Oid));
	aggform = get_aggform(agg);
	// Can deserialize be called with NULL?

	box = palloc(sizeof(StypeBox));
	box->agg = agg;
	box->value = DirectFunctionCall2(aggform->deserialfunc, inner_bytes, PG_GETARG_DATUM(1));
	box->null_value = false;
	PG_RETURN_POINTER(box);
}

/*
 * (box1, box2) -> box
 * box1.value = box.agg.combine(box1.value, box2.value)
 * return box
 */
Datum
stypebox_combine(PG_FUNCTION_ARGS)
{
	StypeBox *box1 = NULL;
	StypeBox *box2 = NULL;
	FunctionCallInfo inner_fcinfo;
	Oid aggOid;
	Form_pg_aggregate aggform;
	Form_pg_proc combineform;

	if (!PG_ISARGNULL(0))
	{
		box1 = PG_GETARG_POINTER(0);
	}
	if (!PG_ISARGNULL(1))
	{
		box2 = PG_GETARG_POINTER(1);
	}

	if (box1 == NULL)
	{
		if (box2 == NULL)
		{
			PG_RETURN_NULL();
		}
		box1 = palloc(sizeof(StypeBox));
		box1->value = (Datum) 0;
		box1->value_null = true;
		box1->agg = box2->agg;
	}

	aggform = get_aggform(box->agg);
	combineform = get_procform(aggform->combinefn);

	// TODO respect strictness
	Assert(IsValidOid(aggform->combineefn));

	if (combineform->proisstrict)
	{
		if (box1->value_null)
		{
			if (box2->value_null)
			{
				PG_RETURN_NULL();
			}
			PG_RETURN_DATUM(box2->value);
		}
		if (box2->value_null)
		{
			PG_RETURN_DATUM(box1->value);
		}
	}

	InitFunctionCallInfoData(&inner_fcinfo, &info, fcinfo->nargs - 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	inner_fcinfo.arg[0] = box1->value;
	inner_fcinfo.argnull[0] = box1->value_null;
	inner_fcinfo.arg[1] = box2->value;
	inner_fcinfo.argnull[1] = box2->value_null;
	// TODO Deal with memory management juggling (see executor/nodeAgg)
	box1->value = FunctionCallInvoke(inner_fcinfo);
	box1->value_null = inner_fcinfo.isnull;
	PG_RETURN_POINTER(box1);
}

/*
 * (box, agg, ...) -> box
 * box.agg = agg;
 * box.value = agg.sfunc(box.value, ...);
 * return box
 */
Datum
worker_partial_agg_sfunc(PG_FUNCTION_ARGS)
{
	StypeBox *box;
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	int i;
	if (PG_ARGISNULL(0)) {
		box = palloc(sizeof(StypeBox));
		box->agg = PG_GETARG_OID(1);
		box->value = (Datum) 0;
		box->value_null = true;
	} else {
		box = PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}
	fmgr_info(box->agg, &info);
	InitFunctionCallInfoData(&inner_fcinfo, &info, fcinfo->nargs - 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	// TODO if strict, deal with it
	// Deal with memory management juggling (see executor/nodeAgg)
	inner_fcinfo.arg[0] = box->value;
	inner_fcinfo.argnull[0] = box->value_null;
	memcpy(&inner_fcinfo.arg[1], &fcinfo.arg[2], sizeof(Datum) * (inner_fcinfo.nargs - 1));
	memcpy(&inner_fcinfo.argnull[1], &fcinfo.argnull[2], sizeof(bool) * (inner_fcinfo.nargs - 1));
	box->value = FunctionCallInvoke(inner_fcinfo);
	box->value_null = inner_fcinfo.isnull;
	PG_RETURN_POINTER(box);
}

/*
 * (box) -> box.agg.stype
 * return box.agg.serialize(box.value)
 */
Datum
worker_partial_agg_ffunc(PG_FUNCTION_ARGS)
{
	StypeBox *box = PG_GETARG_POINTER(0);
	Form_pg_aggregate aggform = get_aggform(box->agg);
	PG_RETURN_DATUM(DirectFunctionCall1(aggform->serialfunc, box->value));
}

/*
 * (box, agg, valbytes) -> box
 * box->agg = agg
 * box->value = agg.sfunc(box->value, agg.deserialize(valbytes))
 * return box
 */
Datum
coord_combine_agg_sfunc(PG_FUNCTION_ARGS)
{
	// TODO
}

/*
 * box -> fval
 * return box.agg.ffunc(box.value)
 */
Datum
coord_combine_agg_ffunc(PG_FUNCTION_ARGS)
{
	StypeBox = PG_GETARG_POINTER(0);
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	Form_pg_aggregate aggform = get_aggform(box->agg);
	Form_pg_proc ffuncform;

	if (!IsValidOid(aggform->aggfinalfn))
	{
		if (box->value_null) {
			return NULL;
		}
		PG_RETURN_DATUM(box->value);
	}

	ffuncform = get_aggform(aggform->aggfinalfn);
	// TODO FINALFUNC EXTRA & stuff
	fmgr_info(aggform->aggfinalfn, &info);
	InitFunctionCallInfoData(&inner_fcinfo, &info, fcinfo->nargs - 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);

}

