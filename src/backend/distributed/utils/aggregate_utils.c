#include "postgres.h"

#include "utils/fmgr.h"

PG_FUNCTION_INFO_V1(stypebox_serialize);
PG_FUNCTION_INFO_V1(stypebox_deserialize);
PG_FUNCTION_INFO_V1(stypebox_combine);
PG_FUNCTION_INFO_V1(worker_partial_agg_sfunc);
PG_FUNCTION_INFO_V1(worker_partial_agg_ffunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_sfunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_ffunc);

typedef struct StypeBox {
	Datum value;
	bool value_null;
	Oid agg;
} StypeBox;

/*
 * (box) -> bytea
 * return bytes(box.agg.name, box.agg.serial(box.value))
 */
Datum
stypebox_serialize(PG_FUNCTION_ARGS)
{
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
	Oid aggOid;
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
	// TODO
	// box1.agg = box1.agg.combine(box1.value, box2.value)
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
	FmgrInfo info;
	fmgr_info(box->agg, &info);
	FunctionCallInfo inner_fcinfo;
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
}

/*
 * (box, agg, valbytes) -> box
 */
Datum
coord_combine_agg_sfunc(PG_FUNCTION_ARGS)
{
}
