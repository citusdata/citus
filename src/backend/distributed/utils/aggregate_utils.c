#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "utils/cache/lsyscache.h"
#include "utils/fmgr.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(stypebox_serialize);
PG_FUNCTION_INFO_V1(stypebox_deserialize);
PG_FUNCTION_INFO_V1(stypebox_combine);
PG_FUNCTION_INFO_V1(worker_partial_agg_sfunc);
PG_FUNCTION_INFO_V1(worker_partial_agg_ffunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_sfunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_ffunc);

// TODO nodeAgg seems to decide to use serial/deserial based on stype == internal
//      Preferably we should match that logic, instead of checking serial/deserial oids

typedef struct StypeBox {
	Datum value;
	Oid agg;
	bool value_null;
} StypeBox;

static HeapTuple *get_aggform(Oid oid, Form_pg_aggregate *form);
static HeapTuple *get_procform(Oid oid, Form_pg_proc *form);
static HeapTuple *get_typeform(Oid oid, Form_pg_type *form);

static HeapTuple *
get_aggform(Oid oid, Form_pg_aggregate *form)
{
	HeapTuple tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for aggregate %u", oid);
	*form = (Form_pg_aggregate) GETSTRUCT(tuple);
	return tuple;
}

static HeapTuple *
get_procform(Oid oid)
{
	HeapTuple tuple = SearchSysCache1(PROCID, ObjectIdGetDatum(fnoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", oid);
	*form = (Form_pg_proc) GETSTRUCT(tuple);
	return tuple;
}

static HeapTuple *
get_typeform(Oid oid)
{
	HeapTuple tuple = SearchSysCache1(TYPEID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", oid);
	*form = (Form_pg_type) GETSTRUCT(tuple);
	return tuple;
}

/*
 * (box) -> bytea
 * return bytes(box.agg.oid, box.agg.serial(box.value))
 */
Datum
stypebox_serialize(PG_FUNCTION_ARGS)
{
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	StypeBox *box = PG_GETARG_POINTER(0);
	HeapTuple *aggtuple;
	HeapTuple *transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	byteap *valbytes;
	byteap *realbytes;
	Oid serial;
	Oid transtype;
	Size valbyteslen_exhdr;
	Size realbyteslen;
	Datum result;

	aggtuple = get_aggform(box->agg, &aggform);
	serial = aggform->serialfunc;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	if (!IsValidOid(serial))
	{
		// TODO do we have to fallback to output/receive if not set?
		// ie is it possible for send/recv to be unset?
		transtypetuple = get_typeform(transtype, &transtypeform);
		serial = transtypeform->typsend;
		ReleaseSysCache(transtypetuple);
	}

	fmgr_info(serial, &info);
	if (info->fn_strict && box->value_null)
	{
		valbytes = NULL;
		valbyteslen_exhdr = 0;
	}
	else
	{
		InitFunctionCallInfoData(&inner_fcinfo, &info, 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
		inner_fcinfo.arg[0] = box->value;
		inner_fcinfo.argnull[0] = box->value_null;
		result = FunctionCallInvoke(&inner_fcinfo);
		if (inner_fcinfo.isnull)
		{
			valbytes = NULL;
			valbyteslen_exhdr = 0;
		}
		else
		{
			valbytes = DatumGetByteaPP(result);
			valbyteslen_exhdr = VARSIZE(valbytes) - VARHDRSZ;
		}
	}

	realbyteslen = VARHDRSZ + valbyteslen_exhdr + sizeof(Oid) + sizeof(bool);
	realbytes = palloc(realbyteslen);
	SET_VARSIZE(realbytes, realbyteslen);
	memcpy(VARDATA(realbytes), &box->agg, sizeof(Oid));
	memcpy(VARDATA(realbytes) + sizeof(Oid), &box->value_null, sizeof(bool));
	if (valbytes != NULL)
	{
		memcpy(VARDATA(realbytes) + sizeof(Oid) + sizeof(bool),
			   VARDATA(valbytes),
			   valbyteslen_exhdr);
		pfree(valbytes); // TODO I get to free this right?
	}

	PG_RETURN_BYTEA_P(valbytes);
}

/*
 * (bytea, internal) -> box
 * box->agg = readagg(bytea)
 * box->value_null = readbool(bytea)
 * if (!box->value_null) box->value = agg.deserial(readrest(bytea))
 * return box
 */
Datum
stypebox_deserialize(PG_FUNCTION_ARGS)
{
	StypeBox *box;
	byteap *bytes = PG_GETARG_BYTEA_PP(0);
	byteap *inner_bytes;
	Oid agg;
	HeapTuple *aggtuple;
	HeapTuple *transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	Oid deserial;
	Oid transtype;
	Oid ioparam;
	Oid recv;
	StringInfoData buf;
	bool value_null;

	memcpy(&agg, VARDATA(bytes), sizeof(Oid));
	memcpy(&value_null, VARDATA(bytes) + sizeof(Oid), sizeof(bool));

	box = palloc(sizeof(StypeBox));
	box->agg = agg;
	if (value_null)
	{
		box->value = (Datum) 0;
		box->value_null = true;
		PG_RETURN_POINTER(box);
	}

	aggtuple = get_aggform(agg, &aggform);
	deserial = aggform->deserialfunc;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	if (aggform->deserialfunc) {
		inner_bytes = PG_GETARG_BYTEA_P_SLICE(0, sizeof(Oid), VARSIZE(bytes) - sizeof(Oid))
		box->value = DirectFunctionCall2(deserial, inner_bytes, PG_GETARG_DATUM(1));
		box->null_value = false;
	}
	else
	{
		transtypetuple = get_typeform(transtype, &transtypeform);
		ioparam = getTypeIOParam(transtypetuple);
		recv = transtypeform->typreceive;
		ReleaseSysCache(transtypetuple);

		initStringInfo(&buf);
		appendBinaryStringInfo(buf,
							   VARDATA(realbytes) + sizeof(Oid) + sizeof(bool),
							   VARSIZE(realbytes) - VARHDRSZ - sizeof(Oid) - sizeof(bool));

		box->value = OidReceiveFunctionCall(recv, &buf, ioparam, -1);
		box->value_null = value_null;
	}

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
	FmgrInfo info;
	Oid aggOid;
	HeapTuple *aggtuple;
	Form_pg_aggregate aggform;

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

	aggtuple = get_aggform(box1->agg, &aggform);
	Assert(IsValidOid(aggform->combineefn));
	combine = aggform->combinefn;
	ReleaseSysCache(aggtuple);

	fmgr_info(combine, &info);

	if (info.fn_strict)
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

	InitFunctionCallInfoData(&inner_fcinfo, &info, 2, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	inner_fcinfo.arg[0] = box1->value;
	inner_fcinfo.argnull[0] = box1->value_null;
	inner_fcinfo.arg[1] = box2->value;
	inner_fcinfo.argnull[1] = box2->value_null;
	// TODO Deal with memory management juggling (see executor/nodeAgg)
	box1->value = FunctionCallInvoke(&inner_fcinfo);
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
	}
	else
	{
		box = PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}
	fmgr_info(box->agg, &info);
	InitFunctionCallInfoData(&inner_fcinfo, &info, fcinfo->nargs - 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	if (info.flinfo->fn_strict) {
		if (box->value_null)
		{
			PG_RETURN_NULL();
		}
		for (i = 2; i<PG_NARGS(); i++)
		{
			if (PG_ARGISNULL(i))
			{
				PG_RETURN_NULL();
			}
		}
	}
	// Deal with memory management juggling (see executor/nodeAgg)
	inner_fcinfo.arg[0] = box->value;
	inner_fcinfo.argnull[0] = box->value_null;
	memcpy(&inner_fcinfo.arg[1], &fcinfo.arg[2], sizeof(Datum) * (inner_fcinfo.nargs - 1));
	memcpy(&inner_fcinfo.argnull[1], &fcinfo.argnull[2], sizeof(bool) * (inner_fcinfo.nargs - 1));
	box->value = FunctionCallInvoke(&inner_fcinfo);
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
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	StypeBox *box = PG_GETARG_POINTER(0);
	HeapTuple *aggtuple;
	Form_pg_aggregate aggform;
	Oid serial;
	Datum result;

	aggtuple = get_aggform(box->agg);
	serial = aggform->serialfunc;
	ReleaseSysCache(aggtuple);

	if (IsValidOid(serial))
	{
		fmgr_info(serial, &info);
		if (info->fn_strict && box->value_null)
		{
			PG_RETURN_NULL();
		}
		InitFunctionCallInfoData(&inner_fcinfo, &info, 1, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
		inner_fcinfo.arg[0] = box->value;
		inner_fcinfo.argnull[0] = box->value_null;
		result = FunctionCallInvoke(&inner_fcinfo);
		if (inner_fcinfo.isnull)
		{
			PG_RETURN_NULL();
		}
		PG_RETURN_DATUM(result);
	}
	else
	{
		if (box->value_null)
		{
			PG_RETURN_NULL();
		}
		PG_RETURN_DATUM(box->value);
	}
}

/*
 * (box, agg, valbytes|value) -> box
 * box->agg = agg
 * if agg.deserialize: box->value = agg.combine(box->value, agg.deserialize(valbytes))
 * else: box->value = agg.combine(box->value, value)
 * return box
 */
Datum
coord_combine_agg_sfunc(PG_FUNCTION_ARGS)
{
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	HeapTuple *aggtuple;
	HeapTuple *transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	Oid combine;
	Oid deserial;
	Datum value;
	bool value_null;

	if (PG_ARGISNULL(0)) {
		box = palloc(sizeof(StypeBox));
		box->agg = PG_GETARG_OID(1);
		box->value = (Datum) 0;
		box->value_null = true;
	}
	else
	{
		box = PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}

	aggtuple = get_aggform(box->agg, &aggform);
	deserial = aggform->deserialfunc;
	combine = aggform->combinefn;
	ReleaseSysCache(aggtuple);

	value_null = PG_ISARGNULL(2);
	if (IsOidValid(deserial))
	{
		fmgr_info(deserial, &info);
		if (!value_null || !info->fn_strict)
		{
			InitFunctionCallInfoData(&inner_fcinfo, &info, 2, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
			inner_fcinfo.arg[0] = value_null ? 0 (Datum) : PG_GETARG_BYTEA_PP(2);
			inner_fcinfo.arg[1] = 0 (Datum);
			inner_fcinfo.argnull[0] = value_null;
			inner_fcinfo.argnull[1] = true;
			value = FunctionCallInvoke(&inner_fcinfo);
			value_null = inner_fcinfo.isnull;
		}
		else
		{
			value = (Datum) 0;
		}
	}
	else
	{
		value = value_null ? (Datum) 0 : PG_GETARG(2);
	}

	fmgr_info(combine, &info);

	if (info.fn_strict) {
		if (box->value_null)
		{
			if (value_null)
			{
				PG_RETURN_NULL();
			}
			box->value = value;
			box->value_null = false;
			PG_RETURN_POINTER(box);
		}
		if (value_null)
		{
			PG_RETURN_POINTER(box);
		}
	}

	InitFunctionCallInfoData(&innerfcinfo, &info, 2, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	inner_fcinfo.arg[0] = box->value;
	inner_fcinfo.argnull[0] = box->value_null;
	inner_fcinfo.arg[1] = value;
	inner_fcinfo.argnull[1] = value_null;
	box->value = DirectFunctionCall2(combine, box->value, value);
	box->value = FunctionCallInvoke(&inner_fcinfo);
	box->value_null = inner_fcinfo.isnull;

	PG_RETURN_POINTER(box);
}

/*
 * (box, ...) -> fval
 * return box.agg.ffunc(box.value)
 */
Datum
coord_combine_agg_ffunc(PG_FUNCTION_ARGS)
{
	Datum ret;
	StypeBox *box = PG_GETARG_POINTER(0);
	FunctionCallInfo inner_fcinfo;
	FmgrInfo info;
	int inner_nargs;
	HeapTuple *aggtuple;
	HeapTuple *ffunctuple;
	Form_pg_aggregate aggform;
	Form_pg_proc ffuncform;
	Oid ffunc;
	bool fextra;
	bool final_strict;
	int i;

	aggtuple = get_aggform(box->agg, &aggform);
	ffunc = aggform->aggfinalfn;
	fextra = aggform->aggfinalextra;
	ReleaseSysCache(aggtuple);

	if (!IsValidOid(ffunc))
	{
		if (box->value_null)
		{
			PG_RETURN_NULL();
		}
		PG_RETURN_DATUM(box->value);
	}

	ffunctuple = get_procform(ffunc, &ffuncform);
	final_strict = ffuncform->proisstrict;
	ReleaseSysCache(ffunctuple);

	if (final_strict && box->value_null)
	{
		PG_RETURN_NULL();
	}

	fmgr_info(ffunc, &info);
	if (fextra)
	{
		inner_nargs = fcinfo->nargs;
	}
	else
	{
		inner_nargs = 1;
	}
	fmgr_info(ffunc, &info);
	InitFunctionCallInfoData(&inner_fcinfo, &info, inner_nargs, fcinfo->collation, fcinfo->context, fcinfo->resultinfo);
	inner_fcinfo.arg[0] = box->value;
	inner_fcinfo.argnull[0] = box->value_null;
	for (i = 1; i<inner_nargs; i++)
	{
		inner_fcinfo.argnull[i] = true;
	}
	ret = FunctionCallInvoke(&inner_fcinfo);
	fcinfo.isnull = inner_fcinfo.isnull;
	return ret;
}

