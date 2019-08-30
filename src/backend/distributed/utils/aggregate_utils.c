#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "distributed/version_compat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "fmgr.h"
#include "pg_config_manual.h"

PG_FUNCTION_INFO_V1(citus_stype_serialize);
PG_FUNCTION_INFO_V1(citus_stype_deserialize);
PG_FUNCTION_INFO_V1(citus_stype_combine);
PG_FUNCTION_INFO_V1(worker_partial_agg_sfunc);
PG_FUNCTION_INFO_V1(worker_partial_agg_ffunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_sfunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_ffunc);

/* TODO nodeAgg seems to decide to use serial/deserial based on stype == internal */
/*      Preferably we should match that logic, instead of checking serial/deserial oids */

typedef struct StypeBox
{
	Datum value;
	Oid agg;
	bool value_null;
} StypeBox;

static HeapTuple get_aggform(Oid oid, Form_pg_aggregate *form);
static HeapTuple get_procform(Oid oid, Form_pg_proc *form);
static HeapTuple get_typeform(Oid oid, Form_pg_type *form);
static void * pallocInAggContext(FunctionCallInfo fcinfo, size_t size);
static void InitializeStypeBox(StypeBox *box, HeapTuple aggTuple, Oid transtype);

static HeapTuple
get_aggform(Oid oid, Form_pg_aggregate *form)
{
	HeapTuple tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for aggregate %u", oid);
	}
	*form = (Form_pg_aggregate) GETSTRUCT(tuple);
	return tuple;
}


static HeapTuple
get_procform(Oid oid, Form_pg_proc *form)
{
	HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for function %u", oid);
	}
	*form = (Form_pg_proc) GETSTRUCT(tuple);
	return tuple;
}


static HeapTuple
get_typeform(Oid oid, Form_pg_type *form)
{
	HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for type %u", oid);
	}
	*form = (Form_pg_type) GETSTRUCT(tuple);
	return tuple;
}


static void *
pallocInAggContext(FunctionCallInfo fcinfo, size_t size)
{
	MemoryContext aggregateContext;
	MemoryContext oldContext;
	void *result;
	if (!AggCheckCallContext(fcinfo, &aggregateContext))
	{
		elog(ERROR, "Aggregate function called without an aggregate context");
	}

	oldContext = MemoryContextSwitchTo(aggregateContext);
	result = palloc(size);
	MemoryContextSwitchTo(oldContext);
	return result;
}


/*
 * See GetAggInitVal from pg's nodeAgg.c
 */
static void
InitializeStypeBox(StypeBox *box, HeapTuple aggTuple, Oid transtype)
{
	Datum textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
										Anum_pg_aggregate_agginitval,
										&box->value_null);
	if (box->value_null)
	{
		box->value = (Datum) 0;
	}
	else
	{
		Oid typinput,
			typioparam;
		char *strInitVal;

		getTypeInputInfo(transtype, &typinput, &typioparam);
		strInitVal = TextDatumGetCString(textInitVal);
		box->value = OidInputFunctionCall(typinput, strInitVal,
										  typioparam, -1);
		pfree(strInitVal);
	}
}


/*
 * (box) -> bytea
 * return bytes(box.agg.oid, box.agg.serial(box.value))
 */
Datum
citus_stype_serialize(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(inner_fcinfo, 1);
	FmgrInfo infodata;
	FmgrInfo *info = &infodata;
	StypeBox *box = (StypeBox *) PG_GETARG_POINTER(0);
	HeapTuple aggtuple;
	HeapTuple transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	bytea *valbytes;
	bytea *realbytes;
	Oid serial;
	Oid transtype;
	Size valbyteslen_exhdr;
	Size realbyteslen;
	Datum result;

	elog(WARNING, "citus_stype_serialize");
	elog(WARNING, "\t%d", box->agg);

	aggtuple = get_aggform(box->agg, &aggform);
	serial = aggform->aggserialfn;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	if (serial == InvalidOid)
	{
		elog(WARNING, "\tnoserial, load %d", transtype);

		/* TODO do we have to fallback to output/receive if not set? */
		/* ie is it possible for send/recv to be unset? */
		transtypetuple = get_typeform(transtype, &transtypeform);
		serial = transtypeform->typsend;
		ReleaseSysCache(transtypetuple);
	}

	Assert(serial != InvalidOid);

	fmgr_info(serial, info);
	if (info->fn_strict && box->value_null)
	{
		valbytes = NULL;
		valbyteslen_exhdr = 0;
	}
	else
	{
		InitFunctionCallInfoData(*inner_fcinfo, info, 1, fcinfo->fncollation,
								 fcinfo->context, fcinfo->resultinfo);
		fcSetArgExt(inner_fcinfo, 0, box->value, box->value_null);
		result = FunctionCallInvoke(inner_fcinfo);
		if (inner_fcinfo->isnull)
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
		pfree(valbytes); /* TODO I get to free this right? */
	}

	PG_RETURN_BYTEA_P(realbytes);
}


/*
 * (bytea, internal) -> box
 * box->agg = readagg(bytea)
 * box->value_null = readbool(bytea)
 * if (!box->value_null) box->value = agg.deserial(readrest(bytea))
 * return box
 */
Datum
citus_stype_deserialize(PG_FUNCTION_ARGS)
{
	StypeBox *box;
	bytea *bytes = PG_GETARG_BYTEA_PP(0);
	bytea *inner_bytes;
	Oid agg;
	HeapTuple aggtuple;
	HeapTuple transtypetuple;
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

	elog(WARNING, "citus_stype_deserialize %d %d", agg, value_null);

	box = pallocInAggContext(fcinfo, sizeof(StypeBox));
	box->agg = agg;
	if (value_null)
	{
		box->value = (Datum) 0;
		box->value_null = true;
		PG_RETURN_POINTER(box);
	}

	aggtuple = get_aggform(agg, &aggform);
	deserial = aggform->aggdeserialfn;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	if (deserial != InvalidOid)
	{
		FmgrInfo fdeserialinfo;
		LOCAL_FCINFO(fdeserial_callinfo, 2);

		inner_bytes = PG_GETARG_BYTEA_P_SLICE(0, sizeof(Oid), VARSIZE(bytes) -
											  sizeof(Oid));
		elog(WARNING, "deserial %d", VARSIZE(inner_bytes));
		fmgr_info(deserial, &fdeserialinfo);
		InitFunctionCallInfoData(*fdeserial_callinfo, &fdeserialinfo, 2,
								 fcinfo->fncollation, fcinfo->context,
								 fcinfo->resultinfo);
		fcSetArg(fdeserial_callinfo, 0, PointerGetDatum(inner_bytes));
		fcSetArg(fdeserial_callinfo, 1, PG_GETARG_DATUM(1));
		box->value = FunctionCallInvoke(fdeserial_callinfo);
		box->value_null = fdeserial_callinfo->isnull;
	}

	/* TODO Correct null handling */
	else if (value_null)
	{
		box->value = (Datum) 0;
		box->value_null = true;
	}
	else
	{
		transtypetuple = get_typeform(transtype, &transtypeform);
		ioparam = getTypeIOParam(transtypetuple);
		recv = transtypeform->typreceive;
		ReleaseSysCache(transtypetuple);

		initStringInfo(&buf);
		appendBinaryStringInfo(&buf,
							   VARDATA(bytes) + sizeof(Oid) + sizeof(bool),
							   VARSIZE(bytes) - VARHDRSZ - sizeof(Oid) - sizeof(bool));

		box->value = OidReceiveFunctionCall(recv, &buf, ioparam, -1);
	}

	PG_RETURN_POINTER(box);
}


/*
 * (box1, box2) -> box
 * box1.value = box.agg.combine(box1.value, box2.value)
 * return box
 */
Datum
citus_stype_combine(PG_FUNCTION_ARGS)
{
	StypeBox *box1 = NULL;
	StypeBox *box2 = NULL;
	LOCAL_FCINFO(inner_fcinfo, 2);
	FmgrInfo info;
	Oid combine;
	HeapTuple aggtuple;
	Form_pg_aggregate aggform;

	elog(WARNING, "citus_stype_combine");

	if (!PG_ARGISNULL(0))
	{
		box1 = (StypeBox *) PG_GETARG_POINTER(0);
	}
	if (!PG_ARGISNULL(1))
	{
		box2 = (StypeBox *) PG_GETARG_POINTER(1);
	}

	if (box1 == NULL)
	{
		if (box2 == NULL)
		{
			PG_RETURN_NULL();
		}

		box1 = pallocInAggContext(fcinfo, sizeof(StypeBox));
		box1->value = (Datum) 0;
		box1->value_null = true;
		box1->agg = box2->agg;
	}

	aggtuple = get_aggform(box1->agg, &aggform);
	combine = aggform->aggcombinefn;
	ReleaseSysCache(aggtuple);

	Assert(combine != InvalidOid);
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

	InitFunctionCallInfoData(*inner_fcinfo, &info, 2, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(inner_fcinfo, 0, box1->value, box1->value_null);
	fcSetArgExt(inner_fcinfo, 1, box2->value, box2->value_null);

	/* TODO Deal with memory management juggling (see executor/nodeAgg) */
	box1->value = FunctionCallInvoke(inner_fcinfo);
	box1->value_null = inner_fcinfo->isnull;

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
	Form_pg_aggregate aggform;
	HeapTuple aggtuple;
	Oid aggsfunc;
	LOCAL_FCINFO(inner_fcinfo, FUNC_MAX_ARGS);
	FmgrInfo info;
	int i;
	bool is_initial_call = PG_ARGISNULL(0);

	elog(WARNING, "worker_partial_agg_sfunc");

	if (is_initial_call)
	{
		box = pallocInAggContext(fcinfo, sizeof(StypeBox));
		box->agg = PG_GETARG_OID(1);
	}
	else
	{
		box = (StypeBox *) PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}

	elog(WARNING, "\tbox: %p", box);
	elog(WARNING, "\tagg: %d", box->agg);
	aggtuple = get_aggform(box->agg, &aggform);
	aggsfunc = aggform->aggtransfn;
	if (is_initial_call)
	{
		InitializeStypeBox(box, aggtuple, aggform->aggtranstype);
	}
	ReleaseSysCache(aggtuple);

	fmgr_info(aggsfunc, &info);
	InitFunctionCallInfoData(*inner_fcinfo, &info, fcinfo->nargs - 1, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	if (info.fn_strict)
	{
		for (i = 2; i < PG_NARGS(); i++)
		{
			if (PG_ARGISNULL(i))
			{
				elog(WARNING, "\tworker sfunc retnull %i", i);
				PG_RETURN_POINTER(box);
			}
		}
		if (box->value_null)
		{
			elog(WARNING, "\tworker sfunc seed");
			box->value = PG_GETARG_DATUM(2);
			box->value_null = false;
			PG_RETURN_POINTER(box);
		}
	}

	/* TODO Deal with memory management juggling (see executor/nodeAgg) */
	fcSetArgExt(inner_fcinfo, 0, box->value, box->value_null);
	for (i = 1; i < inner_fcinfo->nargs; i++)
	{
		fcSetArgExt(inner_fcinfo, i, fcGetArgValue(fcinfo, i + 1), fcGetArgNull(fcinfo,
																				i + 1));
	}
	elog(WARNING, "invoke sfunc");
	box->value = FunctionCallInvoke(inner_fcinfo);
	box->value_null = inner_fcinfo->isnull;

	elog(WARNING, "\tworker sfunc agg: %d", box->agg);
	elog(WARNING, "\tworker sfunc null: %d", box->value_null);

	PG_RETURN_POINTER(box);
}


/*
 * (box) -> box.agg.stype
 * return box.agg.serialize(box.value)
 */
Datum
worker_partial_agg_ffunc(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(inner_fcinfo, 1);
	FmgrInfo info;
	StypeBox *box = (StypeBox *) PG_GETARG_POINTER(0);
	HeapTuple aggtuple;
	HeapTuple transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	Oid serial;
	Oid transtype;
	Datum result;

	elog(WARNING, "worker_partial_agg_ffunc %p", box);

	if (box == NULL)
	{
		PG_RETURN_NULL();
	}

	elog(WARNING, "\tagg: %d", box->agg);

	aggtuple = get_aggform(box->agg, &aggform);
	serial = aggform->aggserialfn;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	if (serial == InvalidOid)
	{
		elog(WARNING, "\tload typeform %d", transtype);

		/* TODO do we have to fallback to output/receive if not set? */
		/* ie is it possible for send/recv to be unset? */
		transtypetuple = get_typeform(transtype, &transtypeform);
		serial = transtypeform->typsend;
		ReleaseSysCache(transtypetuple);
	}

	Assert(serial != InvalidOid);

	elog(WARNING, "\tcalling serial %d", serial);
	fmgr_info(serial, &info);
	if (info.fn_strict && box->value_null)
	{
		elog(WARNING, "\t\t& strict NULL");
		PG_RETURN_NULL();
	}
	elog(WARNING, "\t\tinit inner_fcinfo %ld %d", box->value, box->value_null);
	InitFunctionCallInfoData(*inner_fcinfo, &info, 1, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(inner_fcinfo, 0, box->value, box->value_null);

	elog(WARNING, "\t\tinvoke inner_fcinfo %p %p", info.fn_addr, array_send);
	result = FunctionCallInvoke(inner_fcinfo);
	elog(WARNING, "\t\t& done %d", VARSIZE(DatumGetByteaPP(result)));
	if (inner_fcinfo->isnull)
	{
		PG_RETURN_NULL();
	}
	PG_RETURN_DATUM(result);
}


/*
 * (box, agg, valbytes) -> box
 * box->agg = agg
 * box->value = agg.combine(box->value, agg.deserialize(valbytes))
 * return box
 */
Datum
coord_combine_agg_sfunc(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(inner_fcinfo, 3);
	FmgrInfo info;
	HeapTuple aggtuple;
	HeapTuple transtypetuple;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	Oid combine;
	Oid deserial;
	Oid transtype;
	Oid ioparam;
	Datum value;
	bool value_null;
	StypeBox *box;

	elog(WARNING, "coord_combine_agg_sfunc");

	if (PG_ARGISNULL(0))
	{
		box = pallocInAggContext(fcinfo, sizeof(StypeBox));

		box->agg = PG_GETARG_OID(1);
		box->value = (Datum) 0;
		box->value_null = true;
	}
	else
	{
		box = (StypeBox *) PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}

	elog(WARNING, "\tbox->agg = %u", box->agg);

	aggtuple = get_aggform(box->agg, &aggform);
	deserial = aggform->aggdeserialfn;
	combine = aggform->aggcombinefn;
	transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	value_null = PG_ARGISNULL(2);
	if (deserial != InvalidOid)
	{
		fmgr_info(deserial, &info);
		if (!value_null || !info.fn_strict)
		{
			InitFunctionCallInfoData(*inner_fcinfo, &info, 2, fcinfo->fncollation,
									 fcinfo->context, fcinfo->resultinfo);
			fcSetArgExt(inner_fcinfo, 0, value_null ? (Datum) 0 : PG_GETARG_DATUM(2),
						value_null);
			fcSetArgNull(inner_fcinfo, 1);
			value = FunctionCallInvoke(inner_fcinfo);
			value_null = inner_fcinfo->isnull;
		}
		else
		{
			value = (Datum) 0;
		}
	}
	else
	{
		transtypetuple = get_typeform(transtype, &transtypeform);
		ioparam = getTypeIOParam(transtypetuple);
		deserial = transtypeform->typreceive;
		ReleaseSysCache(transtypetuple);

		fmgr_info(deserial, &info);
		if (!value_null || !info.fn_strict)
		{
			StringInfoData buf;

			if (!value_null)
			{
				bytea *data = PG_GETARG_BYTEA_PP(2);
				initStringInfo(&buf);
				appendBinaryStringInfo(&buf, (char *) VARDATA(data), VARSIZE(data) -
									   VARHDRSZ);
				elog(WARNING, "\treceive %d", buf.len);
			}

			InitFunctionCallInfoData(*inner_fcinfo, &info, 3, fcinfo->fncollation,
									 fcinfo->context, fcinfo->resultinfo);
			fcSetArgExt(inner_fcinfo, 0, PointerGetDatum(value_null ? NULL : &buf),
						value_null);
			fcSetArg(inner_fcinfo, 1, ObjectIdGetDatum(ioparam));
			fcSetArg(inner_fcinfo, 2, Int32GetDatum(-1)); /* typmod */

			value = FunctionCallInvoke(inner_fcinfo);
			value_null = inner_fcinfo->isnull;
		}
		else
		{
			value = (Datum) 0;
		}
	}

	fmgr_info(combine, &info);

	if (info.fn_strict)
	{
		if (box->value_null)
		{
			elog(WARNING, "\tbox null");
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
			elog(WARNING, "\tvalue null");
			PG_RETURN_POINTER(box);
		}
	}

	elog(WARNING, "\tcombine %u", box->agg);
	InitFunctionCallInfoData(*inner_fcinfo, &info, 2, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(inner_fcinfo, 0, box->value, box->value_null);
	fcSetArgExt(inner_fcinfo, 1, value, value_null);
	box->value = FunctionCallInvoke(inner_fcinfo);
	box->value_null = inner_fcinfo->isnull;

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
	StypeBox *box = (StypeBox *) PG_GETARG_POINTER(0);
	LOCAL_FCINFO(inner_fcinfo, FUNC_MAX_ARGS);
	FmgrInfo info;
	int inner_nargs;
	HeapTuple aggtuple;
	HeapTuple ffunctuple;
	Form_pg_aggregate aggform;
	Form_pg_proc ffuncform;
	Oid ffunc;
	bool fextra;
	bool final_strict;
	int i;

	elog(WARNING, "coord_combine_agg_ffunc %p", box);

	if (box == NULL)
	{
		PG_RETURN_NULL();
	}

	aggtuple = get_aggform(box->agg, &aggform);
	ffunc = aggform->aggfinalfn;
	fextra = aggform->aggfinalextra;
	ReleaseSysCache(aggtuple);

	if (ffunc == InvalidOid)
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
	InitFunctionCallInfoData(*inner_fcinfo, &info, inner_nargs, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(inner_fcinfo, 0, box->value, box->value_null);
	for (i = 1; i < inner_nargs; i++)
	{
		fcSetArgNull(inner_fcinfo, i);
	}
	ret = FunctionCallInvoke(inner_fcinfo);
	fcinfo->isnull = inner_fcinfo->isnull;
	return ret;
}
