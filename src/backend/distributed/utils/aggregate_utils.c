/*-------------------------------------------------------------------------
 *
 * aggregate_utils.c
 *
 * Implementation of UDFs distributing execution of aggregates across workers.
 *
 * When an aggregate has a combinefunc, we use worker_partial_agg to skip
 * calling finalfunc on workers, instead passing state to coordinator where
 * it uses combinefunc in coord_combine_agg & applying finalfunc only at end.
 *
 * Copyright Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "distributed/version_compat.h"
#include "nodes/nodeFuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pg_config_manual.h"

PG_FUNCTION_INFO_V1(worker_partial_agg_sfunc);
PG_FUNCTION_INFO_V1(worker_partial_agg_ffunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_sfunc);
PG_FUNCTION_INFO_V1(coord_combine_agg_ffunc);

/*
 * internal type for support aggregates to pass transition state alongside
 * aggregation bookkeeping
 */
typedef struct StypeBox
{
	Datum value;
	Oid agg;
	Oid transtype;
	int16_t transtypeLen;
	bool transtypeByVal;
	bool valueNull;
	bool valueInit;
} StypeBox;

static HeapTuple GetAggregateForm(Oid oid, Form_pg_aggregate *form);
static HeapTuple GetProcForm(Oid oid, Form_pg_proc *form);
static HeapTuple GetTypeForm(Oid oid, Form_pg_type *form);
static void * pallocInAggContext(FunctionCallInfo fcinfo, size_t size);
static void aclcheckAggregate(ObjectType objectType, Oid userOid, Oid funcOid);
static Datum GetAggInitVal(Datum textInitVal, Oid transtype);
static void InitializeStypeBox(FunctionCallInfo fcinfo, StypeBox *box, HeapTuple aggTuple,
							   Oid transtype);
static StypeBox * TryCreateStypeBoxFromFcinfoAggref(FunctionCallInfo fcinfo);
static void HandleTransition(StypeBox *box, FunctionCallInfo fcinfo,
							 FunctionCallInfo innerFcinfo);
static void HandleStrictUninit(StypeBox *box, FunctionCallInfo fcinfo, Datum value);
static bool TypecheckWorkerPartialAggArgType(FunctionCallInfo fcinfo, StypeBox *box);
static bool TypecheckCoordCombineAggReturnType(FunctionCallInfo fcinfo, Oid ffunc,
											   StypeBox *box);

/*
 * GetAggregateForm loads corresponding tuple & Form_pg_aggregate for oid
 */
static HeapTuple
GetAggregateForm(Oid oid, Form_pg_aggregate *form)
{
	HeapTuple tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for aggregate %u", oid);
	}
	*form = (Form_pg_aggregate) GETSTRUCT(tuple);
	return tuple;
}


/*
 * GetProcForm loads corresponding tuple & Form_pg_proc for oid
 */
static HeapTuple
GetProcForm(Oid oid, Form_pg_proc *form)
{
	HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for function %u", oid);
	}
	*form = (Form_pg_proc) GETSTRUCT(tuple);
	return tuple;
}


/*
 * GetTypeForm loads corresponding tuple & Form_pg_type for oid
 */
static HeapTuple
GetTypeForm(Oid oid, Form_pg_type *form)
{
	HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "citus cache lookup failed for type %u", oid);
	}
	*form = (Form_pg_type) GETSTRUCT(tuple);
	return tuple;
}


/*
 * pallocInAggContext calls palloc in fcinfo's aggregate context
 */
static void *
pallocInAggContext(FunctionCallInfo fcinfo, size_t size)
{
	MemoryContext aggregateContext;
	if (!AggCheckCallContext(fcinfo, &aggregateContext))
	{
		elog(ERROR, "Aggregate function called without an aggregate context");
	}
	return MemoryContextAlloc(aggregateContext, size);
}


/*
 * aclcheckAggregate verifies that the given user has ACL_EXECUTE to the given proc
 */
static void
aclcheckAggregate(ObjectType objectType, Oid userOid, Oid funcOid)
{
	AclResult aclresult;
	if (funcOid != InvalidOid)
	{
		aclresult = pg_proc_aclcheck(funcOid, userOid, ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
		{
			aclcheck_error(aclresult, objectType, get_func_name(funcOid));
		}
	}
}


/* Copied from nodeAgg.c */
static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	/* *INDENT-OFF* */
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
	/* *INDENT-ON* */
}


/*
 * InitializeStypeBox fills in the rest of an StypeBox's fields besides agg,
 * handling both permission checking & setting up the initial transition state.
 */
static void
InitializeStypeBox(FunctionCallInfo fcinfo, StypeBox *box, HeapTuple aggTuple, Oid
				   transtype)
{
	Form_pg_aggregate aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	Oid userId = GetUserId();

	/* First we make ACL_EXECUTE checks as would be done in nodeAgg.c */
	aclcheckAggregate(OBJECT_AGGREGATE, userId, aggform->aggfnoid);
	aclcheckAggregate(OBJECT_FUNCTION, userId, aggform->aggfinalfn);
	aclcheckAggregate(OBJECT_FUNCTION, userId, aggform->aggtransfn);
	aclcheckAggregate(OBJECT_FUNCTION, userId, aggform->aggdeserialfn);
	aclcheckAggregate(OBJECT_FUNCTION, userId, aggform->aggserialfn);
	aclcheckAggregate(OBJECT_FUNCTION, userId, aggform->aggcombinefn);

	Datum textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
										Anum_pg_aggregate_agginitval,
										&box->valueNull);
	box->transtype = transtype;
	box->valueInit = !box->valueNull;
	if (box->valueNull)
	{
		box->value = (Datum) 0;
	}
	else
	{
		MemoryContext aggregateContext;
		if (!AggCheckCallContext(fcinfo, &aggregateContext))
		{
			elog(ERROR, "InitializeStypeBox called from non aggregate context");
		}
		MemoryContext oldContext = MemoryContextSwitchTo(aggregateContext);

		box->value = GetAggInitVal(textInitVal, transtype);

		MemoryContextSwitchTo(oldContext);
	}
}


/*
 * TryCreateStypeBoxFromFcinfoAggref attempts to initialize an StypeBox through
 * introspection of the fcinfo's Aggref from AggGetAggref. This is required
 * when we receive no intermediate rows.
 *
 * Returns NULL if the Aggref isn't our expected shape.
 */
static StypeBox *
TryCreateStypeBoxFromFcinfoAggref(FunctionCallInfo fcinfo)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	if (aggref == NULL || aggref->args == NIL)
	{
		return NULL;
	}

	TargetEntry *aggArg = linitial(aggref->args);
	if (!IsA(aggArg->expr, Const))
	{
		return NULL;
	}

	Const *aggConst = (Const *) aggArg->expr;
	if (aggConst->consttype != OIDOID && aggConst->consttype != REGPROCEDUREOID)
	{
		return NULL;
	}

	Form_pg_aggregate aggform;
	StypeBox *box = pallocInAggContext(fcinfo, sizeof(StypeBox));
	box->agg = DatumGetObjectId(aggConst->constvalue);
	HeapTuple aggTuple = GetAggregateForm(box->agg, &aggform);
	InitializeStypeBox(fcinfo, box, aggTuple, aggform->aggtranstype);
	ReleaseSysCache(aggTuple);

	return box;
}


/*
 * HandleTransition copies logic used in nodeAgg's advance_transition_function
 * for handling result of transition function.
 */
static void
HandleTransition(StypeBox *box, FunctionCallInfo fcinfo, FunctionCallInfo innerFcinfo)
{
	Datum newVal = FunctionCallInvoke(innerFcinfo);
	bool newValIsNull = innerFcinfo->isnull;

	if (!box->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(box->value))
	{
		if (!newValIsNull)
		{
			MemoryContext aggregateContext;

			if (!AggCheckCallContext(fcinfo, &aggregateContext))
			{
				elog(ERROR,
					 "HandleTransition called from non aggregate context");
			}

			MemoryContext oldContext = MemoryContextSwitchTo(aggregateContext);
			if (!(DatumIsReadWriteExpandedObject(newVal,
												 false, box->transtypeLen) &&
				  MemoryContextGetParent(DatumGetEOHP(newVal)->eoh_context) ==
				  CurrentMemoryContext))
			{
				newVal = datumCopy(newVal, box->transtypeByVal, box->transtypeLen);
			}
			MemoryContextSwitchTo(oldContext);
		}

		if (!box->valueNull)
		{
			if (DatumIsReadWriteExpandedObject(box->value,
											   false, box->transtypeLen))
			{
				DeleteExpandedObject(box->value);
			}
			else
			{
				pfree(DatumGetPointer(box->value));
			}
		}
	}

	box->value = newVal;
	box->valueNull = newValIsNull;
}


/*
 * HandleStrictUninit handles initialization of state for when
 * transition function is strict & state has not yet been initialized.
 */
static void
HandleStrictUninit(StypeBox *box, FunctionCallInfo fcinfo, Datum value)
{
	MemoryContext aggregateContext;

	if (!AggCheckCallContext(fcinfo, &aggregateContext))
	{
		elog(ERROR, "HandleStrictUninit called from non aggregate context");
	}

	MemoryContext oldContext = MemoryContextSwitchTo(aggregateContext);
	box->value = datumCopy(value, box->transtypeByVal, box->transtypeLen);
	MemoryContextSwitchTo(oldContext);

	box->valueNull = false;
	box->valueInit = true;
}


/*
 * worker_partial_agg_sfunc advances transition state,
 * essentially implementing the following pseudocode:
 *
 * (box, agg, ...) -> box
 * box.agg = agg;
 * box.value = agg.sfunc(box.value, ...);
 * return box
 */
Datum
worker_partial_agg_sfunc(PG_FUNCTION_ARGS)
{
	StypeBox *box = NULL;
	Form_pg_aggregate aggform;
	LOCAL_FCINFO(innerFcinfo, FUNC_MAX_ARGS);
	FmgrInfo info;
	int argumentIndex = 0;
	bool initialCall = PG_ARGISNULL(0);

	if (initialCall)
	{
		box = pallocInAggContext(fcinfo, sizeof(StypeBox));
		box->agg = PG_GETARG_OID(1);

		if (!TypecheckWorkerPartialAggArgType(fcinfo, box))
		{
			ereport(ERROR, (errmsg(
								"worker_partial_agg_sfunc could not confirm type correctness")));
		}
	}
	else
	{
		box = (StypeBox *) PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}

	HeapTuple aggtuple = GetAggregateForm(box->agg, &aggform);
	Oid aggsfunc = aggform->aggtransfn;
	if (initialCall)
	{
		InitializeStypeBox(fcinfo, box, aggtuple, aggform->aggtranstype);
	}
	ReleaseSysCache(aggtuple);
	if (initialCall)
	{
		get_typlenbyval(box->transtype,
						&box->transtypeLen,
						&box->transtypeByVal);
	}

	fmgr_info(aggsfunc, &info);
	if (info.fn_strict)
	{
		for (argumentIndex = 2; argumentIndex < PG_NARGS(); argumentIndex++)
		{
			if (PG_ARGISNULL(argumentIndex))
			{
				PG_RETURN_POINTER(box);
			}
		}

		if (!box->valueInit)
		{
			HandleStrictUninit(box, fcinfo, PG_GETARG_DATUM(2));
			PG_RETURN_POINTER(box);
		}

		if (box->valueNull)
		{
			PG_RETURN_POINTER(box);
		}
	}

	InitFunctionCallInfoData(*innerFcinfo, &info, fcinfo->nargs - 1, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(innerFcinfo, 0, box->value, box->valueNull);
	for (argumentIndex = 1; argumentIndex < innerFcinfo->nargs; argumentIndex++)
	{
		fcSetArgExt(innerFcinfo, argumentIndex, fcGetArgValue(fcinfo, argumentIndex + 1),
					fcGetArgNull(fcinfo, argumentIndex + 1));
	}

	HandleTransition(box, fcinfo, innerFcinfo);

	PG_RETURN_POINTER(box);
}


/*
 * worker_partial_agg_ffunc serializes transition state,
 * essentially implementing the following pseudocode:
 *
 * (box) -> text
 * return box.agg.stype.output(box.value)
 */
Datum
worker_partial_agg_ffunc(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(innerFcinfo, 1);
	FmgrInfo info;
	StypeBox *box = (StypeBox *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	Form_pg_aggregate aggform;
	Oid typoutput = InvalidOid;
	bool typIsVarlena = false;

	if (box == NULL)
	{
		box = TryCreateStypeBoxFromFcinfoAggref(fcinfo);
	}

	if (box == NULL || box->valueNull)
	{
		PG_RETURN_NULL();
	}

	HeapTuple aggtuple = GetAggregateForm(box->agg, &aggform);

	if (aggform->aggcombinefn == InvalidOid)
	{
		ereport(ERROR, (errmsg(
							"worker_partial_agg_ffunc expects an aggregate with COMBINEFUNC")));
	}

	if (aggform->aggtranstype == INTERNALOID)
	{
		ereport(ERROR,
				(errmsg(
					 "worker_partial_agg_ffunc does not support aggregates with INTERNAL transition state")));
	}

	Oid transtype = aggform->aggtranstype;
	ReleaseSysCache(aggtuple);

	getTypeOutputInfo(transtype, &typoutput, &typIsVarlena);

	fmgr_info(typoutput, &info);

	InitFunctionCallInfoData(*innerFcinfo, &info, 1, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(innerFcinfo, 0, box->value, box->valueNull);

	Datum result = FunctionCallInvoke(innerFcinfo);

	if (innerFcinfo->isnull)
	{
		PG_RETURN_NULL();
	}
	PG_RETURN_DATUM(result);
}


/*
 * coord_combine_agg_sfunc deserializes transition state from worker
 * & advances transition state using combinefunc,
 * essentially implementing the following pseudocode:
 *
 * (box, agg, text) -> box
 * box.agg = agg
 * box.value = agg.combine(box.value, agg.stype.input(text))
 * return box
 */
Datum
coord_combine_agg_sfunc(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(innerFcinfo, 3);
	FmgrInfo info;
	Form_pg_aggregate aggform;
	Form_pg_type transtypeform;
	Datum value;
	StypeBox *box = NULL;

	if (PG_ARGISNULL(0))
	{
		box = pallocInAggContext(fcinfo, sizeof(StypeBox));
		box->agg = PG_GETARG_OID(1);
	}
	else
	{
		box = (StypeBox *) PG_GETARG_POINTER(0);
		Assert(box->agg == PG_GETARG_OID(1));
	}

	HeapTuple aggtuple = GetAggregateForm(box->agg, &aggform);

	if (aggform->aggcombinefn == InvalidOid)
	{
		ereport(ERROR, (errmsg(
							"coord_combine_agg_sfunc expects an aggregate with COMBINEFUNC")));
	}

	if (aggform->aggtranstype == INTERNALOID)
	{
		ereport(ERROR,
				(errmsg(
					 "coord_combine_agg_sfunc does not support aggregates with INTERNAL transition state")));
	}

	Oid combine = aggform->aggcombinefn;

	if (PG_ARGISNULL(0))
	{
		InitializeStypeBox(fcinfo, box, aggtuple, aggform->aggtranstype);
	}

	ReleaseSysCache(aggtuple);

	if (PG_ARGISNULL(0))
	{
		get_typlenbyval(box->transtype,
						&box->transtypeLen,
						&box->transtypeByVal);
	}

	bool valueNull = PG_ARGISNULL(2);
	HeapTuple transtypetuple = GetTypeForm(box->transtype, &transtypeform);
	Oid ioparam = getTypeIOParam(transtypetuple);
	Oid deserial = transtypeform->typinput;
	ReleaseSysCache(transtypetuple);

	fmgr_info(deserial, &info);
	if (valueNull && info.fn_strict)
	{
		value = (Datum) 0;
	}
	else
	{
		InitFunctionCallInfoData(*innerFcinfo, &info, 3, fcinfo->fncollation,
								 fcinfo->context, fcinfo->resultinfo);
		fcSetArgExt(innerFcinfo, 0, PG_GETARG_DATUM(2), valueNull);
		fcSetArg(innerFcinfo, 1, ObjectIdGetDatum(ioparam));
		fcSetArg(innerFcinfo, 2, Int32GetDatum(-1)); /* typmod */

		value = FunctionCallInvoke(innerFcinfo);
		valueNull = innerFcinfo->isnull;
	}

	fmgr_info(combine, &info);

	if (info.fn_strict)
	{
		if (valueNull)
		{
			PG_RETURN_POINTER(box);
		}

		if (!box->valueInit)
		{
			HandleStrictUninit(box, fcinfo, value);
			PG_RETURN_POINTER(box);
		}

		if (box->valueNull)
		{
			PG_RETURN_POINTER(box);
		}
	}

	InitFunctionCallInfoData(*innerFcinfo, &info, 2, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(innerFcinfo, 0, box->value, box->valueNull);
	fcSetArgExt(innerFcinfo, 1, value, valueNull);

	HandleTransition(box, fcinfo, innerFcinfo);

	PG_RETURN_POINTER(box);
}


/*
 * coord_combine_agg_ffunc applies finalfunc of aggregate to state,
 * essentially implementing the following pseudocode:
 *
 * (box, ...) -> fval
 * return box.agg.ffunc(box.value)
 */
Datum
coord_combine_agg_ffunc(PG_FUNCTION_ARGS)
{
	StypeBox *box = (StypeBox *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	LOCAL_FCINFO(innerFcinfo, FUNC_MAX_ARGS);
	FmgrInfo info;
	int innerNargs = 0;
	Form_pg_aggregate aggform;
	Form_pg_proc ffuncform;

	if (box == NULL)
	{
		box = TryCreateStypeBoxFromFcinfoAggref(fcinfo);

		if (box == NULL)
		{
			PG_RETURN_NULL();
		}
	}

	HeapTuple aggtuple = GetAggregateForm(box->agg, &aggform);
	Oid ffunc = aggform->aggfinalfn;
	bool fextra = aggform->aggfinalextra;
	ReleaseSysCache(aggtuple);

	if (!TypecheckCoordCombineAggReturnType(fcinfo, ffunc, box))
	{
		ereport(ERROR, (errmsg(
							"coord_combine_agg_ffunc could not confirm type correctness")));
	}

	if (ffunc == InvalidOid)
	{
		if (box->valueNull)
		{
			PG_RETURN_NULL();
		}
		PG_RETURN_DATUM(box->value);
	}

	HeapTuple ffunctuple = GetProcForm(ffunc, &ffuncform);
	bool finalStrict = ffuncform->proisstrict;
	ReleaseSysCache(ffunctuple);

	if (finalStrict && box->valueNull)
	{
		PG_RETURN_NULL();
	}

	if (fextra)
	{
		innerNargs = fcinfo->nargs;
	}
	else
	{
		innerNargs = 1;
	}
	fmgr_info(ffunc, &info);
	InitFunctionCallInfoData(*innerFcinfo, &info, innerNargs, fcinfo->fncollation,
							 fcinfo->context, fcinfo->resultinfo);
	fcSetArgExt(innerFcinfo, 0, box->value, box->valueNull);
	for (int argumentIndex = 1; argumentIndex < innerNargs; argumentIndex++)
	{
		fcSetArgNull(innerFcinfo, argumentIndex);
	}

	Datum result = FunctionCallInvoke(innerFcinfo);
	fcinfo->isnull = innerFcinfo->isnull;
	return result;
}


/*
 * TypecheckWorkerPartialAggArgType returns whether the arguments being passed to
 * worker_partial_agg match the arguments expected by the aggregate being distributed.
 */
static bool
TypecheckWorkerPartialAggArgType(FunctionCallInfo fcinfo, StypeBox *box)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	if (aggref == NULL)
	{
		return false;
	}

	Assert(list_length(aggref->args) == 2);
	TargetEntry *aggarg = list_nth(aggref->args, 1);

	bool argtypesNull;
	HeapTuple proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(box->agg));
	if (!HeapTupleIsValid(proctuple))
	{
		return false;
	}

	Datum argtypes = SysCacheGetAttr(PROCOID, proctuple,
									 Anum_pg_proc_proargtypes,
									 &argtypesNull);
	Assert(!argtypesNull);
	ReleaseSysCache(proctuple);

	if (ARR_NDIM(DatumGetArrayTypeP(argtypes)) != 1 ||
		ARR_DIMS(DatumGetArrayTypeP(argtypes))[0] != 1)
	{
		elog(ERROR, "worker_partial_agg_sfunc cannot type check aggregates "
					"taking anything other than 1 argument");
	}

	int arrayIndex = 0;
	Datum argtype = array_get_element(argtypes,
									  1, &arrayIndex, -1, sizeof(Oid), true, 'i',
									  &argtypesNull);
	Assert(!argtypesNull);

	return aggarg != NULL && exprType((Node *) aggarg->expr) == DatumGetObjectId(argtype);
}


/*
 * TypecheckCoordCombineAggReturnType returns whether the return type of the aggregate
 * being distributed by coord_combine_agg matches the null constant used to inform postgres
 * what the aggregate's expected return type is.
 */
static bool
TypecheckCoordCombineAggReturnType(FunctionCallInfo fcinfo, Oid ffunc, StypeBox *box)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	if (aggref == NULL)
	{
		return false;
	}

	Oid finalType = ffunc == InvalidOid ?
					box->transtype : get_func_rettype(ffunc);

	Assert(list_length(aggref->args) == 3);
	TargetEntry *nulltag = list_nth(aggref->args, 2);

	return nulltag != NULL && IsA(nulltag->expr, Const) &&
		   ((Const *) nulltag->expr)->consttype == finalType;
}
