/*-------------------------------------------------------------------------
 *
 * citus_nodefuncs.c
 *	  Helper functions for dealing with nodes
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_planner.h"

static const char *CitusNodeTagNamesD[] = {
	"MultiNode",
	"MultiTreeRoot",
	"MultiProject",
	"MultiCollect",
	"MultiSelect",
	"MultiTable",
	"MultiJoin",
	"MultiPartition",
	"MultiCartesianProduct",
	"MultiExtendedOp",
	"Job",
	"MapMergeJob",
	"MultiPlan",
	"Task",
	"ShardInterval",
	"ShardPlacement",
	"RelationShard"
};

const char **CitusNodeTagNames = CitusNodeTagNamesD;


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_extradata_container);


/*
 * SetRangeTblExtraData adds additional data to a RTE, overwriting previous
 * values, if present.
 *
 * The data is stored as RTE_FUNCTION type RTE of a special
 * citus_extradata_container function, with the extra data serialized into the
 * function arguments. That works, because these RTEs aren't used by Postgres
 * to any significant degree, and Citus' variant of ruleutils.c knows how to
 * deal with these extended RTEs. Note that rte->eref needs to be set prior
 * to calling SetRangeTblExtraData to ensure the funccolcount can be set
 * correctly.
 *
 * NB: If used for postgres defined RTEKinds, fields specific to that RTEKind
 * will not be handled by out/readfuncs.c. For the current uses that's ok.
 */
void
SetRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
					 char *fragmentSchemaName, char *fragmentTableName,
					 List *tableIdList)
{
	RangeTblFunction *fauxFunction = NULL;
	FuncExpr *fauxFuncExpr = NULL;
	Const *rteKindData = NULL;
	Const *fragmentSchemaData = NULL;
	Const *fragmentTableData = NULL;
	Const *tableIdListData = NULL;

	Assert(rte->eref);

	/* store RTE kind as a plain int4 */
	rteKindData = makeNode(Const);
	rteKindData->consttype = INT4OID;
	rteKindData->constlen = 4;
	rteKindData->constvalue = Int32GetDatum(rteKind);
	rteKindData->constbyval = true;
	rteKindData->constisnull = false;
	rteKindData->location = -1;

	/* store the fragment schema as a cstring */
	fragmentSchemaData = makeNode(Const);
	fragmentSchemaData->consttype = CSTRINGOID;
	fragmentSchemaData->constlen = -2;
	fragmentSchemaData->constvalue = CStringGetDatum(fragmentSchemaName);
	fragmentSchemaData->constbyval = false;
	fragmentSchemaData->constisnull = fragmentSchemaName == NULL;
	fragmentSchemaData->location = -1;

	/* store the fragment name as a cstring */
	fragmentTableData = makeNode(Const);
	fragmentTableData->consttype = CSTRINGOID;
	fragmentTableData->constlen = -2;
	fragmentTableData->constvalue = CStringGetDatum(fragmentTableName);
	fragmentTableData->constbyval = false;
	fragmentTableData->constisnull = fragmentTableName == NULL;
	fragmentTableData->location = -1;

	/* store the table id list as an array of integers: FIXME */
	tableIdListData = makeNode(Const);
	tableIdListData->consttype = CSTRINGOID;
	tableIdListData->constbyval = false;
	tableIdListData->constlen = -2;
	tableIdListData->location = -1;

	/* serialize tableIdList to a string, seems simplest that way */
	if (tableIdList != NIL)
	{
		char *serializedList = nodeToString(tableIdList);
		tableIdListData->constisnull = false;
		tableIdListData->constvalue = CStringGetDatum(serializedList);
	}
	else
	{
		tableIdListData->constisnull = true;
	}

	/* create function expression to store our faux arguments in */
	fauxFuncExpr = makeNode(FuncExpr);
	fauxFuncExpr->funcid = CitusExtraDataContainerFuncId();
	fauxFuncExpr->funcretset = true;
	fauxFuncExpr->location = -1;
	fauxFuncExpr->args = list_make4(rteKindData, fragmentSchemaData,
									fragmentTableData, tableIdListData);

	fauxFunction = makeNode(RangeTblFunction);
	fauxFunction->funcexpr = (Node *) fauxFuncExpr;

	/* set the column count to pass ruleutils checks, not used elsewhere */
	fauxFunction->funccolcount = list_length(rte->eref->colnames);

	rte->rtekind = RTE_FUNCTION;
	rte->functions = list_make1(fauxFunction);
}


/*
 * ExtractRangeTblExtraData extracts extra data stored for a range table entry
 * that previously has been stored with
 * Set/ModifyRangeTblExtraData. Parameters can be NULL if unintersting. It is
 * valid to use the function on a RTE without extra data.
 */
void
ExtractRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind *rteKind,
						 char **fragmentSchemaName, char **fragmentTableName,
						 List **tableIdList)
{
	RangeTblFunction *fauxFunction = NULL;
	FuncExpr *fauxFuncExpr = NULL;
	Const *tmpConst = NULL;

	/* set base rte kind first, so this can be used for 'non-extended' RTEs as well */
	if (rteKind != NULL)
	{
		*rteKind = (CitusRTEKind) rte->rtekind;
	}

	/* reset values of optionally-present fields, will later be overwritten, if present */
	if (fragmentSchemaName != NULL)
	{
		*fragmentSchemaName = NULL;
	}

	if (fragmentTableName != NULL)
	{
		*fragmentTableName = NULL;
	}

	if (tableIdList != NULL)
	{
		*tableIdList = NIL;
	}


	/* only function RTEs have our special extra data */
	if (rte->rtekind != RTE_FUNCTION)
	{
		return;
	}

	/* we only ever generate one argument */
	if (list_length(rte->functions) != 1)
	{
		return;
	}

	/* should pretty much always be a FuncExpr, but be liberal in what we expect... */
	fauxFunction = linitial(rte->functions);
	if (!IsA(fauxFunction->funcexpr, FuncExpr))
	{
		return;
	}

	fauxFuncExpr = (FuncExpr *) fauxFunction->funcexpr;

	/*
	 * There will never be a range table entry with this function id, but for
	 * the purpose of this file.
	 */
	if (fauxFuncExpr->funcid != CitusExtraDataContainerFuncId())
	{
		return;
	}

	/*
	 * Extra data for rtes is stored in the function arguments. The first
	 * argument stores the rtekind, second fragmentSchemaName, third
	 * fragmentTableName, fourth tableIdList.
	 */
	if (list_length(fauxFuncExpr->args) != 4)
	{
		ereport(ERROR, (errmsg("unexpected number of function arguments to "
							   "citus_extradata_container")));
		return;
	}

	/* extract rteKind */
	tmpConst = (Const *) linitial(fauxFuncExpr->args);
	Assert(IsA(tmpConst, Const));
	Assert(tmpConst->consttype == INT4OID);
	if (rteKind != NULL)
	{
		*rteKind = DatumGetInt32(tmpConst->constvalue);
	}

	/* extract fragmentSchemaName */
	tmpConst = (Const *) lsecond(fauxFuncExpr->args);
	Assert(IsA(tmpConst, Const));
	Assert(tmpConst->consttype == CSTRINGOID);
	if (fragmentSchemaName != NULL && !tmpConst->constisnull)
	{
		*fragmentSchemaName = DatumGetCString(tmpConst->constvalue);
	}

	/* extract fragmentTableName */
	tmpConst = (Const *) lthird(fauxFuncExpr->args);
	Assert(IsA(tmpConst, Const));
	Assert(tmpConst->consttype == CSTRINGOID);
	if (fragmentTableName != NULL && !tmpConst->constisnull)
	{
		*fragmentTableName = DatumGetCString(tmpConst->constvalue);
	}

	/* extract tableIdList, stored as a serialized integer list */
	tmpConst = (Const *) lfourth(fauxFuncExpr->args);
	Assert(IsA(tmpConst, Const));
	Assert(tmpConst->consttype == CSTRINGOID);
	if (tableIdList != NULL && !tmpConst->constisnull)
	{
		Node *deserializedList = stringToNode(DatumGetCString(tmpConst->constvalue));
		Assert(IsA(deserializedList, IntList));

		*tableIdList = (List *) deserializedList;
	}
}


/*
 * ModifyRangeTblExtraData sets the RTE extra data fields for the passed
 * fields, leaving the current values in place for the ones not specified.
 *
 * rteKind has to be specified, fragmentSchemaName, fragmentTableName,
 * tableIdList can be set to NULL/NIL respectively to leave the current values
 * in-place.
 */
void
ModifyRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
						char *fragmentSchemaName, char *fragmentTableName,
						List *tableIdList)
{
	/* load existing values for the arguments not specifying a new value */
	ExtractRangeTblExtraData(rte, NULL,
							 fragmentSchemaName == NULL ? &fragmentSchemaName : NULL,
							 fragmentTableName == NULL ? &fragmentTableName : NULL,
							 tableIdList == NIL ? &tableIdList : NULL);

	SetRangeTblExtraData(rte, rteKind,
						 fragmentSchemaName, fragmentTableName,
						 tableIdList);
}


/* GetRangeTblKind returns rtekind of a RTE, be it an extended one or not. */
CitusRTEKind
GetRangeTblKind(RangeTblEntry *rte)
{
	CitusRTEKind rteKind = CITUS_RTE_RELATION /* invalid */;

	switch (rte->rtekind)
	{
		/* directly rtekind if it's not possibly an extended RTE */
		case RTE_RELATION:
		case RTE_SUBQUERY:
		case RTE_JOIN:
		case RTE_VALUES:
		case RTE_CTE:
		{
			rteKind = (CitusRTEKind) rte->rtekind;
			break;
		}

		case RTE_FUNCTION:
		{
			/*
			 * Extract extra data - correct even if a plain RTE_FUNCTION, not
			 * an extended one, ExtractRangeTblExtraData handles that case
			 * transparently.
			 */
			ExtractRangeTblExtraData(rte, &rteKind, NULL, NULL, NULL);
			break;
		}
	}

	return rteKind;
}


/*
 * citus_extradata_container is a placeholder function to store information
 * needed by Citus in plain postgres node trees. Executor and other hooks
 * should always intercept statements containing calls to this function. It's
 * not actually SQL callable by the user because of an INTERNAL argument.
 */
Datum
citus_extradata_container(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("not supposed to get here, did you cheat?")));

	PG_RETURN_NULL();
}


#if (PG_VERSION_NUM >= 90600)

static void
CopyUnsupportedCitusNode(struct ExtensibleNode *newnode,
						 const struct ExtensibleNode *oldnode)
{
	ereport(ERROR, (errmsg("not implemented")));
}


static bool
EqualUnsupportedCitusNode(const struct ExtensibleNode *a,
						  const struct ExtensibleNode *b)
{
	ereport(ERROR, (errmsg("not implemented")));
}


/* *INDENT-OFF* */
#define DEFINE_NODE_METHODS(type) \
	{ \
		#type, \
		sizeof(type), \
		CopyUnsupportedCitusNode, \
		EqualUnsupportedCitusNode, \
		Out##type, \
		Read##type \
	}

#define DEFINE_NODE_METHODS_NO_READ(type) \
	{ \
		#type, \
		sizeof(type), \
		CopyUnsupportedCitusNode, \
		EqualUnsupportedCitusNode, \
		Out##type, \
		ReadUnsupportedCitusNode \
	}


/* *INDENT-ON* */
const ExtensibleNodeMethods nodeMethods[] =
{
	DEFINE_NODE_METHODS(MultiPlan),
	DEFINE_NODE_METHODS(Job),
	DEFINE_NODE_METHODS(ShardInterval),
	DEFINE_NODE_METHODS(MapMergeJob),
	DEFINE_NODE_METHODS(ShardPlacement),
	DEFINE_NODE_METHODS(RelationShard),
	DEFINE_NODE_METHODS(Task),

	/* nodes with only output support */
	DEFINE_NODE_METHODS_NO_READ(MultiNode),
	DEFINE_NODE_METHODS_NO_READ(MultiTreeRoot),
	DEFINE_NODE_METHODS_NO_READ(MultiProject),
	DEFINE_NODE_METHODS_NO_READ(MultiCollect),
	DEFINE_NODE_METHODS_NO_READ(MultiSelect),
	DEFINE_NODE_METHODS_NO_READ(MultiTable),
	DEFINE_NODE_METHODS_NO_READ(MultiJoin),
	DEFINE_NODE_METHODS_NO_READ(MultiPartition),
	DEFINE_NODE_METHODS_NO_READ(MultiCartesianProduct),
	DEFINE_NODE_METHODS_NO_READ(MultiExtendedOp)
};
#endif

void
RegisterNodes(void)
{
#if (PG_VERSION_NUM >= 90600)
	int off;

	StaticAssertExpr(lengthof(nodeMethods) == lengthof(CitusNodeTagNamesD),
					 "number of node methods and names do not match");

	for (off = 0; off < lengthof(nodeMethods); off++)
	{
		RegisterExtensibleNodeMethods(&nodeMethods[off]);
	}
#endif
}
