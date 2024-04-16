/*-------------------------------------------------------------------------
 *
 * param_utils.c
 *	  Utilities to process paramaters.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

#include "distributed/param_utils.h"

/*
 * IsExternParamUsedInQuery returns true if the passed in paramId
 * is used in the query, false otherwise.
 */
bool
GetParamsUsedInQuery(Node *expression, Bitmapset **paramBitmap)
{
	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, Param))
	{
		Param *param = (Param *) expression;
		int paramId = param->paramid;

		/* only care about user supplied parameters */
		if (param->paramkind != PARAM_EXTERN)
		{
			return false;
		}

		/* Found a parameter, mark it in the bitmap and continue */
		*paramBitmap = bms_add_member(*paramBitmap, paramId);

		/* Continue searching */
		return false;
	}

	/* keep traversing */
	if (IsA(expression, Query))
	{
		return query_tree_walker((Query *) expression,
								 GetParamsUsedInQuery,
								 paramBitmap,
								 0);
	}
	else
	{
		return expression_tree_walker(expression,
									  GetParamsUsedInQuery,
									  paramBitmap);
	}
}


/*
 * MarkUnreferencedExternParams marks parameter's type to zero if the
 * parameter is not used in the query.
 */
void
MarkUnreferencedExternParams(Node *expression, ParamListInfo boundParams)
{
	int parameterCount = boundParams->numParams;
	Bitmapset *paramBitmap = NULL;

	/* Fetch all parameters used in the query */
	GetParamsUsedInQuery(expression, &paramBitmap);

	/* Check for any missing parameters */
	for (int parameterNum = 1; parameterNum <= parameterCount; parameterNum++)
	{
		if (!bms_is_member(parameterNum, paramBitmap))
		{
			boundParams->params[parameterNum - 1].ptype = 0;
		}
	}
}
