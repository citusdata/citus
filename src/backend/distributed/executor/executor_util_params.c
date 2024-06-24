/*-------------------------------------------------------------------------
 *
 * executor_util_tasks.c
 *
 * Utility functions for dealing with task lists in the executor.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "utils/lsyscache.h"

#include "distributed/executor_util.h"


/*
 * ExtractParametersForRemoteExecution extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * It changes oid of custom types to InvalidOid so that they are the same in workers
 * and coordinators.
 */
void
ExtractParametersForRemoteExecution(ParamListInfo paramListInfo, Oid **parameterTypes,
									const char ***parameterValues)
{
	ExtractParametersFromParamList(paramListInfo, parameterTypes,
								   parameterValues, false);
}


/*
 * ExtractParametersFromParamList extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * If useOriginalCustomTypeOids is true, it uses the original oids for custom types.
 */
void
ExtractParametersFromParamList(ParamListInfo paramListInfo,
							   Oid **parameterTypes,
							   const char ***parameterValues, bool
							   useOriginalCustomTypeOids)
{
	int parameterCount = paramListInfo->numParams;

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the coordinator and worker nodes. Therefore, the worker nodes can
		 * infer the correct oid.
		 */
		if (parameterData->ptype >= FirstNormalObjectId && !useOriginalCustomTypeOids)
		{
			(*parameterTypes)[parameterIndex] = 0;
		}
		else
		{
			(*parameterTypes)[parameterIndex] = parameterData->ptype;
		}

		/*
		 * If the parameter is not referenced / used (ptype == 0) and
		 * would otherwise have errored out inside standard_planner()),
		 * don't pass a value to the remote side, and pass text oid to prevent
		 * undetermined data type errors on workers.
		 */
		if (parameterData->ptype == 0)
		{
			(*parameterValues)[parameterIndex] = NULL;
			(*parameterTypes)[parameterIndex] = TEXTOID;

			continue;
		}

		/*
		 * If the parameter is NULL then we preserve its type, but
		 * don't need to evaluate its value.
		 */
		if (parameterData->isnull)
		{
			(*parameterValues)[parameterIndex] = NULL;

			continue;
		}

		getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId,
						  &variableLengthType);

		(*parameterValues)[parameterIndex] = OidOutputFunctionCall(typeOutputFunctionId,
																   parameterData->value);
	}
}
