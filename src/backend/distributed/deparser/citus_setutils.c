#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/print.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/log_utils.h"


void AppendVarSetValue(StringInfo buf, VariableSetStmt *setStmt);

/*
 * AppendVarSetValueDb deparses a VariableSetStmt with VAR_SET_VALUE kind.
 * It takes from flatten_set_variable_args in postgres's utils/misc/guc.c,
 * however flatten_set_variable_args does not apply correct quoting.
 */
void
AppendVarSetValue(StringInfo buf, VariableSetStmt *setStmt)
{
	ListCell *varArgCell = NULL;
	ListCell *firstCell = list_head(setStmt->args);

	Assert(setStmt->kind == VAR_SET_VALUE);

	foreach(varArgCell, setStmt->args)
	{
		Node *varArgNode = lfirst(varArgCell);
		A_Const *varArgConst = NULL;
		TypeName *typeName = NULL;

		if (IsA(varArgNode, A_Const))
		{
			varArgConst = (A_Const *) varArgNode;
		}
		else if (IsA(varArgNode, TypeCast))
		{
			TypeCast *varArgTypeCast = (TypeCast *) varArgNode;

			varArgConst = castNode(A_Const, varArgTypeCast->arg);
			typeName = varArgTypeCast->typeName;
		}
		else
		{
			elog(ERROR, "unrecognized node type: %d", varArgNode->type);
		}

		/* don't know how to start SET until we inspect first arg */
		if (varArgCell != firstCell)
		{
			appendStringInfoChar(buf, ',');
		}
		else if (typeName != NULL)
		{
			appendStringInfoString(buf, " SET TIME ZONE");
		}
		else
		{
			appendStringInfo(buf, " SET %s =", quote_identifier(setStmt->name));
		}

		Node *value = (Node *) &varArgConst->val;
		switch (value->type)
		{
			case T_Integer:
			{
				appendStringInfo(buf, " %d", intVal(value));
				break;
			}

			case T_Float:
			{
				appendStringInfo(buf, " %s", nodeToString(value));
				break;
			}

			case T_String:
			{
				if (typeName != NULL)
				{
					/*
					 * Must be a ConstInterval argument for TIME ZONE. Coerce
					 * to interval and back to normalize the value and account
					 * for any typmod.
					 */
					Oid typoid = InvalidOid;
					int32 typmod = -1;

					typenameTypeIdAndMod(NULL, typeName, &typoid, &typmod);
					Assert(typoid == INTERVALOID);

					Datum interval =
						DirectFunctionCall3(interval_in,
											CStringGetDatum(strVal(value)),
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(typmod));

					char *intervalout =
						DatumGetCString(DirectFunctionCall1(interval_out,
															interval));
					appendStringInfo(buf, " INTERVAL '%s'", intervalout);
				}
				else
				{
					appendStringInfo(buf, " %s", quote_literal_cstr(strVal(value)));
				}
				break;
			}

			default:
			{
				elog(ERROR, "Unexpected Value type in VAR_SET_VALUE arguments.");
				break;
			}
		}
	}
}


/*
 * AppendVariableSetDb appends a string representing the VariableSetStmt to a buffer
 */
void
AppendVariableSet(StringInfo buf, VariableSetStmt *setStmt)
{
	switch (setStmt->kind)
	{
		case VAR_SET_VALUE:
		{
			AppendVarSetValue(buf, setStmt);
			break;
		}

		case VAR_SET_CURRENT:
		{
			appendStringInfo(buf, " SET %s FROM CURRENT", quote_identifier(
								 setStmt->name));
			break;
		}

		case VAR_SET_DEFAULT:
		{
			appendStringInfo(buf, " SET %s TO DEFAULT", quote_identifier(setStmt->name));
			break;
		}

		case VAR_RESET:
		{
			appendStringInfo(buf, " RESET %s", quote_identifier(setStmt->name));
			break;
		}

		case VAR_RESET_ALL:
		{
			appendStringInfoString(buf, " RESET ALL");
			break;
		}

		/* VAR_SET_MULTI is a special case for SET TRANSACTION that should not occur here */
		case VAR_SET_MULTI:
		default:
		{
			ereport(ERROR, (errmsg("Unable to deparse SET statement")));
			break;
		}
	}
}
