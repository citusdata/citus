/*-------------------------------------------------------------------------
 *
 * procedure_body_analysis.c
 *
 * This file implements a PLpgSQL plugin that analyzes stored procedure
 * bodies at execution time (in the func_beg callback) to determine
 * whether the procedure contains a single distributed SQL statement.
 *
 * When citus.enable_procedure_transaction_skip is enabled, single-statement
 * procedures that target a single shard can skip coordinated (2PC)
 * transactions. Rather than using a fragile runtime counter that can lead
 * to partial commits, we analyze the procedure body *before* any statement
 * executes and set the ProcedureBodyIsSingleStatement flag.
 *
 * The analysis recursively walks the PLpgSQL statement tree counting
 * SQL-producing statements (EXECSQL, PERFORM, DYNEXECUTE) and detecting
 * disqualifying statements (COMMIT, ROLLBACK). If the body contains
 * exactly one SQL statement and no COMMIT/ROLLBACK, the flag is set to
 * true, allowing the executor to skip 2PC.
 *
 * For multi-statement procedures, the flag remains false and the normal
 * coordinated transaction path is used — no ERROR is raised, no partial
 * commits occur.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_proc.h"
#include "plpgsql.h"
#include "utils/elog.h"

#include "distributed/procedure_body_analysis.h"
#include "distributed/transaction_management.h"


/* flag set by func_beg to indicate whether the procedure body is a single statement */
bool ProcedureBodyIsSingleStatement = false;

/* saved previous plugin pointer for chaining */
static PLpgSQL_plugin *prev_plpgsql_plugin = NULL;

/* our plugin struct */
static PLpgSQL_plugin citus_plpgsql_plugin;


/*
 * WalkStatementList recursively walks a list of PLpgSQL statements,
 * counting SQL-producing statements and detecting disqualifiers.
 *
 * Returns the total count of SQL statements found so far. Sets
 * *disqualified to true if a COMMIT or ROLLBACK is found at any
 * nesting level. Short-circuits as soon as the result is determined
 * (count > 1 or disqualified).
 */
static int
WalkStatementList(List *stmts, bool *disqualified)
{
	int count = 0;
	ListCell *lc;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		/* short-circuit: already disqualified or enough statements counted */
		if (*disqualified || count > 1)
		{
			return count;
		}

		switch (stmt->cmd_type)
		{
			/* SQL-producing statements: count them */
			case PLPGSQL_STMT_EXECSQL:
			case PLPGSQL_STMT_PERFORM:
			case PLPGSQL_STMT_DYNEXECUTE:
			{
				count++;
				break;
			}

			/* CALL may invoke another procedure with distributed statements */
			case PLPGSQL_STMT_CALL:
			{
				count++;
				break;
			}

			/* COMMIT / ROLLBACK: procedure manages its own transactions */
			case PLPGSQL_STMT_COMMIT:
			case PLPGSQL_STMT_ROLLBACK:
			{
				*disqualified = true;
				return count;
			}

			/* Container statements: recurse into their bodies */
			case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *block = (PLpgSQL_stmt_block *) stmt;
				count += WalkStatementList(block->body, disqualified);

				/* Also walk exception handler bodies */
				if (block->exceptions != NULL)
				{
					ListCell *exc_lc;
					foreach(exc_lc, block->exceptions->exc_list)
					{
						PLpgSQL_exception *exc =
							(PLpgSQL_exception *) lfirst(exc_lc);
						count += WalkStatementList(exc->action, disqualified);
						if (*disqualified || count > 1)
						{
							return count;
						}
					}
				}
				break;
			}

			case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *if_stmt = (PLpgSQL_stmt_if *) stmt;
				count += WalkStatementList(if_stmt->then_body, disqualified);
				if (*disqualified || count > 1)
				{
					return count;
				}

				/* Walk ELSIF branches */
				ListCell *elsif_lc;
				foreach(elsif_lc, if_stmt->elsif_list)
				{
					PLpgSQL_if_elsif *elsif =
						(PLpgSQL_if_elsif *) lfirst(elsif_lc);
					count += WalkStatementList(elsif->stmts, disqualified);
					if (*disqualified || count > 1)
					{
						return count;
					}
				}

				count += WalkStatementList(if_stmt->else_body, disqualified);
				break;
			}

			case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *case_stmt = (PLpgSQL_stmt_case *) stmt;
				ListCell *when_lc;
				foreach(when_lc, case_stmt->case_when_list)
				{
					PLpgSQL_case_when *when =
						(PLpgSQL_case_when *) lfirst(when_lc);
					count += WalkStatementList(when->stmts, disqualified);
					if (*disqualified || count > 1)
					{
						return count;
					}
				}
				count += WalkStatementList(case_stmt->else_stmts, disqualified);
				break;
			}

			/*
			 * Loop containers: disqualify unconditionally. A SQL statement
			 * inside a loop may execute multiple times at runtime. Without
			 * a coordinated transaction each iteration auto-commits, so a
			 * failure mid-loop would leave prior iterations committed —
			 * exactly the partial-commit scenario we want to prevent.
			 */
			case PLPGSQL_STMT_LOOP:
			case PLPGSQL_STMT_WHILE:
			case PLPGSQL_STMT_FORI:
			case PLPGSQL_STMT_FORS:
			case PLPGSQL_STMT_FORC:
			case PLPGSQL_STMT_DYNFORS:
			case PLPGSQL_STMT_FOREACH_A:
			{
				*disqualified = true;
				return count;
			}

			/*
			 * Non-SQL, non-container statements (ASSIGN, RETURN, RAISE,
			 * GETDIAG, EXIT, OPEN, FETCH, CLOSE, ASSERT, RETURN_NEXT,
			 * RETURN_QUERY): do not count, do not recurse.
			 */
			default:
				break;
		}
	}

	return count;
}


/*
 * citus_func_beg is our PLpgSQL plugin func_beg callback.
 *
 * Called after local variables are initialized, before any statement
 * in the function body executes. We use this to analyze the procedure
 * body and set ProcedureBodyIsSingleStatement.
 */
static void
citus_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	/*
	 * Only analyze for top-level procedure calls (not nested) when the
	 * optimization GUC is enabled.
	 */
	if (EnableProcedureTransactionSkip &&
		func->fn_prokind == PROKIND_PROCEDURE &&
		StoredProcedureLevel == 1)
	{
		bool disqualified = false;
		int sqlCount = WalkStatementList(func->action->body, &disqualified);

		ProcedureBodyIsSingleStatement = (!disqualified && sqlCount == 1);

		if (ProcedureBodyIsSingleStatement)
		{
			ereport(DEBUG4, (errmsg("procedure body analysis: single-statement "
									"procedure detected, eligible for 2PC skip")));
		}
		else
		{
			ereport(DEBUG4, (errmsg("procedure body analysis: multi-statement "
									"or disqualified procedure detected "
									"(sql_count=%d, disqualified=%s), "
									"using coordinated transaction",
									sqlCount,
									disqualified ? "true" : "false")));
		}
	}

	/* chain to previous plugin if any */
	if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_beg)
	{
		prev_plpgsql_plugin->func_beg(estate, func);
	}
}


/*
 * citus_func_setup is our PLpgSQL plugin func_setup callback.
 * Chains to the previous plugin.
 */
static void
citus_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_setup)
	{
		prev_plpgsql_plugin->func_setup(estate, func);
	}
}


/*
 * citus_func_end is our PLpgSQL plugin func_end callback.
 * Resets the flag and chains to the previous plugin.
 */
static void
citus_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	/*
	 * Reset the flag when exiting a top-level procedure so it does
	 * not leak into subsequent calls.
	 */
	if (func->fn_prokind == PROKIND_PROCEDURE &&
		StoredProcedureLevel == 1)
	{
		ProcedureBodyIsSingleStatement = false;
	}

	if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_end)
	{
		prev_plpgsql_plugin->func_end(estate, func);
	}
}


/*
 * citus_stmt_beg is our PLpgSQL plugin stmt_beg callback.
 * Chains to the previous plugin.
 */
static void
citus_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_beg)
	{
		prev_plpgsql_plugin->stmt_beg(estate, stmt);
	}
}


/*
 * citus_stmt_end is our PLpgSQL plugin stmt_end callback.
 * Chains to the previous plugin.
 */
static void
citus_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_end)
	{
		prev_plpgsql_plugin->stmt_end(estate, stmt);
	}
}


/*
 * InstallProcedureBodyAnalysisPlugin registers our PLpgSQL plugin
 * via the rendezvous variable mechanism. Must be called from _PG_init().
 */
void
InstallProcedureBodyAnalysisPlugin(void)
{
	PLpgSQL_plugin **plugin_ptr =
		(PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");

	/* save any previously installed plugin for chaining */
	prev_plpgsql_plugin = *plugin_ptr;

	/* set up our plugin callbacks */
	citus_plpgsql_plugin.func_setup = citus_func_setup;
	citus_plpgsql_plugin.func_beg = citus_func_beg;
	citus_plpgsql_plugin.func_end = citus_func_end;
	citus_plpgsql_plugin.stmt_beg = citus_stmt_beg;
	citus_plpgsql_plugin.stmt_end = citus_stmt_end;

	/* install our plugin */
	*plugin_ptr = &citus_plpgsql_plugin;
}
