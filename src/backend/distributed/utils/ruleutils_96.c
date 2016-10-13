/*-------------------------------------------------------------------------
 *
 * ruleutils.c
 *	  Functions to convert stored expressions/querytrees back to
 *	  source text
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ruleutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_language.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "common/keywords.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "parser/parse_node.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteSupport.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "utils/xml.h"


/* ----------
 * Pretty formatting constants
 * ----------
 */

/* Indent counts */
#define PRETTYINDENT_STD		8
#define PRETTYINDENT_JOIN		4
#define PRETTYINDENT_VAR		4

#define PRETTYINDENT_LIMIT		40		/* wrap limit */

/* Pretty flags */
#define PRETTYFLAG_PAREN		1
#define PRETTYFLAG_INDENT		2

/* Default line length for pretty-print wrapping: 0 means wrap always */
#define WRAP_COLUMN_DEFAULT		0

/* macro to test if pretty action needed */
#define PRETTY_PAREN(context)	((context)->prettyFlags & PRETTYFLAG_PAREN)
#define PRETTY_INDENT(context)	((context)->prettyFlags & PRETTYFLAG_INDENT)


/* ----------
 * Local data types
 * ----------
 */

/* Context info needed for invoking a recursive querytree display routine */
typedef struct
{
	StringInfo	buf;			/* output buffer to append to */
	List	   *namespaces;		/* List of deparse_namespace nodes */
	List	   *windowClause;	/* Current query level's WINDOW clause */
	List	   *windowTList;	/* targetlist for resolving WINDOW clause */
	int			prettyFlags;	/* enabling of pretty-print functions */
	int			wrapColumn;		/* max line length, or -1 for no limit */
	int			indentLevel;	/* current indent level for prettyprint */
	bool		varprefix;		/* TRUE to print prefixes on Vars */
	ParseExprKind special_exprkind;		/* set only for exprkinds needing
										 * special handling */
} deparse_context;

/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * The rangetable is the list of actual RTEs from the query tree, and the
 * cte list is the list of actual CTEs.
 *
 * rtable_names holds the alias name to be used for each RTE (either a C
 * string, or NULL for nameless RTEs such as unnamed joins).
 * rtable_columns holds the column alias names to be used for each RTE.
 *
 * In some cases we need to make names of merged JOIN USING columns unique
 * across the whole query, not only per-RTE.  If so, unique_using is TRUE
 * and using_names is a list of C strings representing names already assigned
 * to USING columns.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the PlanState node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * PlanState's ancestors.  In addition, we store its outer and inner subplan
 * state nodes, as well as their plan nodes' targetlists, and the index tlist
 * if the current plan node might contain INDEX_VAR Vars.  (These fields could
 * be derived on-the-fly from the current PlanState, but it seems notationally
 * clearer to set them up as separate fields.)
 */
typedef struct
{
	List	   *rtable;			/* List of RangeTblEntry nodes */
	List	   *rtable_names;	/* Parallel list of names for RTEs */
	List	   *rtable_columns; /* Parallel list of deparse_columns structs */
	List	   *ctes;			/* List of CommonTableExpr nodes */
	/* Workspace for column alias assignment: */
	bool		unique_using;	/* Are we making USING names globally unique */
	List	   *using_names;	/* List of assigned names for USING columns */
	/* Remaining fields are used only when deparsing a Plan tree: */
	PlanState  *planstate;		/* immediate parent of current expression */
	List	   *ancestors;		/* ancestors of planstate */
	PlanState  *outer_planstate;	/* outer subplan state, or NULL if none */
	PlanState  *inner_planstate;	/* inner subplan state, or NULL if none */
	List	   *outer_tlist;	/* referent for OUTER_VAR Vars */
	List	   *inner_tlist;	/* referent for INNER_VAR Vars */
	List	   *index_tlist;	/* referent for INDEX_VAR Vars */
} deparse_namespace;

/*
 * Per-relation data about column alias names.
 *
 * Selecting aliases is unreasonably complicated because of the need to dump
 * rules/views whose underlying tables may have had columns added, deleted, or
 * renamed since the query was parsed.  We must nonetheless print the rule/view
 * in a form that can be reloaded and will produce the same results as before.
 *
 * For each RTE used in the query, we must assign column aliases that are
 * unique within that RTE.  SQL does not require this of the original query,
 * but due to factors such as *-expansion we need to be able to uniquely
 * reference every column in a decompiled query.  As long as we qualify all
 * column references, per-RTE uniqueness is sufficient for that.
 *
 * However, we can't ensure per-column name uniqueness for unnamed join RTEs,
 * since they just inherit column names from their input RTEs, and we can't
 * rename the columns at the join level.  Most of the time this isn't an issue
 * because we don't need to reference the join's output columns as such; we
 * can reference the input columns instead.  That approach can fail for merged
 * JOIN USING columns, however, so when we have one of those in an unnamed
 * join, we have to make that column's alias globally unique across the whole
 * query to ensure it can be referenced unambiguously.
 *
 * Another problem is that a JOIN USING clause requires the columns to be
 * merged to have the same aliases in both input RTEs, and that no other
 * columns in those RTEs or their children conflict with the USING names.
 * To handle that, we do USING-column alias assignment in a recursive
 * traversal of the query's jointree.  When descending through a JOIN with
 * USING, we preassign the USING column names to the child columns, overriding
 * other rules for column alias assignment.  We also mark each RTE with a list
 * of all USING column names selected for joins containing that RTE, so that
 * when we assign other columns' aliases later, we can avoid conflicts.
 *
 * Another problem is that if a JOIN's input tables have had columns added or
 * deleted since the query was parsed, we must generate a column alias list
 * for the join that matches the current set of input columns --- otherwise, a
 * change in the number of columns in the left input would throw off matching
 * of aliases to columns of the right input.  Thus, positions in the printable
 * column alias list are not necessarily one-for-one with varattnos of the
 * JOIN, so we need a separate new_colnames[] array for printing purposes.
 */
typedef struct
{
	/*
	 * colnames is an array containing column aliases to use for columns that
	 * existed when the query was parsed.  Dropped columns have NULL entries.
	 * This array can be directly indexed by varattno to get a Var's name.
	 *
	 * Non-NULL entries are guaranteed unique within the RTE, *except* when
	 * this is for an unnamed JOIN RTE.  In that case we merely copy up names
	 * from the two input RTEs.
	 *
	 * During the recursive descent in set_using_names(), forcible assignment
	 * of a child RTE's column name is represented by pre-setting that element
	 * of the child's colnames array.  So at that stage, NULL entries in this
	 * array just mean that no name has been preassigned, not necessarily that
	 * the column is dropped.
	 */
	int			num_cols;		/* length of colnames[] array */
	char	  **colnames;		/* array of C strings and NULLs */

	/*
	 * new_colnames is an array containing column aliases to use for columns
	 * that would exist if the query was re-parsed against the current
	 * definitions of its base tables.  This is what to print as the column
	 * alias list for the RTE.  This array does not include dropped columns,
	 * but it will include columns added since original parsing.  Indexes in
	 * it therefore have little to do with current varattno values.  As above,
	 * entries are unique unless this is for an unnamed JOIN RTE.  (In such an
	 * RTE, we never actually print this array, but we must compute it anyway
	 * for possible use in computing column names of upper joins.) The
	 * parallel array is_new_col marks which of these columns are new since
	 * original parsing.  Entries with is_new_col false must match the
	 * non-NULL colnames entries one-for-one.
	 */
	int			num_new_cols;	/* length of new_colnames[] array */
	char	  **new_colnames;	/* array of C strings */
	bool	   *is_new_col;		/* array of bool flags */

	/* This flag tells whether we should actually print a column alias list */
	bool		printaliases;

	/* This list has all names used as USING names in joins above this RTE */
	List	   *parentUsing;	/* names assigned to parent merged columns */

	/*
	 * If this struct is for a JOIN RTE, we fill these fields during the
	 * set_using_names() pass to describe its relationship to its child RTEs.
	 *
	 * leftattnos and rightattnos are arrays with one entry per existing
	 * output column of the join (hence, indexable by join varattno).  For a
	 * simple reference to a column of the left child, leftattnos[i] is the
	 * child RTE's attno and rightattnos[i] is zero; and conversely for a
	 * column of the right child.  But for merged columns produced by JOIN
	 * USING/NATURAL JOIN, both leftattnos[i] and rightattnos[i] are nonzero.
	 * Also, if the column has been dropped, both are zero.
	 *
	 * If it's a JOIN USING, usingNames holds the alias names selected for the
	 * merged columns (these might be different from the original USING list,
	 * if we had to modify names to achieve uniqueness).
	 */
	int			leftrti;		/* rangetable index of left child */
	int			rightrti;		/* rangetable index of right child */
	int		   *leftattnos;		/* left-child varattnos of join cols, or 0 */
	int		   *rightattnos;	/* right-child varattnos of join cols, or 0 */
	List	   *usingNames;		/* names assigned to merged columns */
} deparse_columns;

/* This macro is analogous to rt_fetch(), but for deparse_columns structs */
#define deparse_columns_fetch(rangetable_index, dpns) \
	((deparse_columns *) list_nth((dpns)->rtable_columns, (rangetable_index)-1))

/*
 * Entry in set_rtable_names' hash table
 */
typedef struct
{
	char		name[NAMEDATALEN];		/* Hash key --- must be first */
	int			counter;		/* Largest addition used so far for name */
} NameHashEntry;


/* ----------
 * Global data
 * ----------
 */
static SPIPlanPtr plan_getrulebyoid = NULL;
static const char *query_getrulebyoid = "SELECT * FROM pg_catalog.pg_rewrite WHERE oid = $1";
static SPIPlanPtr plan_getviewrule = NULL;
static const char *query_getviewrule = "SELECT * FROM pg_catalog.pg_rewrite WHERE ev_class = $1 AND rulename = $2";

/* GUC parameters */
bool		quote_all_identifiers = false;


/* ----------
 * Local functions
 *
 * Most of these functions used to use fixed-size buffers to build their
 * results.  Now, they take an (already initialized) StringInfo object
 * as a parameter, and append their text output to its contents.
 * ----------
 */
static char *deparse_expression_pretty(Node *expr, List *dpcontext,
						  bool forceprefix, bool showimplicit,
						  int prettyFlags, int startIndent);
static char *pg_get_viewdef_worker(Oid viewoid,
					  int prettyFlags, int wrapColumn);
static char *pg_get_triggerdef_worker(Oid trigid, bool pretty);
static void decompile_column_index_array(Datum column_index_array, Oid relId,
							 StringInfo buf);
static char *pg_get_ruledef_worker(Oid ruleoid, int prettyFlags);
static char *pg_get_indexdef_worker(Oid indexrelid, int colno,
					   const Oid *excludeOps,
					   bool attrsOnly, bool showTblSpc,
					   int prettyFlags, bool missing_ok);
static char *pg_get_constraintdef_worker(Oid constraintId, bool fullCommand,
							int prettyFlags, bool missing_ok);
static text *pg_get_expr_worker(text *expr, Oid relid, const char *relname,
				   int prettyFlags);
static int print_function_arguments(StringInfo buf, HeapTuple proctup,
						 bool print_table_args, bool print_defaults);
static void print_function_rettype(StringInfo buf, HeapTuple proctup);
static void print_function_trftypes(StringInfo buf, HeapTuple proctup);
static void set_rtable_names(deparse_namespace *dpns, List *parent_namespaces,
				 Bitmapset *rels_used);
static void set_deparse_for_query(deparse_namespace *dpns, Query *query,
					  List *parent_namespaces);
static void set_simple_column_names(deparse_namespace *dpns);
static bool has_dangerous_join_using(deparse_namespace *dpns, Node *jtnode);
static void set_using_names(deparse_namespace *dpns, Node *jtnode,
				List *parentUsing);
static void set_relation_column_names(deparse_namespace *dpns,
						  RangeTblEntry *rte,
						  deparse_columns *colinfo);
static void set_join_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
					  deparse_columns *colinfo);
static bool colname_is_unique(char *colname, deparse_namespace *dpns,
				  deparse_columns *colinfo);
static char *make_colname_unique(char *colname, deparse_namespace *dpns,
					deparse_columns *colinfo);
static void expand_colnames_array_to(deparse_columns *colinfo, int n);
static void identify_join_columns(JoinExpr *j, RangeTblEntry *jrte,
					  deparse_columns *colinfo);
static void flatten_join_using_qual(Node *qual,
						List **leftvars, List **rightvars);
static char *get_rtable_name(int rtindex, deparse_context *context);
static void set_deparse_planstate(deparse_namespace *dpns, PlanState *ps);
static void push_child_plan(deparse_namespace *dpns, PlanState *ps,
				deparse_namespace *save_dpns);
static void pop_child_plan(deparse_namespace *dpns,
			   deparse_namespace *save_dpns);
static void push_ancestor_plan(deparse_namespace *dpns, ListCell *ancestor_cell,
				   deparse_namespace *save_dpns);
static void pop_ancestor_plan(deparse_namespace *dpns,
				  deparse_namespace *save_dpns);
static void make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags);
static void make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags, int wrapColumn);
static void get_query_def(Query *query, StringInfo buf, List *parentnamespace,
			  TupleDesc resultDesc,
			  int prettyFlags, int wrapColumn, int startIndent);
static void get_values_def(List *values_lists, deparse_context *context);
static void get_with_clause(Query *query, deparse_context *context);
static void get_select_query_def(Query *query, deparse_context *context,
					 TupleDesc resultDesc);
static void get_insert_query_def(Query *query, deparse_context *context);
static void get_update_query_def(Query *query, deparse_context *context);
static void get_update_query_targetlist_def(Query *query, List *targetList,
								deparse_context *context,
								RangeTblEntry *rte);
static void get_delete_query_def(Query *query, deparse_context *context);
static void get_utility_query_def(Query *query, deparse_context *context);
static void get_basic_select_query(Query *query, deparse_context *context,
					   TupleDesc resultDesc);
static void get_target_list(List *targetList, deparse_context *context,
				TupleDesc resultDesc);
static void get_setop_query(Node *setOp, Query *query,
				deparse_context *context,
				TupleDesc resultDesc);
static Node *get_rule_sortgroupclause(Index ref, List *tlist,
						 bool force_colno,
						 deparse_context *context);
static void get_rule_groupingset(GroupingSet *gset, List *targetlist,
					 bool omit_parens, deparse_context *context);
static void get_rule_orderby(List *orderList, List *targetList,
				 bool force_colno, deparse_context *context);
static void get_rule_windowclause(Query *query, deparse_context *context);
static void get_rule_windowspec(WindowClause *wc, List *targetList,
					deparse_context *context);
static char *get_variable(Var *var, int levelsup, bool istoplevel,
			 deparse_context *context);
static void get_special_variable(Node *node, deparse_context *context,
					 void *private);
static void resolve_special_varno(Node *node, deparse_context *context,
					  void *private,
					  void (*callback) (Node *, deparse_context *, void *));
static Node *find_param_referent(Param *param, deparse_context *context,
					deparse_namespace **dpns_p, ListCell **ancestor_cell_p);
static void get_parameter(Param *param, deparse_context *context);
static const char *get_simple_binary_op_name(OpExpr *expr);
static bool isSimpleNode(Node *node, Node *parentNode, int prettyFlags);
static void appendContextKeyword(deparse_context *context, const char *str,
					 int indentBefore, int indentAfter, int indentPlus);
static void removeStringInfoSpaces(StringInfo str);
static void get_rule_expr(Node *node, deparse_context *context,
			  bool showimplicit);
static void get_rule_expr_toplevel(Node *node, deparse_context *context,
					   bool showimplicit);
static void get_oper_expr(OpExpr *expr, deparse_context *context);
static void get_func_expr(FuncExpr *expr, deparse_context *context,
			  bool showimplicit);
static void get_agg_expr(Aggref *aggref, deparse_context *context,
			 Aggref *original_aggref);
static void get_agg_combine_expr(Node *node, deparse_context *context,
					 void *private);
static void get_windowfunc_expr(WindowFunc *wfunc, deparse_context *context);
static void get_coercion_expr(Node *arg, deparse_context *context,
				  Oid resulttype, int32 resulttypmod,
				  Node *parentNode);
static void get_const_expr(Const *constval, deparse_context *context,
			   int showtype);
static void get_const_collation(Const *constval, deparse_context *context);
static void simple_quote_literal(StringInfo buf, const char *val);
static void get_sublink_expr(SubLink *sublink, deparse_context *context);
static void get_from_clause(Query *query, const char *prefix,
				deparse_context *context);
static void get_from_clause_item(Node *jtnode, Query *query,
					 deparse_context *context);
static void get_column_alias_list(deparse_columns *colinfo,
					  deparse_context *context);
static void get_from_clause_coldeflist(RangeTblFunction *rtfunc,
						   deparse_columns *colinfo,
						   deparse_context *context);
static void get_tablesample_def(TableSampleClause *tablesample,
					deparse_context *context);
static void get_opclass_name(Oid opclass, Oid actual_datatype,
				 StringInfo buf);
static Node *processIndirection(Node *node, deparse_context *context);
static void printSubscripts(ArrayRef *aref, deparse_context *context);
static char *get_relation_name(Oid relid);
static char *generate_relation_name(Oid relid, List *namespaces);
static char *generate_qualified_relation_name(Oid relid);
static char *generate_function_name(Oid funcid, int nargs,
					   List *argnames, Oid *argtypes,
					   bool has_variadic, bool *use_variadic_p,
					   ParseExprKind special_exprkind);
static char *generate_operator_name(Oid operid, Oid arg1, Oid arg2);
static text *string_to_text(char *str);
static char *flatten_reloptions(Oid relid);

#define only_marker(rte)  ((rte)->inh ? "" : "ONLY ")


/* ----------
 * get_ruledef			- Do it all and return a text
 *				  that could be used as a statement
 *				  to recreate the rule
 * ----------
 */
Datum
pg_get_ruledef(PG_FUNCTION_ARGS)
{
	Oid			ruleoid = PG_GETARG_OID(0);
	int			prettyFlags;
	char	   *res;

	prettyFlags = PRETTYFLAG_INDENT;

	res = pg_get_ruledef_worker(ruleoid, prettyFlags);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}


Datum
pg_get_ruledef_ext(PG_FUNCTION_ARGS)
{
	Oid			ruleoid = PG_GETARG_OID(0);
	bool		pretty = PG_GETARG_BOOL(1);
	int			prettyFlags;
	char	   *res;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	res = pg_get_ruledef_worker(ruleoid, prettyFlags);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}


static char *
pg_get_ruledef_worker(Oid ruleoid, int prettyFlags)
{
	Datum		args[1];
	char		nulls[1];
	int			spirc;
	HeapTuple	ruletup;
	TupleDesc	rulettc;
	StringInfoData buf;

	/*
	 * Do this first so that string is alloc'd in outer context not SPI's.
	 */
	initStringInfo(&buf);

	/*
	 * Connect to SPI manager
	 */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * On the first call prepare the plan to lookup pg_rewrite. We read
	 * pg_rewrite over the SPI manager instead of using the syscache to be
	 * checked for read access on pg_rewrite.
	 */
	if (plan_getrulebyoid == NULL)
	{
		Oid			argtypes[1];
		SPIPlanPtr	plan;

		argtypes[0] = OIDOID;
		plan = SPI_prepare(query_getrulebyoid, 1, argtypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed for \"%s\"", query_getrulebyoid);
		SPI_keepplan(plan);
		plan_getrulebyoid = plan;
	}

	/*
	 * Get the pg_rewrite tuple for this rule
	 */
	args[0] = ObjectIdGetDatum(ruleoid);
	nulls[0] = ' ';
	spirc = SPI_execute_plan(plan_getrulebyoid, args, nulls, true, 0);
	if (spirc != SPI_OK_SELECT)
		elog(ERROR, "failed to get pg_rewrite tuple for rule %u", ruleoid);
	if (SPI_processed != 1)
	{
		/*
		 * There is no tuple data available here, just keep the output buffer
		 * empty.
		 */
	}
	else
	{
		/*
		 * Get the rule's definition and put it into executor's memory
		 */
		ruletup = SPI_tuptable->vals[0];
		rulettc = SPI_tuptable->tupdesc;
		make_ruledef(&buf, ruletup, rulettc, prettyFlags);
	}

	/*
	 * Disconnect from SPI manager
	 */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	if (buf.len == 0)
		return NULL;

	return buf.data;
}


/* ----------
 * get_viewdef			- Mainly the same thing, but we
 *				  only return the SELECT part of a view
 * ----------
 */
Datum
pg_get_viewdef(PG_FUNCTION_ARGS)
{
	/* By OID */
	Oid			viewoid = PG_GETARG_OID(0);
	int			prettyFlags;
	char	   *res;

	prettyFlags = PRETTYFLAG_INDENT;

	res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}


Datum
pg_get_viewdef_ext(PG_FUNCTION_ARGS)
{
	/* By OID */
	Oid			viewoid = PG_GETARG_OID(0);
	bool		pretty = PG_GETARG_BOOL(1);
	int			prettyFlags;
	char	   *res;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

Datum
pg_get_viewdef_wrap(PG_FUNCTION_ARGS)
{
	/* By OID */
	Oid			viewoid = PG_GETARG_OID(0);
	int			wrap = PG_GETARG_INT32(1);
	int			prettyFlags;
	char	   *res;

	/* calling this implies we want pretty printing */
	prettyFlags = PRETTYFLAG_PAREN | PRETTYFLAG_INDENT;

	res = pg_get_viewdef_worker(viewoid, prettyFlags, wrap);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

Datum
pg_get_viewdef_name(PG_FUNCTION_ARGS)
{
	/* By qualified name */
	text	   *viewname = PG_GETARG_TEXT_P(0);
	int			prettyFlags;
	RangeVar   *viewrel;
	Oid			viewoid;
	char	   *res;

	prettyFlags = PRETTYFLAG_INDENT;

	/* Look up view name.  Can't lock it - we might not have privileges. */
	viewrel = makeRangeVarFromNameList(textToQualifiedNameList(viewname));
	viewoid = RangeVarGetRelid(viewrel, NoLock, false);

	res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}


Datum
pg_get_viewdef_name_ext(PG_FUNCTION_ARGS)
{
	/* By qualified name */
	text	   *viewname = PG_GETARG_TEXT_P(0);
	bool		pretty = PG_GETARG_BOOL(1);
	int			prettyFlags;
	RangeVar   *viewrel;
	Oid			viewoid;
	char	   *res;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	/* Look up view name.  Can't lock it - we might not have privileges. */
	viewrel = makeRangeVarFromNameList(textToQualifiedNameList(viewname));
	viewoid = RangeVarGetRelid(viewrel, NoLock, false);

	res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

/*
 * Common code for by-OID and by-name variants of pg_get_viewdef
 */
static char *
pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn)
{
	Datum		args[2];
	char		nulls[2];
	int			spirc;
	HeapTuple	ruletup;
	TupleDesc	rulettc;
	StringInfoData buf;

	/*
	 * Do this first so that string is alloc'd in outer context not SPI's.
	 */
	initStringInfo(&buf);

	/*
	 * Connect to SPI manager
	 */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * On the first call prepare the plan to lookup pg_rewrite. We read
	 * pg_rewrite over the SPI manager instead of using the syscache to be
	 * checked for read access on pg_rewrite.
	 */
	if (plan_getviewrule == NULL)
	{
		Oid			argtypes[2];
		SPIPlanPtr	plan;

		argtypes[0] = OIDOID;
		argtypes[1] = NAMEOID;
		plan = SPI_prepare(query_getviewrule, 2, argtypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed for \"%s\"", query_getviewrule);
		SPI_keepplan(plan);
		plan_getviewrule = plan;
	}

	/*
	 * Get the pg_rewrite tuple for the view's SELECT rule
	 */
	args[0] = ObjectIdGetDatum(viewoid);
	args[1] = DirectFunctionCall1(namein, CStringGetDatum(ViewSelectRuleName));
	nulls[0] = ' ';
	nulls[1] = ' ';
	spirc = SPI_execute_plan(plan_getviewrule, args, nulls, true, 0);
	if (spirc != SPI_OK_SELECT)
		elog(ERROR, "failed to get pg_rewrite tuple for view %u", viewoid);
	if (SPI_processed != 1)
	{
		/*
		 * There is no tuple data available here, just keep the output buffer
		 * empty.
		 */
	}
	else
	{
		/*
		 * Get the rule's definition and put it into executor's memory
		 */
		ruletup = SPI_tuptable->vals[0];
		rulettc = SPI_tuptable->tupdesc;
		make_viewdef(&buf, ruletup, rulettc, prettyFlags, wrapColumn);
	}

	/*
	 * Disconnect from SPI manager
	 */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	if (buf.len == 0)
		return NULL;

	return buf.data;
}

/* ----------
 * get_triggerdef			- Get the definition of a trigger
 * ----------
 */
Datum
pg_get_triggerdef(PG_FUNCTION_ARGS)
{
	Oid			trigid = PG_GETARG_OID(0);
	char	   *res;

	res = pg_get_triggerdef_worker(trigid, false);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

Datum
pg_get_triggerdef_ext(PG_FUNCTION_ARGS)
{
	Oid			trigid = PG_GETARG_OID(0);
	bool		pretty = PG_GETARG_BOOL(1);
	char	   *res;

	res = pg_get_triggerdef_worker(trigid, pretty);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

static char *
pg_get_triggerdef_worker(Oid trigid, bool pretty)
{
	HeapTuple	ht_trig;
	Form_pg_trigger trigrec;
	StringInfoData buf;
	Relation	tgrel;
	ScanKeyData skey[1];
	SysScanDesc tgscan;
	int			findx = 0;
	char	   *tgname;
	Oid			argtypes[1];	/* dummy */
	Datum		value;
	bool		isnull;

	/*
	 * Fetch the pg_trigger tuple by the Oid of the trigger
	 */
	tgrel = heap_open(TriggerRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(trigid));

	tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true,
								NULL, 1, skey);

	ht_trig = systable_getnext(tgscan);

	if (!HeapTupleIsValid(ht_trig))
	{
		systable_endscan(tgscan);
		heap_close(tgrel, AccessShareLock);
		return NULL;
	}

	trigrec = (Form_pg_trigger) GETSTRUCT(ht_trig);

	/*
	 * Start the trigger definition. Note that the trigger's name should never
	 * be schema-qualified, but the trigger rel's name may be.
	 */
	initStringInfo(&buf);

	tgname = NameStr(trigrec->tgname);
	appendStringInfo(&buf, "CREATE %sTRIGGER %s ",
					 OidIsValid(trigrec->tgconstraint) ? "CONSTRAINT " : "",
					 quote_identifier(tgname));

	if (TRIGGER_FOR_BEFORE(trigrec->tgtype))
		appendStringInfoString(&buf, "BEFORE");
	else if (TRIGGER_FOR_AFTER(trigrec->tgtype))
		appendStringInfoString(&buf, "AFTER");
	else if (TRIGGER_FOR_INSTEAD(trigrec->tgtype))
		appendStringInfoString(&buf, "INSTEAD OF");
	else
		elog(ERROR, "unexpected tgtype value: %d", trigrec->tgtype);

	if (TRIGGER_FOR_INSERT(trigrec->tgtype))
	{
		appendStringInfoString(&buf, " INSERT");
		findx++;
	}
	if (TRIGGER_FOR_DELETE(trigrec->tgtype))
	{
		if (findx > 0)
			appendStringInfoString(&buf, " OR DELETE");
		else
			appendStringInfoString(&buf, " DELETE");
		findx++;
	}
	if (TRIGGER_FOR_UPDATE(trigrec->tgtype))
	{
		if (findx > 0)
			appendStringInfoString(&buf, " OR UPDATE");
		else
			appendStringInfoString(&buf, " UPDATE");
		findx++;
		/* tgattr is first var-width field, so OK to access directly */
		if (trigrec->tgattr.dim1 > 0)
		{
			int			i;

			appendStringInfoString(&buf, " OF ");
			for (i = 0; i < trigrec->tgattr.dim1; i++)
			{
				char	   *attname;

				if (i > 0)
					appendStringInfoString(&buf, ", ");
				attname = get_relid_attribute_name(trigrec->tgrelid,
												   trigrec->tgattr.values[i]);
				appendStringInfoString(&buf, quote_identifier(attname));
			}
		}
	}
	if (TRIGGER_FOR_TRUNCATE(trigrec->tgtype))
	{
		if (findx > 0)
			appendStringInfoString(&buf, " OR TRUNCATE");
		else
			appendStringInfoString(&buf, " TRUNCATE");
		findx++;
	}
	appendStringInfo(&buf, " ON %s ",
					 generate_relation_name(trigrec->tgrelid, NIL));

	if (OidIsValid(trigrec->tgconstraint))
	{
		if (OidIsValid(trigrec->tgconstrrelid))
			appendStringInfo(&buf, "FROM %s ",
						generate_relation_name(trigrec->tgconstrrelid, NIL));
		if (!trigrec->tgdeferrable)
			appendStringInfoString(&buf, "NOT ");
		appendStringInfoString(&buf, "DEFERRABLE INITIALLY ");
		if (trigrec->tginitdeferred)
			appendStringInfoString(&buf, "DEFERRED ");
		else
			appendStringInfoString(&buf, "IMMEDIATE ");
	}

	if (TRIGGER_FOR_ROW(trigrec->tgtype))
		appendStringInfoString(&buf, "FOR EACH ROW ");
	else
		appendStringInfoString(&buf, "FOR EACH STATEMENT ");

	/* If the trigger has a WHEN qualification, add that */
	value = fastgetattr(ht_trig, Anum_pg_trigger_tgqual,
						tgrel->rd_att, &isnull);
	if (!isnull)
	{
		Node	   *qual;
		char		relkind;
		deparse_context context;
		deparse_namespace dpns;
		RangeTblEntry *oldrte;
		RangeTblEntry *newrte;

		appendStringInfoString(&buf, "WHEN (");

		qual = stringToNode(TextDatumGetCString(value));

		relkind = get_rel_relkind(trigrec->tgrelid);

		/* Build minimal OLD and NEW RTEs for the rel */
		oldrte = makeNode(RangeTblEntry);
		oldrte->rtekind = RTE_RELATION;
		oldrte->relid = trigrec->tgrelid;
		oldrte->relkind = relkind;
		oldrte->alias = makeAlias("old", NIL);
		oldrte->eref = oldrte->alias;
		oldrte->lateral = false;
		oldrte->inh = false;
		oldrte->inFromCl = true;

		newrte = makeNode(RangeTblEntry);
		newrte->rtekind = RTE_RELATION;
		newrte->relid = trigrec->tgrelid;
		newrte->relkind = relkind;
		newrte->alias = makeAlias("new", NIL);
		newrte->eref = newrte->alias;
		newrte->lateral = false;
		newrte->inh = false;
		newrte->inFromCl = true;

		/* Build two-element rtable */
		memset(&dpns, 0, sizeof(dpns));
		dpns.rtable = list_make2(oldrte, newrte);
		dpns.ctes = NIL;
		set_rtable_names(&dpns, NIL, NULL);
		set_simple_column_names(&dpns);

		/* Set up context with one-deep namespace stack */
		context.buf = &buf;
		context.namespaces = list_make1(&dpns);
		context.windowClause = NIL;
		context.windowTList = NIL;
		context.varprefix = true;
		context.prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;
		context.wrapColumn = WRAP_COLUMN_DEFAULT;
		context.indentLevel = PRETTYINDENT_STD;
		context.special_exprkind = EXPR_KIND_NONE;

		get_rule_expr(qual, &context, false);

		appendStringInfoString(&buf, ") ");
	}

	appendStringInfo(&buf, "EXECUTE PROCEDURE %s(",
					 generate_function_name(trigrec->tgfoid, 0,
											NIL, argtypes,
											false, NULL, EXPR_KIND_NONE));

	if (trigrec->tgnargs > 0)
	{
		char	   *p;
		int			i;

		value = fastgetattr(ht_trig, Anum_pg_trigger_tgargs,
							tgrel->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "tgargs is null for trigger %u", trigid);
		p = (char *) VARDATA(DatumGetByteaP(value));
		for (i = 0; i < trigrec->tgnargs; i++)
		{
			if (i > 0)
				appendStringInfoString(&buf, ", ");
			simple_quote_literal(&buf, p);
			/* advance p to next string embedded in tgargs */
			while (*p)
				p++;
			p++;
		}
	}

	/* We deliberately do not put semi-colon at end */
	appendStringInfoChar(&buf, ')');

	/* Clean up */
	systable_endscan(tgscan);

	heap_close(tgrel, AccessShareLock);

	return buf.data;
}

/* ----------
 * get_indexdef			- Get the definition of an index
 *
 * In the extended version, there is a colno argument as well as pretty bool.
 *	if colno == 0, we want a complete index definition.
 *	if colno > 0, we only want the Nth index key's variable or expression.
 *
 * Note that the SQL-function versions of this omit any info about the
 * index tablespace; this is intentional because pg_dump wants it that way.
 * However pg_get_indexdef_string() includes index tablespace if not default.
 * ----------
 */
Datum
pg_get_indexdef(PG_FUNCTION_ARGS)
{
	Oid			indexrelid = PG_GETARG_OID(0);
	int			prettyFlags;
	char	   *res;

	prettyFlags = PRETTYFLAG_INDENT;

	res = pg_get_indexdef_worker(indexrelid, 0, NULL, false, false,
								 prettyFlags, true);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

Datum
pg_get_indexdef_ext(PG_FUNCTION_ARGS)
{
	Oid			indexrelid = PG_GETARG_OID(0);
	int32		colno = PG_GETARG_INT32(1);
	bool		pretty = PG_GETARG_BOOL(2);
	int			prettyFlags;
	char	   *res;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	res = pg_get_indexdef_worker(indexrelid, colno, NULL, colno != 0, false,
								 prettyFlags, true);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

/* Internal version that returns a palloc'd C string; no pretty-printing */
char *
pg_get_indexdef_string(Oid indexrelid)
{
	return pg_get_indexdef_worker(indexrelid, 0, NULL, false, true, 0, false);
}

/* Internal version that just reports the column definitions */
char *
pg_get_indexdef_columns(Oid indexrelid, bool pretty)
{
	int			prettyFlags;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;
	return pg_get_indexdef_worker(indexrelid, 0, NULL, true, false,
								  prettyFlags, false);
}

/*
 * Internal workhorse to decompile an index definition.
 *
 * This is now used for exclusion constraints as well: if excludeOps is not
 * NULL then it points to an array of exclusion operator OIDs.
 */
static char *
pg_get_indexdef_worker(Oid indexrelid, int colno,
					   const Oid *excludeOps,
					   bool attrsOnly, bool showTblSpc,
					   int prettyFlags, bool missing_ok)
{
	/* might want a separate isConstraint parameter later */
	bool		isConstraint = (excludeOps != NULL);
	HeapTuple	ht_idx;
	HeapTuple	ht_idxrel;
	HeapTuple	ht_am;
	Form_pg_index idxrec;
	Form_pg_class idxrelrec;
	Form_pg_am	amrec;
	IndexAmRoutine *amroutine;
	List	   *indexprs;
	ListCell   *indexpr_item;
	List	   *context;
	Oid			indrelid;
	int			keyno;
	Datum		indcollDatum;
	Datum		indclassDatum;
	Datum		indoptionDatum;
	bool		isnull;
	oidvector  *indcollation;
	oidvector  *indclass;
	int2vector *indoption;
	StringInfoData buf;
	char	   *str;
	char	   *sep;

	/*
	 * Fetch the pg_index tuple by the Oid of the index
	 */
	ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idx))
	{
		if (missing_ok)
			return NULL;
		elog(ERROR, "cache lookup failed for index %u", indexrelid);
	}
	idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

	indrelid = idxrec->indrelid;
	Assert(indexrelid == idxrec->indexrelid);

	/* Must get indcollation, indclass, and indoption the hard way */
	indcollDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
								   Anum_pg_index_indcollation, &isnull);
	Assert(!isnull);
	indcollation = (oidvector *) DatumGetPointer(indcollDatum);

	indclassDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);

	indoptionDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									 Anum_pg_index_indoption, &isnull);
	Assert(!isnull);
	indoption = (int2vector *) DatumGetPointer(indoptionDatum);

	/*
	 * Fetch the pg_class tuple of the index relation
	 */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation %u", indexrelid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/*
	 * Fetch the pg_am tuple of the index' access method
	 */
	ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
	if (!HeapTupleIsValid(ht_am))
		elog(ERROR, "cache lookup failed for access method %u",
			 idxrelrec->relam);
	amrec = (Form_pg_am) GETSTRUCT(ht_am);

	/* Fetch the index AM's API struct */
	amroutine = GetIndexAmRoutine(amrec->amhandler);

	/*
	 * Get the index expressions, if any.  (NOTE: we do not use the relcache
	 * versions of the expressions and predicate, because we want to display
	 * non-const-folded expressions.)
	 */
	if (!heap_attisnull(ht_idx, Anum_pg_index_indexprs))
	{
		Datum		exprsDatum;
		bool		isnull;
		char	   *exprsString;

		exprsDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									 Anum_pg_index_indexprs, &isnull);
		Assert(!isnull);
		exprsString = TextDatumGetCString(exprsDatum);
		indexprs = (List *) stringToNode(exprsString);
		pfree(exprsString);
	}
	else
		indexprs = NIL;

	indexpr_item = list_head(indexprs);

	context = deparse_context_for(get_relation_name(indrelid), indrelid);

	/*
	 * Start the index definition.  Note that the index's name should never be
	 * schema-qualified, but the indexed rel's name may be.
	 */
	initStringInfo(&buf);

	if (!attrsOnly)
	{
		if (!isConstraint)
			appendStringInfo(&buf, "CREATE %sINDEX %s ON %s USING %s (",
							 idxrec->indisunique ? "UNIQUE " : "",
							 quote_identifier(NameStr(idxrelrec->relname)),
							 generate_relation_name(indrelid, NIL),
							 quote_identifier(NameStr(amrec->amname)));
		else	/* currently, must be EXCLUDE constraint */
			appendStringInfo(&buf, "EXCLUDE USING %s (",
							 quote_identifier(NameStr(amrec->amname)));
	}

	/*
	 * Report the indexed attributes
	 */
	sep = "";
	for (keyno = 0; keyno < idxrec->indnatts; keyno++)
	{
		AttrNumber	attnum = idxrec->indkey.values[keyno];
		int16		opt = indoption->values[keyno];
		Oid			keycoltype;
		Oid			keycolcollation;

		if (!colno)
			appendStringInfoString(&buf, sep);
		sep = ", ";

		if (attnum != 0)
		{
			/* Simple index column */
			char	   *attname;
			int32		keycoltypmod;

			attname = get_relid_attribute_name(indrelid, attnum);
			if (!colno || colno == keyno + 1)
				appendStringInfoString(&buf, quote_identifier(attname));
			get_atttypetypmodcoll(indrelid, attnum,
								  &keycoltype, &keycoltypmod,
								  &keycolcollation);
		}
		else
		{
			/* expressional index */
			Node	   *indexkey;

			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);
			/* Deparse */
			str = deparse_expression_pretty(indexkey, context, false, false,
											prettyFlags, 0);
			if (!colno || colno == keyno + 1)
			{
				/* Need parens if it's not a bare function call */
				if (indexkey && IsA(indexkey, FuncExpr) &&
				 ((FuncExpr *) indexkey)->funcformat == COERCE_EXPLICIT_CALL)
					appendStringInfoString(&buf, str);
				else
					appendStringInfo(&buf, "(%s)", str);
			}
			keycoltype = exprType(indexkey);
			keycolcollation = exprCollation(indexkey);
		}

		if (!attrsOnly && (!colno || colno == keyno + 1))
		{
			Oid			indcoll;

			/* Add collation, if not default for column */
			indcoll = indcollation->values[keyno];
			if (OidIsValid(indcoll) && indcoll != keycolcollation)
				appendStringInfo(&buf, " COLLATE %s",
								 generate_collation_name((indcoll)));

			/* Add the operator class name, if not default */
			get_opclass_name(indclass->values[keyno], keycoltype, &buf);

			/* Add options if relevant */
			if (amroutine->amcanorder)
			{
				/* if it supports sort ordering, report DESC and NULLS opts */
				if (opt & INDOPTION_DESC)
				{
					appendStringInfoString(&buf, " DESC");
					/* NULLS FIRST is the default in this case */
					if (!(opt & INDOPTION_NULLS_FIRST))
						appendStringInfoString(&buf, " NULLS LAST");
				}
				else
				{
					if (opt & INDOPTION_NULLS_FIRST)
						appendStringInfoString(&buf, " NULLS FIRST");
				}
			}

			/* Add the exclusion operator if relevant */
			if (excludeOps != NULL)
				appendStringInfo(&buf, " WITH %s",
								 generate_operator_name(excludeOps[keyno],
														keycoltype,
														keycoltype));
		}
	}

	if (!attrsOnly)
	{
		appendStringInfoChar(&buf, ')');

		/*
		 * If it has options, append "WITH (options)"
		 */
		str = flatten_reloptions(indexrelid);
		if (str)
		{
			appendStringInfo(&buf, " WITH (%s)", str);
			pfree(str);
		}

		/*
		 * If it's in a nondefault tablespace, say so, but only if requested
		 */
		if (showTblSpc)
		{
			Oid			tblspc;

			tblspc = get_rel_tablespace(indexrelid);
			if (OidIsValid(tblspc))
			{
				if (isConstraint)
					appendStringInfoString(&buf, " USING INDEX");
				appendStringInfo(&buf, " TABLESPACE %s",
							  quote_identifier(get_tablespace_name(tblspc)));
			}
		}

		/*
		 * If it's a partial index, decompile and append the predicate
		 */
		if (!heap_attisnull(ht_idx, Anum_pg_index_indpred))
		{
			Node	   *node;
			Datum		predDatum;
			bool		isnull;
			char	   *predString;

			/* Convert text string to node tree */
			predDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
										Anum_pg_index_indpred, &isnull);
			Assert(!isnull);
			predString = TextDatumGetCString(predDatum);
			node = (Node *) stringToNode(predString);
			pfree(predString);

			/* Deparse */
			str = deparse_expression_pretty(node, context, false, false,
											prettyFlags, 0);
			if (isConstraint)
				appendStringInfo(&buf, " WHERE (%s)", str);
			else
				appendStringInfo(&buf, " WHERE %s", str);
		}
	}

	/* Clean up */
	ReleaseSysCache(ht_idx);
	ReleaseSysCache(ht_idxrel);
	ReleaseSysCache(ht_am);

	return buf.data;
}


/*
 * pg_get_constraintdef
 *
 * Returns the definition for the constraint, ie, everything that needs to
 * appear after "ALTER TABLE ... ADD CONSTRAINT <constraintname>".
 */
Datum
pg_get_constraintdef(PG_FUNCTION_ARGS)
{
	Oid			constraintId = PG_GETARG_OID(0);
	int			prettyFlags;
	char	   *res;

	prettyFlags = PRETTYFLAG_INDENT;

	res = pg_get_constraintdef_worker(constraintId, false, prettyFlags, true);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

Datum
pg_get_constraintdef_ext(PG_FUNCTION_ARGS)
{
	Oid			constraintId = PG_GETARG_OID(0);
	bool		pretty = PG_GETARG_BOOL(1);
	int			prettyFlags;
	char	   *res;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	res = pg_get_constraintdef_worker(constraintId, false, prettyFlags, true);

	if (res == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(string_to_text(res));
}

/*
 * Internal version that returns a full ALTER TABLE ... ADD CONSTRAINT command
 */
char *
pg_get_constraintdef_command(Oid constraintId)
{
	return pg_get_constraintdef_worker(constraintId, true, 0, false);
}

/*
 * As of 9.4, we now use an MVCC snapshot for this.
 */
static char *
pg_get_constraintdef_worker(Oid constraintId, bool fullCommand,
							int prettyFlags, bool missing_ok)
{
	HeapTuple	tup;
	Form_pg_constraint conForm;
	StringInfoData buf;
	SysScanDesc scandesc;
	ScanKeyData scankey[1];
	Snapshot	snapshot = RegisterSnapshot(GetTransactionSnapshot());
	Relation	relation = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scankey[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(constraintId));

	scandesc = systable_beginscan(relation,
								  ConstraintOidIndexId,
								  true,
								  snapshot,
								  1,
								  scankey);

	/*
	 * We later use the tuple with SysCacheGetAttr() as if we had obtained it
	 * via SearchSysCache, which works fine.
	 */
	tup = systable_getnext(scandesc);

	UnregisterSnapshot(snapshot);

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
		{
			systable_endscan(scandesc);
			heap_close(relation, AccessShareLock);
			return NULL;
		}
		elog(ERROR, "cache lookup failed for constraint %u", constraintId);
	}

	conForm = (Form_pg_constraint) GETSTRUCT(tup);

	initStringInfo(&buf);

	if (fullCommand)
	{
		/*
		 * Currently, callers want ALTER TABLE (without ONLY) for CHECK
		 * constraints, and other types of constraints don't inherit anyway so
		 * it doesn't matter whether we say ONLY or not.  Someday we might
		 * need to let callers specify whether to put ONLY in the command.
		 */
		appendStringInfo(&buf, "ALTER TABLE %s ADD CONSTRAINT %s ",
						 generate_qualified_relation_name(conForm->conrelid),
						 quote_identifier(NameStr(conForm->conname)));
	}

	switch (conForm->contype)
	{
		case CONSTRAINT_FOREIGN:
			{
				Datum		val;
				bool		isnull;
				const char *string;

				/* Start off the constraint definition */
				appendStringInfoString(&buf, "FOREIGN KEY (");

				/* Fetch and build referencing-column list */
				val = SysCacheGetAttr(CONSTROID, tup,
									  Anum_pg_constraint_conkey, &isnull);
				if (isnull)
					elog(ERROR, "null conkey for constraint %u",
						 constraintId);

				decompile_column_index_array(val, conForm->conrelid, &buf);

				/* add foreign relation name */
				appendStringInfo(&buf, ") REFERENCES %s(",
								 generate_relation_name(conForm->confrelid,
														NIL));

				/* Fetch and build referenced-column list */
				val = SysCacheGetAttr(CONSTROID, tup,
									  Anum_pg_constraint_confkey, &isnull);
				if (isnull)
					elog(ERROR, "null confkey for constraint %u",
						 constraintId);

				decompile_column_index_array(val, conForm->confrelid, &buf);

				appendStringInfoChar(&buf, ')');

				/* Add match type */
				switch (conForm->confmatchtype)
				{
					case FKCONSTR_MATCH_FULL:
						string = " MATCH FULL";
						break;
					case FKCONSTR_MATCH_PARTIAL:
						string = " MATCH PARTIAL";
						break;
					case FKCONSTR_MATCH_SIMPLE:
						string = "";
						break;
					default:
						elog(ERROR, "unrecognized confmatchtype: %d",
							 conForm->confmatchtype);
						string = "";	/* keep compiler quiet */
						break;
				}
				appendStringInfoString(&buf, string);

				/* Add ON UPDATE and ON DELETE clauses, if needed */
				switch (conForm->confupdtype)
				{
					case FKCONSTR_ACTION_NOACTION:
						string = NULL;	/* suppress default */
						break;
					case FKCONSTR_ACTION_RESTRICT:
						string = "RESTRICT";
						break;
					case FKCONSTR_ACTION_CASCADE:
						string = "CASCADE";
						break;
					case FKCONSTR_ACTION_SETNULL:
						string = "SET NULL";
						break;
					case FKCONSTR_ACTION_SETDEFAULT:
						string = "SET DEFAULT";
						break;
					default:
						elog(ERROR, "unrecognized confupdtype: %d",
							 conForm->confupdtype);
						string = NULL;	/* keep compiler quiet */
						break;
				}
				if (string)
					appendStringInfo(&buf, " ON UPDATE %s", string);

				switch (conForm->confdeltype)
				{
					case FKCONSTR_ACTION_NOACTION:
						string = NULL;	/* suppress default */
						break;
					case FKCONSTR_ACTION_RESTRICT:
						string = "RESTRICT";
						break;
					case FKCONSTR_ACTION_CASCADE:
						string = "CASCADE";
						break;
					case FKCONSTR_ACTION_SETNULL:
						string = "SET NULL";
						break;
					case FKCONSTR_ACTION_SETDEFAULT:
						string = "SET DEFAULT";
						break;
					default:
						elog(ERROR, "unrecognized confdeltype: %d",
							 conForm->confdeltype);
						string = NULL;	/* keep compiler quiet */
						break;
				}
				if (string)
					appendStringInfo(&buf, " ON DELETE %s", string);

				break;
			}
		case CONSTRAINT_PRIMARY:
		case CONSTRAINT_UNIQUE:
			{
				Datum		val;
				bool		isnull;
				Oid			indexId;

				/* Start off the constraint definition */
				if (conForm->contype == CONSTRAINT_PRIMARY)
					appendStringInfoString(&buf, "PRIMARY KEY (");
				else
					appendStringInfoString(&buf, "UNIQUE (");

				/* Fetch and build target column list */
				val = SysCacheGetAttr(CONSTROID, tup,
									  Anum_pg_constraint_conkey, &isnull);
				if (isnull)
					elog(ERROR, "null conkey for constraint %u",
						 constraintId);

				decompile_column_index_array(val, conForm->conrelid, &buf);

				appendStringInfoChar(&buf, ')');

				indexId = get_constraint_index(constraintId);

				/* XXX why do we only print these bits if fullCommand? */
				if (fullCommand && OidIsValid(indexId))
				{
					char	   *options = flatten_reloptions(indexId);
					Oid			tblspc;

					if (options)
					{
						appendStringInfo(&buf, " WITH (%s)", options);
						pfree(options);
					}

					tblspc = get_rel_tablespace(indexId);
					if (OidIsValid(tblspc))
						appendStringInfo(&buf, " USING INDEX TABLESPACE %s",
							  quote_identifier(get_tablespace_name(tblspc)));
				}

				break;
			}
		case CONSTRAINT_CHECK:
			{
				Datum		val;
				bool		isnull;
				char	   *conbin;
				char	   *consrc;
				Node	   *expr;
				List	   *context;

				/* Fetch constraint expression in parsetree form */
				val = SysCacheGetAttr(CONSTROID, tup,
									  Anum_pg_constraint_conbin, &isnull);
				if (isnull)
					elog(ERROR, "null conbin for constraint %u",
						 constraintId);

				conbin = TextDatumGetCString(val);
				expr = stringToNode(conbin);

				/* Set up deparsing context for Var nodes in constraint */
				if (conForm->conrelid != InvalidOid)
				{
					/* relation constraint */
					context = deparse_context_for(get_relation_name(conForm->conrelid),
												  conForm->conrelid);
				}
				else
				{
					/* domain constraint --- can't have Vars */
					context = NIL;
				}

				consrc = deparse_expression_pretty(expr, context, false, false,
												   prettyFlags, 0);

				/*
				 * Now emit the constraint definition, adding NO INHERIT if
				 * necessary.
				 *
				 * There are cases where the constraint expression will be
				 * fully parenthesized and we don't need the outer parens ...
				 * but there are other cases where we do need 'em.  Be
				 * conservative for now.
				 *
				 * Note that simply checking for leading '(' and trailing ')'
				 * would NOT be good enough, consider "(x > 0) AND (y > 0)".
				 */
				appendStringInfo(&buf, "CHECK (%s)%s",
								 consrc,
								 conForm->connoinherit ? " NO INHERIT" : "");
				break;
			}
		case CONSTRAINT_TRIGGER:

			/*
			 * There isn't an ALTER TABLE syntax for creating a user-defined
			 * constraint trigger, but it seems better to print something than
			 * throw an error; if we throw error then this function couldn't
			 * safely be applied to all rows of pg_constraint.
			 */
			appendStringInfoString(&buf, "TRIGGER");
			break;
		case CONSTRAINT_EXCLUSION:
			{
				Oid			indexOid = conForm->conindid;
				Datum		val;
				bool		isnull;
				Datum	   *elems;
				int			nElems;
				int			i;
				Oid		   *operators;

				/* Extract operator OIDs from the pg_constraint tuple */
				val = SysCacheGetAttr(CONSTROID, tup,
									  Anum_pg_constraint_conexclop,
									  &isnull);
				if (isnull)
					elog(ERROR, "null conexclop for constraint %u",
						 constraintId);

				deconstruct_array(DatumGetArrayTypeP(val),
								  OIDOID, sizeof(Oid), true, 'i',
								  &elems, NULL, &nElems);

				operators = (Oid *) palloc(nElems * sizeof(Oid));
				for (i = 0; i < nElems; i++)
					operators[i] = DatumGetObjectId(elems[i]);

				/* pg_get_indexdef_worker does the rest */
				/* suppress tablespace because pg_dump wants it that way */
				appendStringInfoString(&buf,
									   pg_get_indexdef_worker(indexOid,
															  0,
															  operators,
															  false,
															  false,
															  prettyFlags,
															  false));
				break;
			}
		default:
			elog(ERROR, "invalid constraint type \"%c\"", conForm->contype);
			break;
	}

	if (conForm->condeferrable)
		appendStringInfoString(&buf, " DEFERRABLE");
	if (conForm->condeferred)
		appendStringInfoString(&buf, " INITIALLY DEFERRED");
	if (!conForm->convalidated)
		appendStringInfoString(&buf, " NOT VALID");

	/* Cleanup */
	systable_endscan(scandesc);
	heap_close(relation, AccessShareLock);

	return buf.data;
}


/*
 * Convert an int16[] Datum into a comma-separated list of column names
 * for the indicated relation; append the list to buf.
 */
static void
decompile_column_index_array(Datum column_index_array, Oid relId,
							 StringInfo buf)
{
	Datum	   *keys;
	int			nKeys;
	int			j;

	/* Extract data from array of int16 */
	deconstruct_array(DatumGetArrayTypeP(column_index_array),
					  INT2OID, 2, true, 's',
					  &keys, NULL, &nKeys);

	for (j = 0; j < nKeys; j++)
	{
		char	   *colName;

		colName = get_relid_attribute_name(relId, DatumGetInt16(keys[j]));

		if (j == 0)
			appendStringInfoString(buf, quote_identifier(colName));
		else
			appendStringInfo(buf, ", %s", quote_identifier(colName));
	}
}


/* ----------
 * get_expr			- Decompile an expression tree
 *
 * Input: an expression tree in nodeToString form, and a relation OID
 *
 * Output: reverse-listed expression
 *
 * Currently, the expression can only refer to a single relation, namely
 * the one specified by the second parameter.  This is sufficient for
 * partial indexes, column default expressions, etc.  We also support
 * Var-free expressions, for which the OID can be InvalidOid.
 * ----------
 */
Datum
pg_get_expr(PG_FUNCTION_ARGS)
{
	text	   *expr = PG_GETARG_TEXT_P(0);
	Oid			relid = PG_GETARG_OID(1);
	int			prettyFlags;
	char	   *relname;

	prettyFlags = PRETTYFLAG_INDENT;

	if (OidIsValid(relid))
	{
		/* Get the name for the relation */
		relname = get_rel_name(relid);

		/*
		 * If the OID isn't actually valid, don't throw an error, just return
		 * NULL.  This is a bit questionable, but it's what we've done
		 * historically, and it can help avoid unwanted failures when
		 * examining catalog entries for just-deleted relations.
		 */
		if (relname == NULL)
			PG_RETURN_NULL();
	}
	else
		relname = NULL;

	PG_RETURN_TEXT_P(pg_get_expr_worker(expr, relid, relname, prettyFlags));
}

Datum
pg_get_expr_ext(PG_FUNCTION_ARGS)
{
	text	   *expr = PG_GETARG_TEXT_P(0);
	Oid			relid = PG_GETARG_OID(1);
	bool		pretty = PG_GETARG_BOOL(2);
	int			prettyFlags;
	char	   *relname;

	prettyFlags = pretty ? PRETTYFLAG_PAREN | PRETTYFLAG_INDENT : PRETTYFLAG_INDENT;

	if (OidIsValid(relid))
	{
		/* Get the name for the relation */
		relname = get_rel_name(relid);
		/* See notes above */
		if (relname == NULL)
			PG_RETURN_NULL();
	}
	else
		relname = NULL;

	PG_RETURN_TEXT_P(pg_get_expr_worker(expr, relid, relname, prettyFlags));
}

static text *
pg_get_expr_worker(text *expr, Oid relid, const char *relname, int prettyFlags)
{
	Node	   *node;
	List	   *context;
	char	   *exprstr;
	char	   *str;

	/* Convert input TEXT object to C string */
	exprstr = text_to_cstring(expr);

	/* Convert expression to node tree */
	node = (Node *) stringToNode(exprstr);

	pfree(exprstr);

	/* Prepare deparse context if needed */
	if (OidIsValid(relid))
		context = deparse_context_for(relname, relid);
	else
		context = NIL;

	/* Deparse */
	str = deparse_expression_pretty(node, context, false, false,
									prettyFlags, 0);

	return string_to_text(str);
}


/* ----------
 * get_userbyid			- Get a user name by roleid and
 *				  fallback to 'unknown (OID=n)'
 * ----------
 */
Datum
pg_get_userbyid(PG_FUNCTION_ARGS)
{
	Oid			roleid = PG_GETARG_OID(0);
	Name		result;
	HeapTuple	roletup;
	Form_pg_authid role_rec;

	/*
	 * Allocate space for the result
	 */
	result = (Name) palloc(NAMEDATALEN);
	memset(NameStr(*result), 0, NAMEDATALEN);

	/*
	 * Get the pg_authid entry and print the result
	 */
	roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (HeapTupleIsValid(roletup))
	{
		role_rec = (Form_pg_authid) GETSTRUCT(roletup);
		StrNCpy(NameStr(*result), NameStr(role_rec->rolname), NAMEDATALEN);
		ReleaseSysCache(roletup);
	}
	else
		sprintf(NameStr(*result), "unknown (OID=%u)", roleid);

	PG_RETURN_NAME(result);
}


/*
 * pg_get_serial_sequence
 *		Get the name of the sequence used by a serial column,
 *		formatted suitably for passing to setval, nextval or currval.
 *		First parameter is not treated as double-quoted, second parameter
 *		is --- see documentation for reason.
 */
Datum
pg_get_serial_sequence(PG_FUNCTION_ARGS)
{
	text	   *tablename = PG_GETARG_TEXT_P(0);
	text	   *columnname = PG_GETARG_TEXT_PP(1);
	RangeVar   *tablerv;
	Oid			tableOid;
	char	   *column;
	AttrNumber	attnum;
	Oid			sequenceId = InvalidOid;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	tup;

	/* Look up table name.  Can't lock it - we might not have privileges. */
	tablerv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
	tableOid = RangeVarGetRelid(tablerv, NoLock, false);

	/* Get the number of the column */
	column = text_to_cstring(columnname);

	attnum = get_attnum(tableOid, column);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						column, tablerv->relname)));

	/* Search the dependency table for the dependent sequence */
	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tableOid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_refobjsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(attnum));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any auto dependency of a sequence on a column must be
		 * what we are looking for.  (We need the relkind test because indexes
		 * can also have auto dependencies on columns.)
		 */
		if (deprec->classid == RelationRelationId &&
			deprec->objsubid == 0 &&
			deprec->deptype == DEPENDENCY_AUTO &&
			get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE)
		{
			sequenceId = deprec->objid;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(depRel, AccessShareLock);

	if (OidIsValid(sequenceId))
	{
		char	   *result;

		result = generate_qualified_relation_name(sequenceId);

		PG_RETURN_TEXT_P(string_to_text(result));
	}

	PG_RETURN_NULL();
}


/*
 * pg_get_functiondef
 *		Returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 *		the specified function.
 *
 * Note: if you change the output format of this function, be careful not
 * to break psql's rules (in \ef and \sf) for identifying the start of the
 * function body.  To wit: the function body starts on a line that begins
 * with "AS ", and no preceding line will look like that.
 */
Datum
pg_get_functiondef(PG_FUNCTION_ARGS)
{
	Oid			funcid = PG_GETARG_OID(0);
	StringInfoData buf;
	StringInfoData dq;
	HeapTuple	proctup;
	Form_pg_proc proc;
	Datum		tmp;
	bool		isnull;
	const char *prosrc;
	const char *name;
	const char *nsp;
	float4		procost;
	int			oldlen;

	initStringInfo(&buf);

	/* Look up the function */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		PG_RETURN_NULL();

	proc = (Form_pg_proc) GETSTRUCT(proctup);
	name = NameStr(proc->proname);

	if (proc->proisagg)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an aggregate function", name)));

	/*
	 * We always qualify the function name, to ensure the right function gets
	 * replaced.
	 */
	nsp = get_namespace_name(proc->pronamespace);
	appendStringInfo(&buf, "CREATE OR REPLACE FUNCTION %s(",
					 quote_qualified_identifier(nsp, name));
	(void) print_function_arguments(&buf, proctup, false, true);
	appendStringInfoString(&buf, ")\n RETURNS ");
	print_function_rettype(&buf, proctup);

	print_function_trftypes(&buf, proctup);

	appendStringInfo(&buf, "\n LANGUAGE %s\n",
				  quote_identifier(get_language_name(proc->prolang, false)));

	/* Emit some miscellaneous options on one line */
	oldlen = buf.len;

	if (proc->proiswindow)
		appendStringInfoString(&buf, " WINDOW");
	switch (proc->provolatile)
	{
		case PROVOLATILE_IMMUTABLE:
			appendStringInfoString(&buf, " IMMUTABLE");
			break;
		case PROVOLATILE_STABLE:
			appendStringInfoString(&buf, " STABLE");
			break;
		case PROVOLATILE_VOLATILE:
			break;
	}

	switch (proc->proparallel)
	{
		case PROPARALLEL_SAFE:
			appendStringInfoString(&buf, " PARALLEL SAFE");
			break;
		case PROPARALLEL_RESTRICTED:
			appendStringInfoString(&buf, " PARALLEL RESTRICTED");
			break;
		case PROPARALLEL_UNSAFE:
			break;
	}

	if (proc->proisstrict)
		appendStringInfoString(&buf, " STRICT");
	if (proc->prosecdef)
		appendStringInfoString(&buf, " SECURITY DEFINER");
	if (proc->proleakproof)
		appendStringInfoString(&buf, " LEAKPROOF");

	/* This code for the default cost and rows should match functioncmds.c */
	if (proc->prolang == INTERNALlanguageId ||
		proc->prolang == ClanguageId)
		procost = 1;
	else
		procost = 100;
	if (proc->procost != procost)
		appendStringInfo(&buf, " COST %g", proc->procost);

	if (proc->prorows > 0 && proc->prorows != 1000)
		appendStringInfo(&buf, " ROWS %g", proc->prorows);

	if (oldlen != buf.len)
		appendStringInfoChar(&buf, '\n');

	/* Emit any proconfig options, one per line */
	tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proconfig, &isnull);
	if (!isnull)
	{
		ArrayType  *a = DatumGetArrayTypeP(tmp);
		int			i;

		Assert(ARR_ELEMTYPE(a) == TEXTOID);
		Assert(ARR_NDIM(a) == 1);
		Assert(ARR_LBOUND(a)[0] == 1);

		for (i = 1; i <= ARR_DIMS(a)[0]; i++)
		{
			Datum		d;

			d = array_ref(a, 1, &i,
						  -1 /* varlenarray */ ,
						  -1 /* TEXT's typlen */ ,
						  false /* TEXT's typbyval */ ,
						  'i' /* TEXT's typalign */ ,
						  &isnull);
			if (!isnull)
			{
				char	   *configitem = TextDatumGetCString(d);
				char	   *pos;

				pos = strchr(configitem, '=');
				if (pos == NULL)
					continue;
				*pos++ = '\0';

				appendStringInfo(&buf, " SET %s TO ",
								 quote_identifier(configitem));

				/*
				 * Some GUC variable names are 'LIST' type and hence must not
				 * be quoted.
				 */
				if (pg_strcasecmp(configitem, "DateStyle") == 0
					|| pg_strcasecmp(configitem, "search_path") == 0)
					appendStringInfoString(&buf, pos);
				else
					simple_quote_literal(&buf, pos);
				appendStringInfoChar(&buf, '\n');
			}
		}
	}

	/* And finally the function definition ... */
	appendStringInfoString(&buf, "AS ");

	tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_probin, &isnull);
	if (!isnull)
	{
		simple_quote_literal(&buf, TextDatumGetCString(tmp));
		appendStringInfoString(&buf, ", ");		/* assume prosrc isn't null */
	}

	tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc");
	prosrc = TextDatumGetCString(tmp);

	/*
	 * We always use dollar quoting.  Figure out a suitable delimiter.
	 *
	 * Since the user is likely to be editing the function body string, we
	 * shouldn't use a short delimiter that he might easily create a conflict
	 * with.  Hence prefer "$function$", but extend if needed.
	 */
	initStringInfo(&dq);
	appendStringInfoString(&dq, "$function");
	while (strstr(prosrc, dq.data) != NULL)
		appendStringInfoChar(&dq, 'x');
	appendStringInfoChar(&dq, '$');

	appendStringInfoString(&buf, dq.data);
	appendStringInfoString(&buf, prosrc);
	appendStringInfoString(&buf, dq.data);

	appendStringInfoChar(&buf, '\n');

	ReleaseSysCache(proctup);

	PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * pg_get_function_arguments
 *		Get a nicely-formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		CREATE FUNCTION.
 */
Datum
pg_get_function_arguments(PG_FUNCTION_ARGS)
{
	Oid			funcid = PG_GETARG_OID(0);
	StringInfoData buf;
	HeapTuple	proctup;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		PG_RETURN_NULL();

	initStringInfo(&buf);

	(void) print_function_arguments(&buf, proctup, false, true);

	ReleaseSysCache(proctup);

	PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * pg_get_function_identity_arguments
 *		Get a formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		ALTER FUNCTION, etc.  In particular, don't print defaults.
 */
Datum
pg_get_function_identity_arguments(PG_FUNCTION_ARGS)
{
	Oid			funcid = PG_GETARG_OID(0);
	StringInfoData buf;
	HeapTuple	proctup;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		PG_RETURN_NULL();

	initStringInfo(&buf);

	(void) print_function_arguments(&buf, proctup, false, false);

	ReleaseSysCache(proctup);

	PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * pg_get_function_result
 *		Get a nicely-formatted version of the result type of a function.
 *		This is what would appear after RETURNS in CREATE FUNCTION.
 */
Datum
pg_get_function_result(PG_FUNCTION_ARGS)
{
	Oid			funcid = PG_GETARG_OID(0);
	StringInfoData buf;
	HeapTuple	proctup;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		PG_RETURN_NULL();

	initStringInfo(&buf);

	print_function_rettype(&buf, proctup);

	ReleaseSysCache(proctup);

	PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * Guts of pg_get_function_result: append the function's return type
 * to the specified buffer.
 */
static void
print_function_rettype(StringInfo buf, HeapTuple proctup)
{
	Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(proctup);
	int			ntabargs = 0;
	StringInfoData rbuf;

	initStringInfo(&rbuf);

	if (proc->proretset)
	{
		/* It might be a table function; try to print the arguments */
		appendStringInfoString(&rbuf, "TABLE(");
		ntabargs = print_function_arguments(&rbuf, proctup, true, false);
		if (ntabargs > 0)
			appendStringInfoChar(&rbuf, ')');
		else
			resetStringInfo(&rbuf);
	}

	if (ntabargs == 0)
	{
		/* Not a table function, so do the normal thing */
		if (proc->proretset)
			appendStringInfoString(&rbuf, "SETOF ");
		appendStringInfoString(&rbuf, format_type_be(proc->prorettype));
	}

	appendStringInfoString(buf, rbuf.data);
}

/*
 * Common code for pg_get_function_arguments and pg_get_function_result:
 * append the desired subset of arguments to buf.  We print only TABLE
 * arguments when print_table_args is true, and all the others when it's false.
 * We print argument defaults only if print_defaults is true.
 * Function return value is the number of arguments printed.
 */
static int
print_function_arguments(StringInfo buf, HeapTuple proctup,
						 bool print_table_args, bool print_defaults)
{
	Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(proctup);
	int			numargs;
	Oid		   *argtypes;
	char	  **argnames;
	char	   *argmodes;
	int			insertorderbyat = -1;
	int			argsprinted;
	int			inputargno;
	int			nlackdefaults;
	ListCell   *nextargdefault = NULL;
	int			i;

	numargs = get_func_arg_info(proctup,
								&argtypes, &argnames, &argmodes);

	nlackdefaults = numargs;
	if (print_defaults && proc->pronargdefaults > 0)
	{
		Datum		proargdefaults;
		bool		isnull;

		proargdefaults = SysCacheGetAttr(PROCOID, proctup,
										 Anum_pg_proc_proargdefaults,
										 &isnull);
		if (!isnull)
		{
			char	   *str;
			List	   *argdefaults;

			str = TextDatumGetCString(proargdefaults);
			argdefaults = (List *) stringToNode(str);
			Assert(IsA(argdefaults, List));
			pfree(str);
			nextargdefault = list_head(argdefaults);
			/* nlackdefaults counts only *input* arguments lacking defaults */
			nlackdefaults = proc->pronargs - list_length(argdefaults);
		}
	}

	/* Check for special treatment of ordered-set aggregates */
	if (proc->proisagg)
	{
		HeapTuple	aggtup;
		Form_pg_aggregate agg;

		aggtup = SearchSysCache1(AGGFNOID,
								 ObjectIdGetDatum(HeapTupleGetOid(proctup)));
		if (!HeapTupleIsValid(aggtup))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 HeapTupleGetOid(proctup));
		agg = (Form_pg_aggregate) GETSTRUCT(aggtup);
		if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
			insertorderbyat = agg->aggnumdirectargs;
		ReleaseSysCache(aggtup);
	}

	argsprinted = 0;
	inputargno = 0;
	for (i = 0; i < numargs; i++)
	{
		Oid			argtype = argtypes[i];
		char	   *argname = argnames ? argnames[i] : NULL;
		char		argmode = argmodes ? argmodes[i] : PROARGMODE_IN;
		const char *modename;
		bool		isinput;

		switch (argmode)
		{
			case PROARGMODE_IN:
				modename = "";
				isinput = true;
				break;
			case PROARGMODE_INOUT:
				modename = "INOUT ";
				isinput = true;
				break;
			case PROARGMODE_OUT:
				modename = "OUT ";
				isinput = false;
				break;
			case PROARGMODE_VARIADIC:
				modename = "VARIADIC ";
				isinput = true;
				break;
			case PROARGMODE_TABLE:
				modename = "";
				isinput = false;
				break;
			default:
				elog(ERROR, "invalid parameter mode '%c'", argmode);
				modename = NULL;	/* keep compiler quiet */
				isinput = false;
				break;
		}
		if (isinput)
			inputargno++;		/* this is a 1-based counter */

		if (print_table_args != (argmode == PROARGMODE_TABLE))
			continue;

		if (argsprinted == insertorderbyat)
		{
			if (argsprinted)
				appendStringInfoChar(buf, ' ');
			appendStringInfoString(buf, "ORDER BY ");
		}
		else if (argsprinted)
			appendStringInfoString(buf, ", ");

		appendStringInfoString(buf, modename);
		if (argname && argname[0])
			appendStringInfo(buf, "%s ", quote_identifier(argname));
		appendStringInfoString(buf, format_type_be(argtype));
		if (print_defaults && isinput && inputargno > nlackdefaults)
		{
			Node	   *expr;

			Assert(nextargdefault != NULL);
			expr = (Node *) lfirst(nextargdefault);
			nextargdefault = lnext(nextargdefault);

			appendStringInfo(buf, " DEFAULT %s",
							 deparse_expression(expr, NIL, false, false));
		}
		argsprinted++;

		/* nasty hack: print the last arg twice for variadic ordered-set agg */
		if (argsprinted == insertorderbyat && i == numargs - 1)
		{
			i--;
			/* aggs shouldn't have defaults anyway, but just to be sure ... */
			print_defaults = false;
		}
	}

	return argsprinted;
}

static bool
is_input_argument(int nth, const char *argmodes)
{
	return (!argmodes
			|| argmodes[nth] == PROARGMODE_IN
			|| argmodes[nth] == PROARGMODE_INOUT
			|| argmodes[nth] == PROARGMODE_VARIADIC);
}

/*
 * Append used transformated types to specified buffer
 */
static void
print_function_trftypes(StringInfo buf, HeapTuple proctup)
{
	Oid		   *trftypes;
	int			ntypes;

	ntypes = get_func_trftypes(proctup, &trftypes);
	if (ntypes > 0)
	{
		int			i;

		appendStringInfoString(buf, "\n TRANSFORM ");
		for (i = 0; i < ntypes; i++)
		{
			if (i != 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "FOR TYPE %s", format_type_be(trftypes[i]));
		}
	}
}

/*
 * Get textual representation of a function argument's default value.  The
 * second argument of this function is the argument number among all arguments
 * (i.e. proallargtypes, *not* proargtypes), starting with 1, because that's
 * how information_schema.sql uses it.
 */
Datum
pg_get_function_arg_default(PG_FUNCTION_ARGS)
{
	Oid			funcid = PG_GETARG_OID(0);
	int32		nth_arg = PG_GETARG_INT32(1);
	HeapTuple	proctup;
	Form_pg_proc proc;
	int			numargs;
	Oid		   *argtypes;
	char	  **argnames;
	char	   *argmodes;
	int			i;
	List	   *argdefaults;
	Node	   *node;
	char	   *str;
	int			nth_inputarg;
	Datum		proargdefaults;
	bool		isnull;
	int			nth_default;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		PG_RETURN_NULL();

	numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);
	if (nth_arg < 1 || nth_arg > numargs || !is_input_argument(nth_arg - 1, argmodes))
	{
		ReleaseSysCache(proctup);
		PG_RETURN_NULL();
	}

	nth_inputarg = 0;
	for (i = 0; i < nth_arg; i++)
		if (is_input_argument(i, argmodes))
			nth_inputarg++;

	proargdefaults = SysCacheGetAttr(PROCOID, proctup,
									 Anum_pg_proc_proargdefaults,
									 &isnull);
	if (isnull)
	{
		ReleaseSysCache(proctup);
		PG_RETURN_NULL();
	}

	str = TextDatumGetCString(proargdefaults);
	argdefaults = (List *) stringToNode(str);
	Assert(IsA(argdefaults, List));
	pfree(str);

	proc = (Form_pg_proc) GETSTRUCT(proctup);

	/*
	 * Calculate index into proargdefaults: proargdefaults corresponds to the
	 * last N input arguments, where N = pronargdefaults.
	 */
	nth_default = nth_inputarg - 1 - (proc->pronargs - proc->pronargdefaults);

	if (nth_default < 0 || nth_default >= list_length(argdefaults))
	{
		ReleaseSysCache(proctup);
		PG_RETURN_NULL();
	}
	node = list_nth(argdefaults, nth_default);
	str = deparse_expression(node, NIL, false, false);

	ReleaseSysCache(proctup);

	PG_RETURN_TEXT_P(string_to_text(str));
}


/*
 * deparse_expression			- General utility for deparsing expressions
 *
 * calls deparse_expression_pretty with all prettyPrinting disabled
 */
char *
deparse_expression(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit)
{
	return deparse_expression_pretty(expr, dpcontext, forceprefix,
									 showimplicit, 0, 0);
}

/* ----------
 * deparse_expression_pretty	- General utility for deparsing expressions
 *
 * expr is the node tree to be deparsed.  It must be a transformed expression
 * tree (ie, not the raw output of gram.y).
 *
 * dpcontext is a list of deparse_namespace nodes representing the context
 * for interpreting Vars in the node tree.  It can be NIL if no Vars are
 * expected.
 *
 * forceprefix is TRUE to force all Vars to be prefixed with their table names.
 *
 * showimplicit is TRUE to force all implicit casts to be shown explicitly.
 *
 * Tries to pretty up the output according to prettyFlags and startIndent.
 *
 * The result is a palloc'd string.
 * ----------
 */
static char *
deparse_expression_pretty(Node *expr, List *dpcontext,
						  bool forceprefix, bool showimplicit,
						  int prettyFlags, int startIndent)
{
	StringInfoData buf;
	deparse_context context;

	initStringInfo(&buf);
	context.buf = &buf;
	context.namespaces = dpcontext;
	context.windowClause = NIL;
	context.windowTList = NIL;
	context.varprefix = forceprefix;
	context.prettyFlags = prettyFlags;
	context.wrapColumn = WRAP_COLUMN_DEFAULT;
	context.indentLevel = startIndent;
	context.special_exprkind = EXPR_KIND_NONE;

	get_rule_expr(expr, &context, showimplicit);

	return buf.data;
}

/* ----------
 * deparse_context_for			- Build deparse context for a single relation
 *
 * Given the reference name (alias) and OID of a relation, build deparsing
 * context for an expression referencing only that relation (as varno 1,
 * varlevelsup 0).  This is sufficient for many uses of deparse_expression.
 * ----------
 */
List *
deparse_context_for(const char *aliasname, Oid relid)
{
	deparse_namespace *dpns;
	RangeTblEntry *rte;

	dpns = (deparse_namespace *) palloc0(sizeof(deparse_namespace));

	/* Build a minimal RTE for the rel */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = relid;
	rte->relkind = RELKIND_RELATION;	/* no need for exactness here */
	rte->alias = makeAlias(aliasname, NIL);
	rte->eref = rte->alias;
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = true;

	/* Build one-element rtable */
	dpns->rtable = list_make1(rte);
	dpns->ctes = NIL;
	set_rtable_names(dpns, NIL, NULL);
	set_simple_column_names(dpns);

	/* Return a one-deep namespace stack */
	return list_make1(dpns);
}

/*
 * deparse_context_for_plan_rtable - Build deparse context for a plan's rtable
 *
 * When deparsing an expression in a Plan tree, we use the plan's rangetable
 * to resolve names of simple Vars.  The initialization of column names for
 * this is rather expensive if the rangetable is large, and it'll be the same
 * for every expression in the Plan tree; so we do it just once and re-use
 * the result of this function for each expression.  (Note that the result
 * is not usable until set_deparse_context_planstate() is applied to it.)
 *
 * In addition to the plan's rangetable list, pass the per-RTE alias names
 * assigned by a previous call to select_rtable_names_for_explain.
 */
List *
deparse_context_for_plan_rtable(List *rtable, List *rtable_names)
{
	deparse_namespace *dpns;

	dpns = (deparse_namespace *) palloc0(sizeof(deparse_namespace));

	/* Initialize fields that stay the same across the whole plan tree */
	dpns->rtable = rtable;
	dpns->rtable_names = rtable_names;
	dpns->ctes = NIL;

	/*
	 * Set up column name aliases.  We will get rather bogus results for join
	 * RTEs, but that doesn't matter because plan trees don't contain any join
	 * alias Vars.
	 */
	set_simple_column_names(dpns);

	/* Return a one-deep namespace stack */
	return list_make1(dpns);
}

/*
 * set_deparse_context_planstate	- Specify Plan node containing expression
 *
 * When deparsing an expression in a Plan tree, we might have to resolve
 * OUTER_VAR, INNER_VAR, or INDEX_VAR references.  To do this, the caller must
 * provide the parent PlanState node.  Then OUTER_VAR and INNER_VAR references
 * can be resolved by drilling down into the left and right child plans.
 * Similarly, INDEX_VAR references can be resolved by reference to the
 * indextlist given in a parent IndexOnlyScan node, or to the scan tlist in
 * ForeignScan and CustomScan nodes.  (Note that we don't currently support
 * deparsing of indexquals in regular IndexScan or BitmapIndexScan nodes;
 * for those, we can only deparse the indexqualorig fields, which won't
 * contain INDEX_VAR Vars.)
 *
 * Note: planstate really ought to be declared as "PlanState *", but we use
 * "Node *" to avoid having to include execnodes.h in ruleutils.h.
 *
 * The ancestors list is a list of the PlanState's parent PlanStates, the
 * most-closely-nested first.  This is needed to resolve PARAM_EXEC Params.
 * Note we assume that all the PlanStates share the same rtable.
 *
 * Once this function has been called, deparse_expression() can be called on
 * subsidiary expression(s) of the specified PlanState node.  To deparse
 * expressions of a different Plan node in the same Plan tree, re-call this
 * function to identify the new parent Plan node.
 *
 * The result is the same List passed in; this is a notational convenience.
 */
List *
set_deparse_context_planstate(List *dpcontext,
							  Node *planstate, List *ancestors)
{
	deparse_namespace *dpns;

	/* Should always have one-entry namespace list for Plan deparsing */
	Assert(list_length(dpcontext) == 1);
	dpns = (deparse_namespace *) linitial(dpcontext);

	/* Set our attention on the specific plan node passed in */
	set_deparse_planstate(dpns, (PlanState *) planstate);
	dpns->ancestors = ancestors;

	return dpcontext;
}

/*
 * select_rtable_names_for_explain	- Select RTE aliases for EXPLAIN
 *
 * Determine the relation aliases we'll use during an EXPLAIN operation.
 * This is just a frontend to set_rtable_names.  We have to expose the aliases
 * to EXPLAIN because EXPLAIN needs to know the right alias names to print.
 */
List *
select_rtable_names_for_explain(List *rtable, Bitmapset *rels_used)
{
	deparse_namespace dpns;

	memset(&dpns, 0, sizeof(dpns));
	dpns.rtable = rtable;
	dpns.ctes = NIL;
	set_rtable_names(&dpns, NIL, rels_used);
	/* We needn't bother computing column aliases yet */

	return dpns.rtable_names;
}

/*
 * set_rtable_names: select RTE aliases to be used in printing a query
 *
 * We fill in dpns->rtable_names with a list of names that is one-for-one with
 * the already-filled dpns->rtable list.  Each RTE name is unique among those
 * in the new namespace plus any ancestor namespaces listed in
 * parent_namespaces.
 *
 * If rels_used isn't NULL, only RTE indexes listed in it are given aliases.
 *
 * Note that this function is only concerned with relation names, not column
 * names.
 */
static void
set_rtable_names(deparse_namespace *dpns, List *parent_namespaces,
				 Bitmapset *rels_used)
{
	HASHCTL		hash_ctl;
	HTAB	   *names_hash;
	NameHashEntry *hentry;
	bool		found;
	int			rtindex;
	ListCell   *lc;

	dpns->rtable_names = NIL;
	/* nothing more to do if empty rtable */
	if (dpns->rtable == NIL)
		return;

	/*
	 * We use a hash table to hold known names, so that this process is O(N)
	 * not O(N^2) for N names.
	 */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(NameHashEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	names_hash = hash_create("set_rtable_names names",
							 list_length(dpns->rtable),
							 &hash_ctl,
							 HASH_ELEM | HASH_CONTEXT);
	/* Preload the hash table with names appearing in parent_namespaces */
	foreach(lc, parent_namespaces)
	{
		deparse_namespace *olddpns = (deparse_namespace *) lfirst(lc);
		ListCell   *lc2;

		foreach(lc2, olddpns->rtable_names)
		{
			char	   *oldname = (char *) lfirst(lc2);

			if (oldname == NULL)
				continue;
			hentry = (NameHashEntry *) hash_search(names_hash,
												   oldname,
												   HASH_ENTER,
												   &found);
			/* we do not complain about duplicate names in parent namespaces */
			hentry->counter = 0;
		}
	}

	/* Now we can scan the rtable */
	rtindex = 1;
	foreach(lc, dpns->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		char	   *refname;

		/* Just in case this takes an unreasonable amount of time ... */
		CHECK_FOR_INTERRUPTS();

		if (rels_used && !bms_is_member(rtindex, rels_used))
		{
			/* Ignore unreferenced RTE */
			refname = NULL;
		}
		else if (rte->alias)
		{
			/* If RTE has a user-defined alias, prefer that */
			refname = rte->alias->aliasname;
		}
		else if (rte->rtekind == RTE_RELATION)
		{
			/* Use the current actual name of the relation */
			refname = get_rel_name(rte->relid);
		}
		else if (rte->rtekind == RTE_JOIN)
		{
			/* Unnamed join has no refname */
			refname = NULL;
		}
		else
		{
			/* Otherwise use whatever the parser assigned */
			refname = rte->eref->aliasname;
		}

		/*
		 * If the selected name isn't unique, append digits to make it so, and
		 * make a new hash entry for it once we've got a unique name.  For a
		 * very long input name, we might have to truncate to stay within
		 * NAMEDATALEN.
		 */
		if (refname)
		{
			hentry = (NameHashEntry *) hash_search(names_hash,
												   refname,
												   HASH_ENTER,
												   &found);
			if (found)
			{
				/* Name already in use, must choose a new one */
				int			refnamelen = strlen(refname);
				char	   *modname = (char *) palloc(refnamelen + 16);
				NameHashEntry *hentry2;

				do
				{
					hentry->counter++;
					for (;;)
					{
						/*
						 * We avoid using %.*s here because it can misbehave
						 * if the data is not valid in what libc thinks is the
						 * prevailing encoding.
						 */
						memcpy(modname, refname, refnamelen);
						sprintf(modname + refnamelen, "_%d", hentry->counter);
						if (strlen(modname) < NAMEDATALEN)
							break;
						/* drop chars from refname to keep all the digits */
						refnamelen = pg_mbcliplen(refname, refnamelen,
												  refnamelen - 1);
					}
					hentry2 = (NameHashEntry *) hash_search(names_hash,
															modname,
															HASH_ENTER,
															&found);
				} while (found);
				hentry2->counter = 0;	/* init new hash entry */
				refname = modname;
			}
			else
			{
				/* Name not previously used, need only initialize hentry */
				hentry->counter = 0;
			}
		}

		dpns->rtable_names = lappend(dpns->rtable_names, refname);
		rtindex++;
	}

	hash_destroy(names_hash);
}

/*
 * set_deparse_for_query: set up deparse_namespace for deparsing a Query tree
 *
 * For convenience, this is defined to initialize the deparse_namespace struct
 * from scratch.
 */
static void
set_deparse_for_query(deparse_namespace *dpns, Query *query,
					  List *parent_namespaces)
{
	ListCell   *lc;
	ListCell   *lc2;

	/* Initialize *dpns and fill rtable/ctes links */
	memset(dpns, 0, sizeof(deparse_namespace));
	dpns->rtable = query->rtable;
	dpns->ctes = query->cteList;

	/* Assign a unique relation alias to each RTE */
	set_rtable_names(dpns, parent_namespaces, NULL);

	/* Initialize dpns->rtable_columns to contain zeroed structs */
	dpns->rtable_columns = NIL;
	while (list_length(dpns->rtable_columns) < list_length(dpns->rtable))
		dpns->rtable_columns = lappend(dpns->rtable_columns,
									   palloc0(sizeof(deparse_columns)));

	/* If it's a utility query, it won't have a jointree */
	if (query->jointree)
	{
		/* Detect whether global uniqueness of USING names is needed */
		dpns->unique_using =
			has_dangerous_join_using(dpns, (Node *) query->jointree);

		/*
		 * Select names for columns merged by USING, via a recursive pass over
		 * the query jointree.
		 */
		set_using_names(dpns, (Node *) query->jointree, NIL);
	}

	/*
	 * Now assign remaining column aliases for each RTE.  We do this in a
	 * linear scan of the rtable, so as to process RTEs whether or not they
	 * are in the jointree (we mustn't miss NEW.*, INSERT target relations,
	 * etc).  JOIN RTEs must be processed after their children, but this is
	 * okay because they appear later in the rtable list than their children
	 * (cf Asserts in identify_join_columns()).
	 */
	forboth(lc, dpns->rtable, lc2, dpns->rtable_columns)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		deparse_columns *colinfo = (deparse_columns *) lfirst(lc2);

		if (rte->rtekind == RTE_JOIN)
			set_join_column_names(dpns, rte, colinfo);
		else
			set_relation_column_names(dpns, rte, colinfo);
	}
}

/*
 * set_simple_column_names: fill in column aliases for non-query situations
 *
 * This handles EXPLAIN and cases where we only have relation RTEs.  Without
 * a join tree, we can't do anything smart about join RTEs, but we don't
 * need to (note that EXPLAIN should never see join alias Vars anyway).
 * If we do hit a join RTE we'll just process it like a non-table base RTE.
 */
static void
set_simple_column_names(deparse_namespace *dpns)
{
	ListCell   *lc;
	ListCell   *lc2;

	/* Initialize dpns->rtable_columns to contain zeroed structs */
	dpns->rtable_columns = NIL;
	while (list_length(dpns->rtable_columns) < list_length(dpns->rtable))
		dpns->rtable_columns = lappend(dpns->rtable_columns,
									   palloc0(sizeof(deparse_columns)));

	/* Assign unique column aliases within each RTE */
	forboth(lc, dpns->rtable, lc2, dpns->rtable_columns)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		deparse_columns *colinfo = (deparse_columns *) lfirst(lc2);

		set_relation_column_names(dpns, rte, colinfo);
	}
}

/*
 * has_dangerous_join_using: search jointree for unnamed JOIN USING
 *
 * Merged columns of a JOIN USING may act differently from either of the input
 * columns, either because they are merged with COALESCE (in a FULL JOIN) or
 * because an implicit coercion of the underlying input column is required.
 * In such a case the column must be referenced as a column of the JOIN not as
 * a column of either input.  And this is problematic if the join is unnamed
 * (alias-less): we cannot qualify the column's name with an RTE name, since
 * there is none.  (Forcibly assigning an alias to the join is not a solution,
 * since that will prevent legal references to tables below the join.)
 * To ensure that every column in the query is unambiguously referenceable,
 * we must assign such merged columns names that are globally unique across
 * the whole query, aliasing other columns out of the way as necessary.
 *
 * Because the ensuing re-aliasing is fairly damaging to the readability of
 * the query, we don't do this unless we have to.  So, we must pre-scan
 * the join tree to see if we have to, before starting set_using_names().
 */
static bool
has_dangerous_join_using(deparse_namespace *dpns, Node *jtnode)
{
	if (IsA(jtnode, RangeTblRef))
	{
		/* nothing to do here */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *lc;

		foreach(lc, f->fromlist)
		{
			if (has_dangerous_join_using(dpns, (Node *) lfirst(lc)))
				return true;
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		/* Is it an unnamed JOIN with USING? */
		if (j->alias == NULL && j->usingClause)
		{
			/*
			 * Yes, so check each join alias var to see if any of them are not
			 * simple references to underlying columns.  If so, we have a
			 * dangerous situation and must pick unique aliases.
			 */
			RangeTblEntry *jrte = rt_fetch(j->rtindex, dpns->rtable);
			ListCell   *lc;

			foreach(lc, jrte->joinaliasvars)
			{
				Var		   *aliasvar = (Var *) lfirst(lc);

				if (aliasvar != NULL && !IsA(aliasvar, Var))
					return true;
			}
		}

		/* Nope, but inspect children */
		if (has_dangerous_join_using(dpns, j->larg))
			return true;
		if (has_dangerous_join_using(dpns, j->rarg))
			return true;
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	return false;
}

/*
 * set_using_names: select column aliases to be used for merged USING columns
 *
 * We do this during a recursive descent of the query jointree.
 * dpns->unique_using must already be set to determine the global strategy.
 *
 * Column alias info is saved in the dpns->rtable_columns list, which is
 * assumed to be filled with pre-zeroed deparse_columns structs.
 *
 * parentUsing is a list of all USING aliases assigned in parent joins of
 * the current jointree node.  (The passed-in list must not be modified.)
 */
static void
set_using_names(deparse_namespace *dpns, Node *jtnode, List *parentUsing)
{
	if (IsA(jtnode, RangeTblRef))
	{
		/* nothing to do now */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *lc;

		foreach(lc, f->fromlist)
			set_using_names(dpns, (Node *) lfirst(lc), parentUsing);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		RangeTblEntry *rte = rt_fetch(j->rtindex, dpns->rtable);
		deparse_columns *colinfo = deparse_columns_fetch(j->rtindex, dpns);
		int		   *leftattnos;
		int		   *rightattnos;
		deparse_columns *leftcolinfo;
		deparse_columns *rightcolinfo;
		int			i;
		ListCell   *lc;

		/* Get info about the shape of the join */
		identify_join_columns(j, rte, colinfo);
		leftattnos = colinfo->leftattnos;
		rightattnos = colinfo->rightattnos;

		/* Look up the not-yet-filled-in child deparse_columns structs */
		leftcolinfo = deparse_columns_fetch(colinfo->leftrti, dpns);
		rightcolinfo = deparse_columns_fetch(colinfo->rightrti, dpns);

		/*
		 * If this join is unnamed, then we cannot substitute new aliases at
		 * this level, so any name requirements pushed down to here must be
		 * pushed down again to the children.
		 */
		if (rte->alias == NULL)
		{
			for (i = 0; i < colinfo->num_cols; i++)
			{
				char	   *colname = colinfo->colnames[i];

				if (colname == NULL)
					continue;

				/* Push down to left column, unless it's a system column */
				if (leftattnos[i] > 0)
				{
					expand_colnames_array_to(leftcolinfo, leftattnos[i]);
					leftcolinfo->colnames[leftattnos[i] - 1] = colname;
				}

				/* Same on the righthand side */
				if (rightattnos[i] > 0)
				{
					expand_colnames_array_to(rightcolinfo, rightattnos[i]);
					rightcolinfo->colnames[rightattnos[i] - 1] = colname;
				}
			}
		}

		/*
		 * If there's a USING clause, select the USING column names and push
		 * those names down to the children.  We have two strategies:
		 *
		 * If dpns->unique_using is TRUE, we force all USING names to be
		 * unique across the whole query level.  In principle we'd only need
		 * the names of dangerous USING columns to be globally unique, but to
		 * safely assign all USING names in a single pass, we have to enforce
		 * the same uniqueness rule for all of them.  However, if a USING
		 * column's name has been pushed down from the parent, we should use
		 * it as-is rather than making a uniqueness adjustment.  This is
		 * necessary when we're at an unnamed join, and it creates no risk of
		 * ambiguity.  Also, if there's a user-written output alias for a
		 * merged column, we prefer to use that rather than the input name;
		 * this simplifies the logic and seems likely to lead to less aliasing
		 * overall.
		 *
		 * If dpns->unique_using is FALSE, we only need USING names to be
		 * unique within their own join RTE.  We still need to honor
		 * pushed-down names, though.
		 *
		 * Though significantly different in results, these two strategies are
		 * implemented by the same code, with only the difference of whether
		 * to put assigned names into dpns->using_names.
		 */
		if (j->usingClause)
		{
			/* Copy the input parentUsing list so we don't modify it */
			parentUsing = list_copy(parentUsing);

			/* USING names must correspond to the first join output columns */
			expand_colnames_array_to(colinfo, list_length(j->usingClause));
			i = 0;
			foreach(lc, j->usingClause)
			{
				char	   *colname = strVal(lfirst(lc));

				/* Assert it's a merged column */
				Assert(leftattnos[i] != 0 && rightattnos[i] != 0);

				/* Adopt passed-down name if any, else select unique name */
				if (colinfo->colnames[i] != NULL)
					colname = colinfo->colnames[i];
				else
				{
					/* Prefer user-written output alias if any */
					if (rte->alias && i < list_length(rte->alias->colnames))
						colname = strVal(list_nth(rte->alias->colnames, i));
					/* Make it appropriately unique */
					colname = make_colname_unique(colname, dpns, colinfo);
					if (dpns->unique_using)
						dpns->using_names = lappend(dpns->using_names,
													colname);
					/* Save it as output column name, too */
					colinfo->colnames[i] = colname;
				}

				/* Remember selected names for use later */
				colinfo->usingNames = lappend(colinfo->usingNames, colname);
				parentUsing = lappend(parentUsing, colname);

				/* Push down to left column, unless it's a system column */
				if (leftattnos[i] > 0)
				{
					expand_colnames_array_to(leftcolinfo, leftattnos[i]);
					leftcolinfo->colnames[leftattnos[i] - 1] = colname;
				}

				/* Same on the righthand side */
				if (rightattnos[i] > 0)
				{
					expand_colnames_array_to(rightcolinfo, rightattnos[i]);
					rightcolinfo->colnames[rightattnos[i] - 1] = colname;
				}

				i++;
			}
		}

		/* Mark child deparse_columns structs with correct parentUsing info */
		leftcolinfo->parentUsing = parentUsing;
		rightcolinfo->parentUsing = parentUsing;

		/* Now recursively assign USING column names in children */
		set_using_names(dpns, j->larg, parentUsing);
		set_using_names(dpns, j->rarg, parentUsing);
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * set_relation_column_names: select column aliases for a non-join RTE
 *
 * Column alias info is saved in *colinfo, which is assumed to be pre-zeroed.
 * If any colnames entries are already filled in, those override local
 * choices.
 */
static void
set_relation_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
						  deparse_columns *colinfo)
{
	int			ncolumns;
	char	  **real_colnames;
	bool		changed_any;
	int			noldcolumns;
	int			i;
	int			j;

	/*
	 * Extract the RTE's "real" column names.  This is comparable to
	 * get_rte_attribute_name, except that it's important to disregard dropped
	 * columns.  We put NULL into the array for a dropped column.
	 */
	if (rte->rtekind == RTE_RELATION)
	{
		/* Relation --- look to the system catalogs for up-to-date info */
		Relation	rel;
		TupleDesc	tupdesc;

		rel = relation_open(rte->relid, AccessShareLock);
		tupdesc = RelationGetDescr(rel);

		ncolumns = tupdesc->natts;
		real_colnames = (char **) palloc(ncolumns * sizeof(char *));

		for (i = 0; i < ncolumns; i++)
		{
			if (tupdesc->attrs[i]->attisdropped)
				real_colnames[i] = NULL;
			else
				real_colnames[i] = pstrdup(NameStr(tupdesc->attrs[i]->attname));
		}
		relation_close(rel, AccessShareLock);
	}
	else
	{
		/* Otherwise use the column names from eref */
		ListCell   *lc;

		ncolumns = list_length(rte->eref->colnames);
		real_colnames = (char **) palloc(ncolumns * sizeof(char *));

		i = 0;
		foreach(lc, rte->eref->colnames)
		{
			/*
			 * If the column name shown in eref is an empty string, then it's
			 * a column that was dropped at the time of parsing the query, so
			 * treat it as dropped.
			 */
			char	   *cname = strVal(lfirst(lc));

			if (cname[0] == '\0')
				cname = NULL;
			real_colnames[i] = cname;
			i++;
		}
	}

	/*
	 * Ensure colinfo->colnames has a slot for each column.  (It could be long
	 * enough already, if we pushed down a name for the last column.)  Note:
	 * it's possible that there are now more columns than there were when the
	 * query was parsed, ie colnames could be longer than rte->eref->colnames.
	 * We must assign unique aliases to the new columns too, else there could
	 * be unresolved conflicts when the view/rule is reloaded.
	 */
	expand_colnames_array_to(colinfo, ncolumns);
	Assert(colinfo->num_cols == ncolumns);

	/*
	 * Make sufficiently large new_colnames and is_new_col arrays, too.
	 *
	 * Note: because we leave colinfo->num_new_cols zero until after the loop,
	 * colname_is_unique will not consult that array, which is fine because it
	 * would only be duplicate effort.
	 */
	colinfo->new_colnames = (char **) palloc(ncolumns * sizeof(char *));
	colinfo->is_new_col = (bool *) palloc(ncolumns * sizeof(bool));

	/*
	 * Scan the columns, select a unique alias for each one, and store it in
	 * colinfo->colnames and colinfo->new_colnames.  The former array has NULL
	 * entries for dropped columns, the latter omits them.  Also mark
	 * new_colnames entries as to whether they are new since parse time; this
	 * is the case for entries beyond the length of rte->eref->colnames.
	 */
	noldcolumns = list_length(rte->eref->colnames);
	changed_any = false;
	j = 0;
	for (i = 0; i < ncolumns; i++)
	{
		char	   *real_colname = real_colnames[i];
		char	   *colname = colinfo->colnames[i];

		/* Skip dropped columns */
		if (real_colname == NULL)
		{
			Assert(colname == NULL);	/* colnames[i] is already NULL */
			continue;
		}

		/* If alias already assigned, that's what to use */
		if (colname == NULL)
		{
			/* If user wrote an alias, prefer that over real column name */
			if (rte->alias && i < list_length(rte->alias->colnames))
				colname = strVal(list_nth(rte->alias->colnames, i));
			else
				colname = real_colname;

			/* Unique-ify and insert into colinfo */
			colname = make_colname_unique(colname, dpns, colinfo);

			colinfo->colnames[i] = colname;
		}

		/* Put names of non-dropped columns in new_colnames[] too */
		colinfo->new_colnames[j] = colname;
		/* And mark them as new or not */
		colinfo->is_new_col[j] = (i >= noldcolumns);
		j++;

		/* Remember if any assigned aliases differ from "real" name */
		if (!changed_any && strcmp(colname, real_colname) != 0)
			changed_any = true;
	}

	/*
	 * Set correct length for new_colnames[] array.  (Note: if columns have
	 * been added, colinfo->num_cols includes them, which is not really quite
	 * right but is harmless, since any new columns must be at the end where
	 * they won't affect varattnos of pre-existing columns.)
	 */
	colinfo->num_new_cols = j;

	/*
	 * For a relation RTE, we need only print the alias column names if any
	 * are different from the underlying "real" names.  For a function RTE,
	 * always emit a complete column alias list; this is to protect against
	 * possible instability of the default column names (eg, from altering
	 * parameter names).  For other RTE types, print if we changed anything OR
	 * if there were user-written column aliases (since the latter would be
	 * part of the underlying "reality").
	 */
	if (rte->rtekind == RTE_RELATION)
		colinfo->printaliases = changed_any;
	else if (rte->rtekind == RTE_FUNCTION)
		colinfo->printaliases = true;
	else if (rte->alias && rte->alias->colnames != NIL)
		colinfo->printaliases = true;
	else
		colinfo->printaliases = changed_any;
}

/*
 * set_join_column_names: select column aliases for a join RTE
 *
 * Column alias info is saved in *colinfo, which is assumed to be pre-zeroed.
 * If any colnames entries are already filled in, those override local
 * choices.  Also, names for USING columns were already chosen by
 * set_using_names().  We further expect that column alias selection has been
 * completed for both input RTEs.
 */
static void
set_join_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
					  deparse_columns *colinfo)
{
	deparse_columns *leftcolinfo;
	deparse_columns *rightcolinfo;
	bool		changed_any;
	int			noldcolumns;
	int			nnewcolumns;
	Bitmapset  *leftmerged = NULL;
	Bitmapset  *rightmerged = NULL;
	int			i;
	int			j;
	int			ic;
	int			jc;

	/* Look up the previously-filled-in child deparse_columns structs */
	leftcolinfo = deparse_columns_fetch(colinfo->leftrti, dpns);
	rightcolinfo = deparse_columns_fetch(colinfo->rightrti, dpns);

	/*
	 * Ensure colinfo->colnames has a slot for each column.  (It could be long
	 * enough already, if we pushed down a name for the last column.)  Note:
	 * it's possible that one or both inputs now have more columns than there
	 * were when the query was parsed, but we'll deal with that below.  We
	 * only need entries in colnames for pre-existing columns.
	 */
	noldcolumns = list_length(rte->eref->colnames);
	expand_colnames_array_to(colinfo, noldcolumns);
	Assert(colinfo->num_cols == noldcolumns);

	/*
	 * Scan the join output columns, select an alias for each one, and store
	 * it in colinfo->colnames.  If there are USING columns, set_using_names()
	 * already selected their names, so we can start the loop at the first
	 * non-merged column.
	 */
	changed_any = false;
	for (i = list_length(colinfo->usingNames); i < noldcolumns; i++)
	{
		char	   *colname = colinfo->colnames[i];
		char	   *real_colname;

		/* Ignore dropped column (only possible for non-merged column) */
		if (colinfo->leftattnos[i] == 0 && colinfo->rightattnos[i] == 0)
		{
			Assert(colname == NULL);
			continue;
		}

		/* Get the child column name */
		if (colinfo->leftattnos[i] > 0)
			real_colname = leftcolinfo->colnames[colinfo->leftattnos[i] - 1];
		else if (colinfo->rightattnos[i] > 0)
			real_colname = rightcolinfo->colnames[colinfo->rightattnos[i] - 1];
		else
		{
			/* We're joining system columns --- use eref name */
			real_colname = strVal(list_nth(rte->eref->colnames, i));
		}
		Assert(real_colname != NULL);

		/* In an unnamed join, just report child column names as-is */
		if (rte->alias == NULL)
		{
			colinfo->colnames[i] = real_colname;
			continue;
		}

		/* If alias already assigned, that's what to use */
		if (colname == NULL)
		{
			/* If user wrote an alias, prefer that over real column name */
			if (rte->alias && i < list_length(rte->alias->colnames))
				colname = strVal(list_nth(rte->alias->colnames, i));
			else
				colname = real_colname;

			/* Unique-ify and insert into colinfo */
			colname = make_colname_unique(colname, dpns, colinfo);

			colinfo->colnames[i] = colname;
		}

		/* Remember if any assigned aliases differ from "real" name */
		if (!changed_any && strcmp(colname, real_colname) != 0)
			changed_any = true;
	}

	/*
	 * Calculate number of columns the join would have if it were re-parsed
	 * now, and create storage for the new_colnames and is_new_col arrays.
	 *
	 * Note: colname_is_unique will be consulting new_colnames[] during the
	 * loops below, so its not-yet-filled entries must be zeroes.
	 */
	nnewcolumns = leftcolinfo->num_new_cols + rightcolinfo->num_new_cols -
		list_length(colinfo->usingNames);
	colinfo->num_new_cols = nnewcolumns;
	colinfo->new_colnames = (char **) palloc0(nnewcolumns * sizeof(char *));
	colinfo->is_new_col = (bool *) palloc0(nnewcolumns * sizeof(bool));

	/*
	 * Generating the new_colnames array is a bit tricky since any new columns
	 * added since parse time must be inserted in the right places.  This code
	 * must match the parser, which will order a join's columns as merged
	 * columns first (in USING-clause order), then non-merged columns from the
	 * left input (in attnum order), then non-merged columns from the right
	 * input (ditto).  If one of the inputs is itself a join, its columns will
	 * be ordered according to the same rule, which means newly-added columns
	 * might not be at the end.  We can figure out what's what by consulting
	 * the leftattnos and rightattnos arrays plus the input is_new_col arrays.
	 *
	 * In these loops, i indexes leftattnos/rightattnos (so it's join varattno
	 * less one), j indexes new_colnames/is_new_col, and ic/jc have similar
	 * meanings for the current child RTE.
	 */

	/* Handle merged columns; they are first and can't be new */
	i = j = 0;
	while (i < noldcolumns &&
		   colinfo->leftattnos[i] != 0 &&
		   colinfo->rightattnos[i] != 0)
	{
		/* column name is already determined and known unique */
		colinfo->new_colnames[j] = colinfo->colnames[i];
		colinfo->is_new_col[j] = false;

		/* build bitmapsets of child attnums of merged columns */
		if (colinfo->leftattnos[i] > 0)
			leftmerged = bms_add_member(leftmerged, colinfo->leftattnos[i]);
		if (colinfo->rightattnos[i] > 0)
			rightmerged = bms_add_member(rightmerged, colinfo->rightattnos[i]);

		i++, j++;
	}

	/* Handle non-merged left-child columns */
	ic = 0;
	for (jc = 0; jc < leftcolinfo->num_new_cols; jc++)
	{
		char	   *child_colname = leftcolinfo->new_colnames[jc];

		if (!leftcolinfo->is_new_col[jc])
		{
			/* Advance ic to next non-dropped old column of left child */
			while (ic < leftcolinfo->num_cols &&
				   leftcolinfo->colnames[ic] == NULL)
				ic++;
			Assert(ic < leftcolinfo->num_cols);
			ic++;
			/* If it is a merged column, we already processed it */
			if (bms_is_member(ic, leftmerged))
				continue;
			/* Else, advance i to the corresponding existing join column */
			while (i < colinfo->num_cols &&
				   colinfo->colnames[i] == NULL)
				i++;
			Assert(i < colinfo->num_cols);
			Assert(ic == colinfo->leftattnos[i]);
			/* Use the already-assigned name of this column */
			colinfo->new_colnames[j] = colinfo->colnames[i];
			i++;
		}
		else
		{
			/*
			 * Unique-ify the new child column name and assign, unless we're
			 * in an unnamed join, in which case just copy
			 */
			if (rte->alias != NULL)
			{
				colinfo->new_colnames[j] =
					make_colname_unique(child_colname, dpns, colinfo);
				if (!changed_any &&
					strcmp(colinfo->new_colnames[j], child_colname) != 0)
					changed_any = true;
			}
			else
				colinfo->new_colnames[j] = child_colname;
		}

		colinfo->is_new_col[j] = leftcolinfo->is_new_col[jc];
		j++;
	}

	/* Handle non-merged right-child columns in exactly the same way */
	ic = 0;
	for (jc = 0; jc < rightcolinfo->num_new_cols; jc++)
	{
		char	   *child_colname = rightcolinfo->new_colnames[jc];

		if (!rightcolinfo->is_new_col[jc])
		{
			/* Advance ic to next non-dropped old column of right child */
			while (ic < rightcolinfo->num_cols &&
				   rightcolinfo->colnames[ic] == NULL)
				ic++;
			Assert(ic < rightcolinfo->num_cols);
			ic++;
			/* If it is a merged column, we already processed it */
			if (bms_is_member(ic, rightmerged))
				continue;
			/* Else, advance i to the corresponding existing join column */
			while (i < colinfo->num_cols &&
				   colinfo->colnames[i] == NULL)
				i++;
			Assert(i < colinfo->num_cols);
			Assert(ic == colinfo->rightattnos[i]);
			/* Use the already-assigned name of this column */
			colinfo->new_colnames[j] = colinfo->colnames[i];
			i++;
		}
		else
		{
			/*
			 * Unique-ify the new child column name and assign, unless we're
			 * in an unnamed join, in which case just copy
			 */
			if (rte->alias != NULL)
			{
				colinfo->new_colnames[j] =
					make_colname_unique(child_colname, dpns, colinfo);
				if (!changed_any &&
					strcmp(colinfo->new_colnames[j], child_colname) != 0)
					changed_any = true;
			}
			else
				colinfo->new_colnames[j] = child_colname;
		}

		colinfo->is_new_col[j] = rightcolinfo->is_new_col[jc];
		j++;
	}

	/* Assert we processed the right number of columns */
#ifdef USE_ASSERT_CHECKING
	while (i < colinfo->num_cols && colinfo->colnames[i] == NULL)
		i++;
	Assert(i == colinfo->num_cols);
	Assert(j == nnewcolumns);
#endif

	/*
	 * For a named join, print column aliases if we changed any from the child
	 * names.  Unnamed joins cannot print aliases.
	 */
	if (rte->alias != NULL)
		colinfo->printaliases = changed_any;
	else
		colinfo->printaliases = false;
}

/*
 * colname_is_unique: is colname distinct from already-chosen column names?
 *
 * dpns is query-wide info, colinfo is for the column's RTE
 */
static bool
colname_is_unique(char *colname, deparse_namespace *dpns,
				  deparse_columns *colinfo)
{
	int			i;
	ListCell   *lc;

	/* Check against already-assigned column aliases within RTE */
	for (i = 0; i < colinfo->num_cols; i++)
	{
		char	   *oldname = colinfo->colnames[i];

		if (oldname && strcmp(oldname, colname) == 0)
			return false;
	}

	/*
	 * If we're building a new_colnames array, check that too (this will be
	 * partially but not completely redundant with the previous checks)
	 */
	for (i = 0; i < colinfo->num_new_cols; i++)
	{
		char	   *oldname = colinfo->new_colnames[i];

		if (oldname && strcmp(oldname, colname) == 0)
			return false;
	}

	/* Also check against USING-column names that must be globally unique */
	foreach(lc, dpns->using_names)
	{
		char	   *oldname = (char *) lfirst(lc);

		if (strcmp(oldname, colname) == 0)
			return false;
	}

	/* Also check against names already assigned for parent-join USING cols */
	foreach(lc, colinfo->parentUsing)
	{
		char	   *oldname = (char *) lfirst(lc);

		if (strcmp(oldname, colname) == 0)
			return false;
	}

	return true;
}

/*
 * make_colname_unique: modify colname if necessary to make it unique
 *
 * dpns is query-wide info, colinfo is for the column's RTE
 */
static char *
make_colname_unique(char *colname, deparse_namespace *dpns,
					deparse_columns *colinfo)
{
	/*
	 * If the selected name isn't unique, append digits to make it so.  For a
	 * very long input name, we might have to truncate to stay within
	 * NAMEDATALEN.
	 */
	if (!colname_is_unique(colname, dpns, colinfo))
	{
		int			colnamelen = strlen(colname);
		char	   *modname = (char *) palloc(colnamelen + 16);
		int			i = 0;

		do
		{
			i++;
			for (;;)
			{
				/*
				 * We avoid using %.*s here because it can misbehave if the
				 * data is not valid in what libc thinks is the prevailing
				 * encoding.
				 */
				memcpy(modname, colname, colnamelen);
				sprintf(modname + colnamelen, "_%d", i);
				if (strlen(modname) < NAMEDATALEN)
					break;
				/* drop chars from colname to keep all the digits */
				colnamelen = pg_mbcliplen(colname, colnamelen,
										  colnamelen - 1);
			}
		} while (!colname_is_unique(modname, dpns, colinfo));
		colname = modname;
	}
	return colname;
}

/*
 * expand_colnames_array_to: make colinfo->colnames at least n items long
 *
 * Any added array entries are initialized to zero.
 */
static void
expand_colnames_array_to(deparse_columns *colinfo, int n)
{
	if (n > colinfo->num_cols)
	{
		if (colinfo->colnames == NULL)
			colinfo->colnames = (char **) palloc0(n * sizeof(char *));
		else
		{
			colinfo->colnames = (char **) repalloc(colinfo->colnames,
												   n * sizeof(char *));
			memset(colinfo->colnames + colinfo->num_cols, 0,
				   (n - colinfo->num_cols) * sizeof(char *));
		}
		colinfo->num_cols = n;
	}
}

/*
 * identify_join_columns: figure out where columns of a join come from
 *
 * Fills the join-specific fields of the colinfo struct, except for
 * usingNames which is filled later.
 */
static void
identify_join_columns(JoinExpr *j, RangeTblEntry *jrte,
					  deparse_columns *colinfo)
{
	int			numjoincols;
	int			i;
	ListCell   *lc;

	/* Extract left/right child RT indexes */
	if (IsA(j->larg, RangeTblRef))
		colinfo->leftrti = ((RangeTblRef *) j->larg)->rtindex;
	else if (IsA(j->larg, JoinExpr))
		colinfo->leftrti = ((JoinExpr *) j->larg)->rtindex;
	else
		elog(ERROR, "unrecognized node type in jointree: %d",
			 (int) nodeTag(j->larg));
	if (IsA(j->rarg, RangeTblRef))
		colinfo->rightrti = ((RangeTblRef *) j->rarg)->rtindex;
	else if (IsA(j->rarg, JoinExpr))
		colinfo->rightrti = ((JoinExpr *) j->rarg)->rtindex;
	else
		elog(ERROR, "unrecognized node type in jointree: %d",
			 (int) nodeTag(j->rarg));

	/* Assert children will be processed earlier than join in second pass */
	Assert(colinfo->leftrti < j->rtindex);
	Assert(colinfo->rightrti < j->rtindex);

	/* Initialize result arrays with zeroes */
	numjoincols = list_length(jrte->joinaliasvars);
	Assert(numjoincols == list_length(jrte->eref->colnames));
	colinfo->leftattnos = (int *) palloc0(numjoincols * sizeof(int));
	colinfo->rightattnos = (int *) palloc0(numjoincols * sizeof(int));

	/* Scan the joinaliasvars list to identify simple column references */
	i = 0;
	foreach(lc, jrte->joinaliasvars)
	{
		Var		   *aliasvar = (Var *) lfirst(lc);

		/* get rid of any implicit coercion above the Var */
		aliasvar = (Var *) strip_implicit_coercions((Node *) aliasvar);

		if (aliasvar == NULL)
		{
			/* It's a dropped column; nothing to do here */
		}
		else if (IsA(aliasvar, Var))
		{
			Assert(aliasvar->varlevelsup == 0);
			Assert(aliasvar->varattno != 0);
			if (aliasvar->varno == colinfo->leftrti)
				colinfo->leftattnos[i] = aliasvar->varattno;
			else if (aliasvar->varno == colinfo->rightrti)
				colinfo->rightattnos[i] = aliasvar->varattno;
			else
				elog(ERROR, "unexpected varno %d in JOIN RTE",
					 aliasvar->varno);
		}
		else if (IsA(aliasvar, CoalesceExpr))
		{
			/*
			 * It's a merged column in FULL JOIN USING.  Ignore it for now and
			 * let the code below identify the merged columns.
			 */
		}
		else
			elog(ERROR, "unrecognized node type in join alias vars: %d",
				 (int) nodeTag(aliasvar));

		i++;
	}

	/*
	 * If there's a USING clause, deconstruct the join quals to identify the
	 * merged columns.  This is a tad painful but if we cannot rely on the
	 * column names, there is no other representation of which columns were
	 * joined by USING.  (Unless the join type is FULL, we can't tell from the
	 * joinaliasvars list which columns are merged.)  Note: we assume that the
	 * merged columns are the first output column(s) of the join.
	 */
	if (j->usingClause)
	{
		List	   *leftvars = NIL;
		List	   *rightvars = NIL;
		ListCell   *lc2;

		/* Extract left- and right-side Vars from the qual expression */
		flatten_join_using_qual(j->quals, &leftvars, &rightvars);
		Assert(list_length(leftvars) == list_length(j->usingClause));
		Assert(list_length(rightvars) == list_length(j->usingClause));

		/* Mark the output columns accordingly */
		i = 0;
		forboth(lc, leftvars, lc2, rightvars)
		{
			Var		   *leftvar = (Var *) lfirst(lc);
			Var		   *rightvar = (Var *) lfirst(lc2);

			Assert(leftvar->varlevelsup == 0);
			Assert(leftvar->varattno != 0);
			if (leftvar->varno != colinfo->leftrti)
				elog(ERROR, "unexpected varno %d in JOIN USING qual",
					 leftvar->varno);
			colinfo->leftattnos[i] = leftvar->varattno;

			Assert(rightvar->varlevelsup == 0);
			Assert(rightvar->varattno != 0);
			if (rightvar->varno != colinfo->rightrti)
				elog(ERROR, "unexpected varno %d in JOIN USING qual",
					 rightvar->varno);
			colinfo->rightattnos[i] = rightvar->varattno;

			i++;
		}
	}
}

/*
 * flatten_join_using_qual: extract Vars being joined from a JOIN/USING qual
 *
 * We assume that transformJoinUsingClause won't have produced anything except
 * AND nodes, equality operator nodes, and possibly implicit coercions, and
 * that the AND node inputs match left-to-right with the original USING list.
 *
 * Caller must initialize the result lists to NIL.
 */
static void
flatten_join_using_qual(Node *qual, List **leftvars, List **rightvars)
{
	if (IsA(qual, BoolExpr))
	{
		/* Handle AND nodes by recursion */
		BoolExpr   *b = (BoolExpr *) qual;
		ListCell   *lc;

		Assert(b->boolop == AND_EXPR);
		foreach(lc, b->args)
		{
			flatten_join_using_qual((Node *) lfirst(lc),
									leftvars, rightvars);
		}
	}
	else if (IsA(qual, OpExpr))
	{
		/* Otherwise we should have an equality operator */
		OpExpr	   *op = (OpExpr *) qual;
		Var		   *var;

		if (list_length(op->args) != 2)
			elog(ERROR, "unexpected unary operator in JOIN/USING qual");
		/* Arguments should be Vars with perhaps implicit coercions */
		var = (Var *) strip_implicit_coercions((Node *) linitial(op->args));
		if (!IsA(var, Var))
			elog(ERROR, "unexpected node type in JOIN/USING qual: %d",
				 (int) nodeTag(var));
		*leftvars = lappend(*leftvars, var);
		var = (Var *) strip_implicit_coercions((Node *) lsecond(op->args));
		if (!IsA(var, Var))
			elog(ERROR, "unexpected node type in JOIN/USING qual: %d",
				 (int) nodeTag(var));
		*rightvars = lappend(*rightvars, var);
	}
	else
	{
		/* Perhaps we have an implicit coercion to boolean? */
		Node	   *q = strip_implicit_coercions(qual);

		if (q != qual)
			flatten_join_using_qual(q, leftvars, rightvars);
		else
			elog(ERROR, "unexpected node type in JOIN/USING qual: %d",
				 (int) nodeTag(qual));
	}
}

/*
 * get_rtable_name: convenience function to get a previously assigned RTE alias
 *
 * The RTE must belong to the topmost namespace level in "context".
 */
static char *
get_rtable_name(int rtindex, deparse_context *context)
{
	deparse_namespace *dpns = (deparse_namespace *) linitial(context->namespaces);

	Assert(rtindex > 0 && rtindex <= list_length(dpns->rtable_names));
	return (char *) list_nth(dpns->rtable_names, rtindex - 1);
}

/*
 * set_deparse_planstate: set up deparse_namespace to parse subexpressions
 * of a given PlanState node
 *
 * This sets the planstate, outer_planstate, inner_planstate, outer_tlist,
 * inner_tlist, and index_tlist fields.  Caller is responsible for adjusting
 * the ancestors list if necessary.  Note that the rtable and ctes fields do
 * not need to change when shifting attention to different plan nodes in a
 * single plan tree.
 */
static void
set_deparse_planstate(deparse_namespace *dpns, PlanState *ps)
{
	dpns->planstate = ps;

	/*
	 * We special-case Append and MergeAppend to pretend that the first child
	 * plan is the OUTER referent; we have to interpret OUTER Vars in their
	 * tlists according to one of the children, and the first one is the most
	 * natural choice.  Likewise special-case ModifyTable to pretend that the
	 * first child plan is the OUTER referent; this is to support RETURNING
	 * lists containing references to non-target relations.
	 */
	if (IsA(ps, AppendState))
		dpns->outer_planstate = ((AppendState *) ps)->appendplans[0];
	else if (IsA(ps, MergeAppendState))
		dpns->outer_planstate = ((MergeAppendState *) ps)->mergeplans[0];
	else if (IsA(ps, ModifyTableState))
		dpns->outer_planstate = ((ModifyTableState *) ps)->mt_plans[0];
	else
		dpns->outer_planstate = outerPlanState(ps);

	if (dpns->outer_planstate)
		dpns->outer_tlist = dpns->outer_planstate->plan->targetlist;
	else
		dpns->outer_tlist = NIL;

	/*
	 * For a SubqueryScan, pretend the subplan is INNER referent.  (We don't
	 * use OUTER because that could someday conflict with the normal meaning.)
	 * Likewise, for a CteScan, pretend the subquery's plan is INNER referent.
	 * For ON CONFLICT .. UPDATE we just need the inner tlist to point to the
	 * excluded expression's tlist. (Similar to the SubqueryScan we don't want
	 * to reuse OUTER, it's used for RETURNING in some modify table cases,
	 * although not INSERT .. CONFLICT).
	 */
	if (IsA(ps, SubqueryScanState))
		dpns->inner_planstate = ((SubqueryScanState *) ps)->subplan;
	else if (IsA(ps, CteScanState))
		dpns->inner_planstate = ((CteScanState *) ps)->cteplanstate;
	else if (IsA(ps, ModifyTableState))
		dpns->inner_planstate = ps;
	else
		dpns->inner_planstate = innerPlanState(ps);

	if (IsA(ps, ModifyTableState))
		dpns->inner_tlist = ((ModifyTableState *) ps)->mt_excludedtlist;
	else if (dpns->inner_planstate)
		dpns->inner_tlist = dpns->inner_planstate->plan->targetlist;
	else
		dpns->inner_tlist = NIL;

	/* Set up referent for INDEX_VAR Vars, if needed */
	if (IsA(ps->plan, IndexOnlyScan))
		dpns->index_tlist = ((IndexOnlyScan *) ps->plan)->indextlist;
	else if (IsA(ps->plan, ForeignScan))
		dpns->index_tlist = ((ForeignScan *) ps->plan)->fdw_scan_tlist;
	else if (IsA(ps->plan, CustomScan))
		dpns->index_tlist = ((CustomScan *) ps->plan)->custom_scan_tlist;
	else
		dpns->index_tlist = NIL;
}

/*
 * push_child_plan: temporarily transfer deparsing attention to a child plan
 *
 * When expanding an OUTER_VAR or INNER_VAR reference, we must adjust the
 * deparse context in case the referenced expression itself uses
 * OUTER_VAR/INNER_VAR.  We modify the top stack entry in-place to avoid
 * affecting levelsup issues (although in a Plan tree there really shouldn't
 * be any).
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_child_plan.
 */
static void
push_child_plan(deparse_namespace *dpns, PlanState *ps,
				deparse_namespace *save_dpns)
{
	/* Save state for restoration later */
	*save_dpns = *dpns;

	/* Link current plan node into ancestors list */
	dpns->ancestors = lcons(dpns->planstate, dpns->ancestors);

	/* Set attention on selected child */
	set_deparse_planstate(dpns, ps);
}

/*
 * pop_child_plan: undo the effects of push_child_plan
 */
static void
pop_child_plan(deparse_namespace *dpns, deparse_namespace *save_dpns)
{
	List	   *ancestors;

	/* Get rid of ancestors list cell added by push_child_plan */
	ancestors = list_delete_first(dpns->ancestors);

	/* Restore fields changed by push_child_plan */
	*dpns = *save_dpns;

	/* Make sure dpns->ancestors is right (may be unnecessary) */
	dpns->ancestors = ancestors;
}

/*
 * push_ancestor_plan: temporarily transfer deparsing attention to an
 * ancestor plan
 *
 * When expanding a Param reference, we must adjust the deparse context
 * to match the plan node that contains the expression being printed;
 * otherwise we'd fail if that expression itself contains a Param or
 * OUTER_VAR/INNER_VAR/INDEX_VAR variable.
 *
 * The target ancestor is conveniently identified by the ListCell holding it
 * in dpns->ancestors.
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_ancestor_plan.
 */
static void
push_ancestor_plan(deparse_namespace *dpns, ListCell *ancestor_cell,
				   deparse_namespace *save_dpns)
{
	PlanState  *ps = (PlanState *) lfirst(ancestor_cell);
	List	   *ancestors;

	/* Save state for restoration later */
	*save_dpns = *dpns;

	/* Build a new ancestor list with just this node's ancestors */
	ancestors = NIL;
	while ((ancestor_cell = lnext(ancestor_cell)) != NULL)
		ancestors = lappend(ancestors, lfirst(ancestor_cell));
	dpns->ancestors = ancestors;

	/* Set attention on selected ancestor */
	set_deparse_planstate(dpns, ps);
}

/*
 * pop_ancestor_plan: undo the effects of push_ancestor_plan
 */
static void
pop_ancestor_plan(deparse_namespace *dpns, deparse_namespace *save_dpns)
{
	/* Free the ancestor list made in push_ancestor_plan */
	list_free(dpns->ancestors);

	/* Restore fields changed by push_ancestor_plan */
	*dpns = *save_dpns;
}


/* ----------
 * make_ruledef			- reconstruct the CREATE RULE command
 *				  for a given pg_rewrite tuple
 * ----------
 */
static void
make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags)
{
	char	   *rulename;
	char		ev_type;
	Oid			ev_class;
	bool		is_instead;
	char	   *ev_qual;
	char	   *ev_action;
	List	   *actions = NIL;
	int			fno;
	Datum		dat;
	bool		isnull;

	/*
	 * Get the attribute values from the rules tuple
	 */
	fno = SPI_fnumber(rulettc, "rulename");
	dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
	Assert(!isnull);
	rulename = NameStr(*(DatumGetName(dat)));

	fno = SPI_fnumber(rulettc, "ev_type");
	dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
	Assert(!isnull);
	ev_type = DatumGetChar(dat);

	fno = SPI_fnumber(rulettc, "ev_class");
	dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
	Assert(!isnull);
	ev_class = DatumGetObjectId(dat);

	fno = SPI_fnumber(rulettc, "is_instead");
	dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
	Assert(!isnull);
	is_instead = DatumGetBool(dat);

	/* these could be nulls */
	fno = SPI_fnumber(rulettc, "ev_qual");
	ev_qual = SPI_getvalue(ruletup, rulettc, fno);

	fno = SPI_fnumber(rulettc, "ev_action");
	ev_action = SPI_getvalue(ruletup, rulettc, fno);
	if (ev_action != NULL)
		actions = (List *) stringToNode(ev_action);

	/*
	 * Build the rules definition text
	 */
	appendStringInfo(buf, "CREATE RULE %s AS",
					 quote_identifier(rulename));

	if (prettyFlags & PRETTYFLAG_INDENT)
		appendStringInfoString(buf, "\n    ON ");
	else
		appendStringInfoString(buf, " ON ");

	/* The event the rule is fired for */
	switch (ev_type)
	{
		case '1':
			appendStringInfoString(buf, "SELECT");
			break;

		case '2':
			appendStringInfoString(buf, "UPDATE");
			break;

		case '3':
			appendStringInfoString(buf, "INSERT");
			break;

		case '4':
			appendStringInfoString(buf, "DELETE");
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("rule \"%s\" has unsupported event type %d",
							rulename, ev_type)));
			break;
	}

	/* The relation the rule is fired on */
	appendStringInfo(buf, " TO %s", generate_relation_name(ev_class, NIL));

	/* If the rule has an event qualification, add it */
	if (ev_qual == NULL)
		ev_qual = "";
	if (strlen(ev_qual) > 0 && strcmp(ev_qual, "<>") != 0)
	{
		Node	   *qual;
		Query	   *query;
		deparse_context context;
		deparse_namespace dpns;

		if (prettyFlags & PRETTYFLAG_INDENT)
			appendStringInfoString(buf, "\n  ");
		appendStringInfoString(buf, " WHERE ");

		qual = stringToNode(ev_qual);

		/*
		 * We need to make a context for recognizing any Vars in the qual
		 * (which can only be references to OLD and NEW).  Use the rtable of
		 * the first query in the action list for this purpose.
		 */
		query = (Query *) linitial(actions);

		/*
		 * If the action is INSERT...SELECT, OLD/NEW have been pushed down
		 * into the SELECT, and that's what we need to look at. (Ugly kluge
		 * ... try to fix this when we redesign querytrees.)
		 */
		query = getInsertSelectQuery(query, NULL);

		/* Must acquire locks right away; see notes in get_query_def() */
		AcquireRewriteLocks(query, false, false);

		context.buf = buf;
		context.namespaces = list_make1(&dpns);
		context.windowClause = NIL;
		context.windowTList = NIL;
		context.varprefix = (list_length(query->rtable) != 1);
		context.prettyFlags = prettyFlags;
		context.wrapColumn = WRAP_COLUMN_DEFAULT;
		context.indentLevel = PRETTYINDENT_STD;
		context.special_exprkind = EXPR_KIND_NONE;

		set_deparse_for_query(&dpns, query, NIL);

		get_rule_expr(qual, &context, false);
	}

	appendStringInfoString(buf, " DO ");

	/* The INSTEAD keyword (if so) */
	if (is_instead)
		appendStringInfoString(buf, "INSTEAD ");

	/* Finally the rules actions */
	if (list_length(actions) > 1)
	{
		ListCell   *action;
		Query	   *query;

		appendStringInfoChar(buf, '(');
		foreach(action, actions)
		{
			query = (Query *) lfirst(action);
			get_query_def(query, buf, NIL, NULL,
						  prettyFlags, WRAP_COLUMN_DEFAULT, 0);
			if (prettyFlags)
				appendStringInfoString(buf, ";\n");
			else
				appendStringInfoString(buf, "; ");
		}
		appendStringInfoString(buf, ");");
	}
	else if (list_length(actions) == 0)
	{
		appendStringInfoString(buf, "NOTHING;");
	}
	else
	{
		Query	   *query;

		query = (Query *) linitial(actions);
		get_query_def(query, buf, NIL, NULL,
					  prettyFlags, WRAP_COLUMN_DEFAULT, 0);
		appendStringInfoChar(buf, ';');
	}
}


/* ----------
 * make_viewdef			- reconstruct the SELECT part of a
 *				  view rewrite rule
 * ----------
 */
static void
make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags, int wrapColumn)
{
	Query	   *query;
	char		ev_type;
	Oid			ev_class;
	bool		is_instead;
	char	   *ev_qual;
	char	   *ev_action;
	List	   *actions = NIL;
	Relation	ev_relation;
	int			fno;
	bool		isnull;

	/*
	 * Get the attribute values from the rules tuple
	 */
	fno = SPI_fnumber(rulettc, "ev_type");
	ev_type = (char) SPI_getbinval(ruletup, rulettc, fno, &isnull);

	fno = SPI_fnumber(rulettc, "ev_class");
	ev_class = (Oid) SPI_getbinval(ruletup, rulettc, fno, &isnull);

	fno = SPI_fnumber(rulettc, "is_instead");
	is_instead = (bool) SPI_getbinval(ruletup, rulettc, fno, &isnull);

	fno = SPI_fnumber(rulettc, "ev_qual");
	ev_qual = SPI_getvalue(ruletup, rulettc, fno);

	fno = SPI_fnumber(rulettc, "ev_action");
	ev_action = SPI_getvalue(ruletup, rulettc, fno);
	if (ev_action != NULL)
		actions = (List *) stringToNode(ev_action);

	if (list_length(actions) != 1)
	{
		/* keep output buffer empty and leave */
		return;
	}

	query = (Query *) linitial(actions);

	if (ev_type != '1' || !is_instead ||
		strcmp(ev_qual, "<>") != 0 || query->commandType != CMD_SELECT)
	{
		/* keep output buffer empty and leave */
		return;
	}

	ev_relation = heap_open(ev_class, AccessShareLock);

	get_query_def(query, buf, NIL, RelationGetDescr(ev_relation),
				  prettyFlags, wrapColumn, 0);
	appendStringInfoChar(buf, ';');

	heap_close(ev_relation, AccessShareLock);
}


/* ----------
 * get_query_def			- Parse back one query parsetree
 *
 * If resultDesc is not NULL, then it is the output tuple descriptor for
 * the view represented by a SELECT query.
 * ----------
 */
static void
get_query_def(Query *query, StringInfo buf, List *parentnamespace,
			  TupleDesc resultDesc,
			  int prettyFlags, int wrapColumn, int startIndent)
{
	deparse_context context;
	deparse_namespace dpns;

	/* Guard against excessively long or deeply-nested queries */
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	/*
	 * Before we begin to examine the query, acquire locks on referenced
	 * relations, and fix up deleted columns in JOIN RTEs.  This ensures
	 * consistent results.  Note we assume it's OK to scribble on the passed
	 * querytree!
	 *
	 * We are only deparsing the query (we are not about to execute it), so we
	 * only need AccessShareLock on the relations it mentions.
	 */
	AcquireRewriteLocks(query, false, false);

	context.buf = buf;
	context.namespaces = lcons(&dpns, list_copy(parentnamespace));
	context.windowClause = NIL;
	context.windowTList = NIL;
	context.varprefix = (parentnamespace != NIL ||
						 list_length(query->rtable) != 1);
	context.prettyFlags = prettyFlags;
	context.wrapColumn = wrapColumn;
	context.indentLevel = startIndent;
	context.special_exprkind = EXPR_KIND_NONE;

	set_deparse_for_query(&dpns, query, parentnamespace);

	switch (query->commandType)
	{
		case CMD_SELECT:
			get_select_query_def(query, &context, resultDesc);
			break;

		case CMD_UPDATE:
			get_update_query_def(query, &context);
			break;

		case CMD_INSERT:
			get_insert_query_def(query, &context);
			break;

		case CMD_DELETE:
			get_delete_query_def(query, &context);
			break;

		case CMD_NOTHING:
			appendStringInfoString(buf, "NOTHING");
			break;

		case CMD_UTILITY:
			get_utility_query_def(query, &context);
			break;

		default:
			elog(ERROR, "unrecognized query command type: %d",
				 query->commandType);
			break;
	}
}

/* ----------
 * get_values_def			- Parse back a VALUES list
 * ----------
 */
static void
get_values_def(List *values_lists, deparse_context *context)
{
	StringInfo	buf = context->buf;
	bool		first_list = true;
	ListCell   *vtl;

	appendStringInfoString(buf, "VALUES ");

	foreach(vtl, values_lists)
	{
		List	   *sublist = (List *) lfirst(vtl);
		bool		first_col = true;
		ListCell   *lc;

		if (first_list)
			first_list = false;
		else
			appendStringInfoString(buf, ", ");

		appendStringInfoChar(buf, '(');
		foreach(lc, sublist)
		{
			Node	   *col = (Node *) lfirst(lc);

			if (first_col)
				first_col = false;
			else
				appendStringInfoChar(buf, ',');

			/*
			 * Print the value.  Whole-row Vars need special treatment.
			 */
			get_rule_expr_toplevel(col, context, false);
		}
		appendStringInfoChar(buf, ')');
	}
}

/* ----------
 * get_with_clause			- Parse back a WITH clause
 * ----------
 */
static void
get_with_clause(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	const char *sep;
	ListCell   *l;

	if (query->cteList == NIL)
		return;

	if (PRETTY_INDENT(context))
	{
		context->indentLevel += PRETTYINDENT_STD;
		appendStringInfoChar(buf, ' ');
	}

	if (query->hasRecursive)
		sep = "WITH RECURSIVE ";
	else
		sep = "WITH ";
	foreach(l, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);

		appendStringInfoString(buf, sep);
		appendStringInfoString(buf, quote_identifier(cte->ctename));
		if (cte->aliascolnames)
		{
			bool		first = true;
			ListCell   *col;

			appendStringInfoChar(buf, '(');
			foreach(col, cte->aliascolnames)
			{
				if (first)
					first = false;
				else
					appendStringInfoString(buf, ", ");
				appendStringInfoString(buf,
									   quote_identifier(strVal(lfirst(col))));
			}
			appendStringInfoChar(buf, ')');
		}
		appendStringInfoString(buf, " AS (");
		if (PRETTY_INDENT(context))
			appendContextKeyword(context, "", 0, 0, 0);
		get_query_def((Query *) cte->ctequery, buf, context->namespaces, NULL,
					  context->prettyFlags, context->wrapColumn,
					  context->indentLevel);
		if (PRETTY_INDENT(context))
			appendContextKeyword(context, "", 0, 0, 0);
		appendStringInfoChar(buf, ')');
		sep = ", ";
	}

	if (PRETTY_INDENT(context))
	{
		context->indentLevel -= PRETTYINDENT_STD;
		appendContextKeyword(context, "", 0, 0, 0);
	}
	else
		appendStringInfoChar(buf, ' ');
}

/* ----------
 * get_select_query_def			- Parse back a SELECT parsetree
 * ----------
 */
static void
get_select_query_def(Query *query, deparse_context *context,
					 TupleDesc resultDesc)
{
	StringInfo	buf = context->buf;
	List	   *save_windowclause;
	List	   *save_windowtlist;
	bool		force_colno;
	ListCell   *l;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/* Set up context for possible window functions */
	save_windowclause = context->windowClause;
	context->windowClause = query->windowClause;
	save_windowtlist = context->windowTList;
	context->windowTList = query->targetList;

	/*
	 * If the Query node has a setOperations tree, then it's the top level of
	 * a UNION/INTERSECT/EXCEPT query; only the WITH, ORDER BY and LIMIT
	 * fields are interesting in the top query itself.
	 */
	if (query->setOperations)
	{
		get_setop_query(query->setOperations, query, context, resultDesc);
		/* ORDER BY clauses must be simple in this case */
		force_colno = true;
	}
	else
	{
		get_basic_select_query(query, context, resultDesc);
		force_colno = false;
	}

	/* Add the ORDER BY clause if given */
	if (query->sortClause != NIL)
	{
		appendContextKeyword(context, " ORDER BY ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_rule_orderby(query->sortClause, query->targetList,
						 force_colno, context);
	}

	/* Add the LIMIT clause if given */
	if (query->limitOffset != NULL)
	{
		appendContextKeyword(context, " OFFSET ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
		get_rule_expr(query->limitOffset, context, false);
	}
	if (query->limitCount != NULL)
	{
		appendContextKeyword(context, " LIMIT ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
		if (IsA(query->limitCount, Const) &&
			((Const *) query->limitCount)->constisnull)
			appendStringInfoString(buf, "ALL");
		else
			get_rule_expr(query->limitCount, context, false);
	}

	/* Add FOR [KEY] UPDATE/SHARE clauses if present */
	if (query->hasForUpdate)
	{
		foreach(l, query->rowMarks)
		{
			RowMarkClause *rc = (RowMarkClause *) lfirst(l);

			/* don't print implicit clauses */
			if (rc->pushedDown)
				continue;

			switch (rc->strength)
			{
				case LCS_NONE:
					/* we intentionally throw an error for LCS_NONE */
					elog(ERROR, "unrecognized LockClauseStrength %d",
						 (int) rc->strength);
					break;
				case LCS_FORKEYSHARE:
					appendContextKeyword(context, " FOR KEY SHARE",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
					break;
				case LCS_FORSHARE:
					appendContextKeyword(context, " FOR SHARE",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
					break;
				case LCS_FORNOKEYUPDATE:
					appendContextKeyword(context, " FOR NO KEY UPDATE",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
					break;
				case LCS_FORUPDATE:
					appendContextKeyword(context, " FOR UPDATE",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
					break;
			}

			appendStringInfo(buf, " OF %s",
							 quote_identifier(get_rtable_name(rc->rti,
															  context)));
			if (rc->waitPolicy == LockWaitError)
				appendStringInfoString(buf, " NOWAIT");
			else if (rc->waitPolicy == LockWaitSkip)
				appendStringInfoString(buf, " SKIP LOCKED");
		}
	}

	context->windowClause = save_windowclause;
	context->windowTList = save_windowtlist;
}

/*
 * Detect whether query looks like SELECT ... FROM VALUES();
 * if so, return the VALUES RTE.  Otherwise return NULL.
 */
static RangeTblEntry *
get_simple_values_rte(Query *query)
{
	RangeTblEntry *result = NULL;
	ListCell   *lc;

	/*
	 * We want to return TRUE even if the Query also contains OLD or NEW rule
	 * RTEs.  So the idea is to scan the rtable and see if there is only one
	 * inFromCl RTE that is a VALUES RTE.
	 */
	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_VALUES && rte->inFromCl)
		{
			if (result)
				return NULL;	/* multiple VALUES (probably not possible) */
			result = rte;
		}
		else if (rte->rtekind == RTE_RELATION && !rte->inFromCl)
			continue;			/* ignore rule entries */
		else
			return NULL;		/* something else -> not simple VALUES */
	}

	/*
	 * We don't need to check the targetlist in any great detail, because
	 * parser/analyze.c will never generate a "bare" VALUES RTE --- they only
	 * appear inside auto-generated sub-queries with very restricted
	 * structure.  However, DefineView might have modified the tlist by
	 * injecting new column aliases; so compare tlist resnames against the
	 * RTE's names to detect that.
	 */
	if (result)
	{
		ListCell   *lcn;

		if (list_length(query->targetList) != list_length(result->eref->colnames))
			return NULL;		/* this probably cannot happen */
		forboth(lc, query->targetList, lcn, result->eref->colnames)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			char	   *cname = strVal(lfirst(lcn));

			if (tle->resjunk)
				return NULL;	/* this probably cannot happen */
			if (tle->resname == NULL || strcmp(tle->resname, cname) != 0)
				return NULL;	/* column name has been changed */
		}
	}

	return result;
}

static void
get_basic_select_query(Query *query, deparse_context *context,
					   TupleDesc resultDesc)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *values_rte;
	char	   *sep;
	ListCell   *l;

	if (PRETTY_INDENT(context))
	{
		context->indentLevel += PRETTYINDENT_STD;
		appendStringInfoChar(buf, ' ');
	}

	/*
	 * If the query looks like SELECT * FROM (VALUES ...), then print just the
	 * VALUES part.  This reverses what transformValuesClause() did at parse
	 * time.
	 */
	values_rte = get_simple_values_rte(query);
	if (values_rte)
	{
		get_values_def(values_rte->values_lists, context);
		return;
	}

	/*
	 * Build up the query string - first we say SELECT
	 */
	appendStringInfoString(buf, "SELECT");

	/* Add the DISTINCT clause if given */
	if (query->distinctClause != NIL)
	{
		if (query->hasDistinctOn)
		{
			appendStringInfoString(buf, " DISTINCT ON (");
			sep = "";
			foreach(l, query->distinctClause)
			{
				SortGroupClause *srt = (SortGroupClause *) lfirst(l);

				appendStringInfoString(buf, sep);
				get_rule_sortgroupclause(srt->tleSortGroupRef, query->targetList,
										 false, context);
				sep = ", ";
			}
			appendStringInfoChar(buf, ')');
		}
		else
			appendStringInfoString(buf, " DISTINCT");
	}

	/* Then we tell what to select (the targetlist) */
	get_target_list(query->targetList, context, resultDesc);

	/* Add the FROM clause if needed */
	get_from_clause(query, " FROM ", context);

	/* Add the WHERE clause if given */
	if (query->jointree->quals != NULL)
	{
		appendContextKeyword(context, " WHERE ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_rule_expr(query->jointree->quals, context, false);
	}

	/* Add the GROUP BY clause if given */
	if (query->groupClause != NULL || query->groupingSets != NULL)
	{
		ParseExprKind save_exprkind;

		appendContextKeyword(context, " GROUP BY ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);

		save_exprkind = context->special_exprkind;
		context->special_exprkind = EXPR_KIND_GROUP_BY;

		if (query->groupingSets == NIL)
		{
			sep = "";
			foreach(l, query->groupClause)
			{
				SortGroupClause *grp = (SortGroupClause *) lfirst(l);

				appendStringInfoString(buf, sep);
				get_rule_sortgroupclause(grp->tleSortGroupRef, query->targetList,
										 false, context);
				sep = ", ";
			}
		}
		else
		{
			sep = "";
			foreach(l, query->groupingSets)
			{
				GroupingSet *grp = lfirst(l);

				appendStringInfoString(buf, sep);
				get_rule_groupingset(grp, query->targetList, true, context);
				sep = ", ";
			}
		}

		context->special_exprkind = save_exprkind;
	}

	/* Add the HAVING clause if given */
	if (query->havingQual != NULL)
	{
		appendContextKeyword(context, " HAVING ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
		get_rule_expr(query->havingQual, context, false);
	}

	/* Add the WINDOW clause if needed */
	if (query->windowClause != NIL)
		get_rule_windowclause(query, context);
}

/* ----------
 * get_target_list			- Parse back a SELECT target list
 *
 * This is also used for RETURNING lists in INSERT/UPDATE/DELETE.
 * ----------
 */
static void
get_target_list(List *targetList, deparse_context *context,
				TupleDesc resultDesc)
{
	StringInfo	buf = context->buf;
	StringInfoData targetbuf;
	bool		last_was_multiline = false;
	char	   *sep;
	int			colno;
	ListCell   *l;

	/* we use targetbuf to hold each TLE's text temporarily */
	initStringInfo(&targetbuf);

	sep = " ";
	colno = 0;
	foreach(l, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);
		char	   *colname;
		char	   *attname;

		if (tle->resjunk)
			continue;			/* ignore junk entries */

		appendStringInfoString(buf, sep);
		sep = ", ";
		colno++;

		/*
		 * Put the new field text into targetbuf so we can decide after we've
		 * got it whether or not it needs to go on a new line.
		 */
		resetStringInfo(&targetbuf);
		context->buf = &targetbuf;

		/*
		 * We special-case Var nodes rather than using get_rule_expr. This is
		 * needed because get_rule_expr will display a whole-row Var as
		 * "foo.*", which is the preferred notation in most contexts, but at
		 * the top level of a SELECT list it's not right (the parser will
		 * expand that notation into multiple columns, yielding behavior
		 * different from a whole-row Var).  We need to call get_variable
		 * directly so that we can tell it to do the right thing, and so that
		 * we can get the attribute name which is the default AS label.
		 */
		if (tle->expr && (IsA(tle->expr, Var)))
		{
			attname = get_variable((Var *) tle->expr, 0, true, context);
		}
		else
		{
			get_rule_expr((Node *) tle->expr, context, true);
			/* We'll show the AS name unless it's this: */
			attname = "?column?";
		}

		/*
		 * Figure out what the result column should be called.  In the context
		 * of a view, use the view's tuple descriptor (so as to pick up the
		 * effects of any column RENAME that's been done on the view).
		 * Otherwise, just use what we can find in the TLE.
		 */
		if (resultDesc && colno <= resultDesc->natts)
			colname = NameStr(resultDesc->attrs[colno - 1]->attname);
		else
			colname = tle->resname;

		/* Show AS unless the column's name is correct as-is */
		if (colname)			/* resname could be NULL */
		{
			if (attname == NULL || strcmp(attname, colname) != 0)
				appendStringInfo(&targetbuf, " AS %s", quote_identifier(colname));
		}

		/* Restore context's output buffer */
		context->buf = buf;

		/* Consider line-wrapping if enabled */
		if (PRETTY_INDENT(context) && context->wrapColumn >= 0)
		{
			int			leading_nl_pos;

			/* Does the new field start with a new line? */
			if (targetbuf.len > 0 && targetbuf.data[0] == '\n')
				leading_nl_pos = 0;
			else
				leading_nl_pos = -1;

			/* If so, we shouldn't add anything */
			if (leading_nl_pos >= 0)
			{
				/* instead, remove any trailing spaces currently in buf */
				removeStringInfoSpaces(buf);
			}
			else
			{
				char	   *trailing_nl;

				/* Locate the start of the current line in the output buffer */
				trailing_nl = strrchr(buf->data, '\n');
				if (trailing_nl == NULL)
					trailing_nl = buf->data;
				else
					trailing_nl++;

				/*
				 * Add a newline, plus some indentation, if the new field is
				 * not the first and either the new field would cause an
				 * overflow or the last field used more than one line.
				 */
				if (colno > 1 &&
					((strlen(trailing_nl) + targetbuf.len > context->wrapColumn) ||
					 last_was_multiline))
					appendContextKeyword(context, "", -PRETTYINDENT_STD,
										 PRETTYINDENT_STD, PRETTYINDENT_VAR);
			}

			/* Remember this field's multiline status for next iteration */
			last_was_multiline =
				(strchr(targetbuf.data + leading_nl_pos + 1, '\n') != NULL);
		}

		/* Add the new field */
		appendStringInfoString(buf, targetbuf.data);
	}

	/* clean up */
	pfree(targetbuf.data);
}

static void
get_setop_query(Node *setOp, Query *query, deparse_context *context,
				TupleDesc resultDesc)
{
	StringInfo	buf = context->buf;
	bool		need_paren;

	/* Guard against excessively long or deeply-nested queries */
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *rte = rt_fetch(rtr->rtindex, query->rtable);
		Query	   *subquery = rte->subquery;

		Assert(subquery != NULL);
		Assert(subquery->setOperations == NULL);
		/* Need parens if WITH, ORDER BY, FOR UPDATE, or LIMIT; see gram.y */
		need_paren = (subquery->cteList ||
					  subquery->sortClause ||
					  subquery->rowMarks ||
					  subquery->limitOffset ||
					  subquery->limitCount);
		if (need_paren)
			appendStringInfoChar(buf, '(');
		get_query_def(subquery, buf, context->namespaces, resultDesc,
					  context->prettyFlags, context->wrapColumn,
					  context->indentLevel);
		if (need_paren)
			appendStringInfoChar(buf, ')');
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;
		int			subindent;

		/*
		 * We force parens when nesting two SetOperationStmts, except when the
		 * lefthand input is another setop of the same kind.  Syntactically,
		 * we could omit parens in rather more cases, but it seems best to use
		 * parens to flag cases where the setop operator changes.  If we use
		 * parens, we also increase the indentation level for the child query.
		 *
		 * There are some cases in which parens are needed around a leaf query
		 * too, but those are more easily handled at the next level down (see
		 * code above).
		 */
		if (IsA(op->larg, SetOperationStmt))
		{
			SetOperationStmt *lop = (SetOperationStmt *) op->larg;

			if (op->op == lop->op && op->all == lop->all)
				need_paren = false;
			else
				need_paren = true;
		}
		else
			need_paren = false;

		if (need_paren)
		{
			appendStringInfoChar(buf, '(');
			subindent = PRETTYINDENT_STD;
			appendContextKeyword(context, "", subindent, 0, 0);
		}
		else
			subindent = 0;

		get_setop_query(op->larg, query, context, resultDesc);

		if (need_paren)
			appendContextKeyword(context, ") ", -subindent, 0, 0);
		else if (PRETTY_INDENT(context))
			appendContextKeyword(context, "", -subindent, 0, 0);
		else
			appendStringInfoChar(buf, ' ');

		switch (op->op)
		{
			case SETOP_UNION:
				appendStringInfoString(buf, "UNION ");
				break;
			case SETOP_INTERSECT:
				appendStringInfoString(buf, "INTERSECT ");
				break;
			case SETOP_EXCEPT:
				appendStringInfoString(buf, "EXCEPT ");
				break;
			default:
				elog(ERROR, "unrecognized set op: %d",
					 (int) op->op);
		}
		if (op->all)
			appendStringInfoString(buf, "ALL ");

		/* Always parenthesize if RHS is another setop */
		need_paren = IsA(op->rarg, SetOperationStmt);

		/*
		 * The indentation code here is deliberately a bit different from that
		 * for the lefthand input, because we want the line breaks in
		 * different places.
		 */
		if (need_paren)
		{
			appendStringInfoChar(buf, '(');
			subindent = PRETTYINDENT_STD;
		}
		else
			subindent = 0;
		appendContextKeyword(context, "", subindent, 0, 0);

		get_setop_query(op->rarg, query, context, resultDesc);

		if (PRETTY_INDENT(context))
			context->indentLevel -= subindent;
		if (need_paren)
			appendContextKeyword(context, ")", 0, 0, 0);
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
}

/*
 * Display a sort/group clause.
 *
 * Also returns the expression tree, so caller need not find it again.
 */
static Node *
get_rule_sortgroupclause(Index ref, List *tlist, bool force_colno,
						 deparse_context *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Node	   *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = (Node *) tle->expr;

	/*
	 * Use column-number form if requested by caller.  Otherwise, if
	 * expression is a constant, force it to be dumped with an explicit cast
	 * as decoration --- this is because a simple integer constant is
	 * ambiguous (and will be misinterpreted by findTargetlistEntry()) if we
	 * dump it without any decoration.  If it's anything more complex than a
	 * simple Var, then force extra parens around it, to ensure it can't be
	 * misinterpreted as a cube() or rollup() construct.
	 */
	if (force_colno)
	{
		Assert(!tle->resjunk);
		appendStringInfo(buf, "%d", tle->resno);
	}
	else if (expr && IsA(expr, Const))
		get_const_expr((Const *) expr, context, 1);
	else if (!expr || IsA(expr, Var))
		get_rule_expr(expr, context, true);
	else
	{
		/*
		 * We must force parens for function-like expressions even if
		 * PRETTY_PAREN is off, since those are the ones in danger of
		 * misparsing. For other expressions we need to force them only if
		 * PRETTY_PAREN is on, since otherwise the expression will output them
		 * itself. (We can't skip the parens.)
		 */
		bool		need_paren = (PRETTY_PAREN(context)
								  || IsA(expr, FuncExpr)
								  ||IsA(expr, Aggref)
								  ||IsA(expr, WindowFunc));

		if (need_paren)
			appendStringInfoString(context->buf, "(");
		get_rule_expr(expr, context, true);
		if (need_paren)
			appendStringInfoString(context->buf, ")");
	}

	return expr;
}

/*
 * Display a GroupingSet
 */
static void
get_rule_groupingset(GroupingSet *gset, List *targetlist,
					 bool omit_parens, deparse_context *context)
{
	ListCell   *l;
	StringInfo	buf = context->buf;
	bool		omit_child_parens = true;
	char	   *sep = "";

	switch (gset->kind)
	{
		case GROUPING_SET_EMPTY:
			appendStringInfoString(buf, "()");
			return;

		case GROUPING_SET_SIMPLE:
			{
				if (!omit_parens || list_length(gset->content) != 1)
					appendStringInfoString(buf, "(");

				foreach(l, gset->content)
				{
					Index		ref = lfirst_int(l);

					appendStringInfoString(buf, sep);
					get_rule_sortgroupclause(ref, targetlist,
											 false, context);
					sep = ", ";
				}

				if (!omit_parens || list_length(gset->content) != 1)
					appendStringInfoString(buf, ")");
			}
			return;

		case GROUPING_SET_ROLLUP:
			appendStringInfoString(buf, "ROLLUP(");
			break;
		case GROUPING_SET_CUBE:
			appendStringInfoString(buf, "CUBE(");
			break;
		case GROUPING_SET_SETS:
			appendStringInfoString(buf, "GROUPING SETS (");
			omit_child_parens = false;
			break;
	}

	foreach(l, gset->content)
	{
		appendStringInfoString(buf, sep);
		get_rule_groupingset(lfirst(l), targetlist, omit_child_parens, context);
		sep = ", ";
	}

	appendStringInfoString(buf, ")");
}

/*
 * Display an ORDER BY list.
 */
static void
get_rule_orderby(List *orderList, List *targetList,
				 bool force_colno, deparse_context *context)
{
	StringInfo	buf = context->buf;
	const char *sep;
	ListCell   *l;

	sep = "";
	foreach(l, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(l);
		Node	   *sortexpr;
		Oid			sortcoltype;
		TypeCacheEntry *typentry;

		appendStringInfoString(buf, sep);
		sortexpr = get_rule_sortgroupclause(srt->tleSortGroupRef, targetList,
											force_colno, context);
		sortcoltype = exprType(sortexpr);
		/* See whether operator is default < or > for datatype */
		typentry = lookup_type_cache(sortcoltype,
									 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
		if (srt->sortop == typentry->lt_opr)
		{
			/* ASC is default, so emit nothing for it */
			if (srt->nulls_first)
				appendStringInfoString(buf, " NULLS FIRST");
		}
		else if (srt->sortop == typentry->gt_opr)
		{
			appendStringInfoString(buf, " DESC");
			/* DESC defaults to NULLS FIRST */
			if (!srt->nulls_first)
				appendStringInfoString(buf, " NULLS LAST");
		}
		else
		{
			appendStringInfo(buf, " USING %s",
							 generate_operator_name(srt->sortop,
													sortcoltype,
													sortcoltype));
			/* be specific to eliminate ambiguity */
			if (srt->nulls_first)
				appendStringInfoString(buf, " NULLS FIRST");
			else
				appendStringInfoString(buf, " NULLS LAST");
		}
		sep = ", ";
	}
}

/*
 * Display a WINDOW clause.
 *
 * Note that the windowClause list might contain only anonymous window
 * specifications, in which case we should print nothing here.
 */
static void
get_rule_windowclause(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	const char *sep;
	ListCell   *l;

	sep = NULL;
	foreach(l, query->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);

		if (wc->name == NULL)
			continue;			/* ignore anonymous windows */

		if (sep == NULL)
			appendContextKeyword(context, " WINDOW ",
								 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		else
			appendStringInfoString(buf, sep);

		appendStringInfo(buf, "%s AS ", quote_identifier(wc->name));

		get_rule_windowspec(wc, query->targetList, context);

		sep = ", ";
	}
}

/*
 * Display a window definition
 */
static void
get_rule_windowspec(WindowClause *wc, List *targetList,
					deparse_context *context)
{
	StringInfo	buf = context->buf;
	bool		needspace = false;
	const char *sep;
	ListCell   *l;

	appendStringInfoChar(buf, '(');
	if (wc->refname)
	{
		appendStringInfoString(buf, quote_identifier(wc->refname));
		needspace = true;
	}
	/* partition clauses are always inherited, so only print if no refname */
	if (wc->partitionClause && !wc->refname)
	{
		if (needspace)
			appendStringInfoChar(buf, ' ');
		appendStringInfoString(buf, "PARTITION BY ");
		sep = "";
		foreach(l, wc->partitionClause)
		{
			SortGroupClause *grp = (SortGroupClause *) lfirst(l);

			appendStringInfoString(buf, sep);
			get_rule_sortgroupclause(grp->tleSortGroupRef, targetList,
									 false, context);
			sep = ", ";
		}
		needspace = true;
	}
	/* print ordering clause only if not inherited */
	if (wc->orderClause && !wc->copiedOrder)
	{
		if (needspace)
			appendStringInfoChar(buf, ' ');
		appendStringInfoString(buf, "ORDER BY ");
		get_rule_orderby(wc->orderClause, targetList, false, context);
		needspace = true;
	}
	/* framing clause is never inherited, so print unless it's default */
	if (wc->frameOptions & FRAMEOPTION_NONDEFAULT)
	{
		if (needspace)
			appendStringInfoChar(buf, ' ');
		if (wc->frameOptions & FRAMEOPTION_RANGE)
			appendStringInfoString(buf, "RANGE ");
		else if (wc->frameOptions & FRAMEOPTION_ROWS)
			appendStringInfoString(buf, "ROWS ");
		else
			Assert(false);
		if (wc->frameOptions & FRAMEOPTION_BETWEEN)
			appendStringInfoString(buf, "BETWEEN ");
		if (wc->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)
			appendStringInfoString(buf, "UNBOUNDED PRECEDING ");
		else if (wc->frameOptions & FRAMEOPTION_START_CURRENT_ROW)
			appendStringInfoString(buf, "CURRENT ROW ");
		else if (wc->frameOptions & FRAMEOPTION_START_VALUE)
		{
			get_rule_expr(wc->startOffset, context, false);
			if (wc->frameOptions & FRAMEOPTION_START_VALUE_PRECEDING)
				appendStringInfoString(buf, " PRECEDING ");
			else if (wc->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING)
				appendStringInfoString(buf, " FOLLOWING ");
			else
				Assert(false);
		}
		else
			Assert(false);
		if (wc->frameOptions & FRAMEOPTION_BETWEEN)
		{
			appendStringInfoString(buf, "AND ");
			if (wc->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING)
				appendStringInfoString(buf, "UNBOUNDED FOLLOWING ");
			else if (wc->frameOptions & FRAMEOPTION_END_CURRENT_ROW)
				appendStringInfoString(buf, "CURRENT ROW ");
			else if (wc->frameOptions & FRAMEOPTION_END_VALUE)
			{
				get_rule_expr(wc->endOffset, context, false);
				if (wc->frameOptions & FRAMEOPTION_END_VALUE_PRECEDING)
					appendStringInfoString(buf, " PRECEDING ");
				else if (wc->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING)
					appendStringInfoString(buf, " FOLLOWING ");
				else
					Assert(false);
			}
			else
				Assert(false);
		}
		/* we will now have a trailing space; remove it */
		buf->len--;
	}
	appendStringInfoChar(buf, ')');
}

/* ----------
 * get_insert_query_def			- Parse back an INSERT parsetree
 * ----------
 */
static void
get_insert_query_def(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *select_rte = NULL;
	RangeTblEntry *values_rte = NULL;
	RangeTblEntry *rte;
	char	   *sep;
	ListCell   *l;
	List	   *strippedexprs;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/*
	 * If it's an INSERT ... SELECT or multi-row VALUES, there will be a
	 * single RTE for the SELECT or VALUES.  Plain VALUES has neither.
	 */
	foreach(l, query->rtable)
	{
		rte = (RangeTblEntry *) lfirst(l);

		if (rte->rtekind == RTE_SUBQUERY)
		{
			if (select_rte)
				elog(ERROR, "too many subquery RTEs in INSERT");
			select_rte = rte;
		}

		if (rte->rtekind == RTE_VALUES)
		{
			if (values_rte)
				elog(ERROR, "too many values RTEs in INSERT");
			values_rte = rte;
		}
	}
	if (select_rte && values_rte)
		elog(ERROR, "both subquery and values RTEs in INSERT");

	/*
	 * Start the query with INSERT INTO relname
	 */
	rte = rt_fetch(query->resultRelation, query->rtable);
	Assert(rte->rtekind == RTE_RELATION);

	if (PRETTY_INDENT(context))
	{
		context->indentLevel += PRETTYINDENT_STD;
		appendStringInfoChar(buf, ' ');
	}
	appendStringInfo(buf, "INSERT INTO %s ",
					 generate_relation_name(rte->relid, NIL));
	/* INSERT requires AS keyword for target alias */
	if (rte->alias != NULL)
		appendStringInfo(buf, "AS %s ",
						 quote_identifier(rte->alias->aliasname));

	/*
	 * Add the insert-column-names list.  Any indirection decoration needed on
	 * the column names can be inferred from the top targetlist.
	 */
	strippedexprs = NIL;
	sep = "";
	if (query->targetList)
		appendStringInfoChar(buf, '(');
	foreach(l, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
			continue;			/* ignore junk entries */

		appendStringInfoString(buf, sep);
		sep = ", ";

		/*
		 * Put out name of target column; look in the catalogs, not at
		 * tle->resname, since resname will fail to track RENAME.
		 */
		appendStringInfoString(buf,
						quote_identifier(get_relid_attribute_name(rte->relid,
															   tle->resno)));

		/*
		 * Print any indirection needed (subfields or subscripts), and strip
		 * off the top-level nodes representing the indirection assignments.
		 * Add the stripped expressions to strippedexprs.  (If it's a
		 * single-VALUES statement, the stripped expressions are the VALUES to
		 * print below.  Otherwise they're just Vars and not really
		 * interesting.)
		 */
		strippedexprs = lappend(strippedexprs,
								processIndirection((Node *) tle->expr,
												   context));
	}
	if (query->targetList)
		appendStringInfoString(buf, ") ");

	if (select_rte)
	{
		/* Add the SELECT */
		get_query_def(select_rte->subquery, buf, NIL, NULL,
					  context->prettyFlags, context->wrapColumn,
					  context->indentLevel);
	}
	else if (values_rte)
	{
		/* Add the multi-VALUES expression lists */
		get_values_def(values_rte->values_lists, context);
	}
	else if (strippedexprs)
	{
		/* Add the single-VALUES expression list */
		appendContextKeyword(context, "VALUES (",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
		get_rule_expr((Node *) strippedexprs, context, false);
		appendStringInfoChar(buf, ')');
	}
	else
	{
		/* No expressions, so it must be DEFAULT VALUES */
		appendStringInfoString(buf, "DEFAULT VALUES");
	}

	/* Add ON CONFLICT if present */
	if (query->onConflict)
	{
		OnConflictExpr *confl = query->onConflict;

		appendStringInfoString(buf, " ON CONFLICT");

		if (confl->arbiterElems)
		{
			/* Add the single-VALUES expression list */
			appendStringInfoChar(buf, '(');
			get_rule_expr((Node *) confl->arbiterElems, context, false);
			appendStringInfoChar(buf, ')');

			/* Add a WHERE clause (for partial indexes) if given */
			if (confl->arbiterWhere != NULL)
			{
				bool		save_varprefix;

				/*
				 * Force non-prefixing of Vars, since parser assumes that they
				 * belong to target relation.  WHERE clause does not use
				 * InferenceElem, so this is separately required.
				 */
				save_varprefix = context->varprefix;
				context->varprefix = false;

				appendContextKeyword(context, " WHERE ",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
				get_rule_expr(confl->arbiterWhere, context, false);

				context->varprefix = save_varprefix;
			}
		}
		else if (OidIsValid(confl->constraint))
		{
			char	   *constraint = get_constraint_name(confl->constraint);

			if (!constraint)
				elog(ERROR, "cache lookup failed for constraint %u",
					 confl->constraint);
			appendStringInfo(buf, " ON CONSTRAINT %s",
							 quote_identifier(constraint));
		}

		if (confl->action == ONCONFLICT_NOTHING)
		{
			appendStringInfoString(buf, " DO NOTHING");
		}
		else
		{
			appendStringInfoString(buf, " DO UPDATE SET ");
			/* Deparse targetlist */
			get_update_query_targetlist_def(query, confl->onConflictSet,
											context, rte);

			/* Add a WHERE clause if given */
			if (confl->onConflictWhere != NULL)
			{
				appendContextKeyword(context, " WHERE ",
									 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
				get_rule_expr(confl->onConflictWhere, context, false);
			}
		}
	}

	/* Add RETURNING if present */
	if (query->returningList)
	{
		appendContextKeyword(context, " RETURNING",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_target_list(query->returningList, context, NULL);
	}
}


/* ----------
 * get_update_query_def			- Parse back an UPDATE parsetree
 * ----------
 */
static void
get_update_query_def(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *rte;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/*
	 * Start the query with UPDATE relname SET
	 */
	rte = rt_fetch(query->resultRelation, query->rtable);
	Assert(rte->rtekind == RTE_RELATION);
	if (PRETTY_INDENT(context))
	{
		appendStringInfoChar(buf, ' ');
		context->indentLevel += PRETTYINDENT_STD;
	}
	appendStringInfo(buf, "UPDATE %s%s",
					 only_marker(rte),
					 generate_relation_name(rte->relid, NIL));
	if (rte->alias != NULL)
		appendStringInfo(buf, " %s",
						 quote_identifier(rte->alias->aliasname));
	appendStringInfoString(buf, " SET ");

	/* Deparse targetlist */
	get_update_query_targetlist_def(query, query->targetList, context, rte);

	/* Add the FROM clause if needed */
	get_from_clause(query, " FROM ", context);

	/* Add a WHERE clause if given */
	if (query->jointree->quals != NULL)
	{
		appendContextKeyword(context, " WHERE ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_rule_expr(query->jointree->quals, context, false);
	}

	/* Add RETURNING if present */
	if (query->returningList)
	{
		appendContextKeyword(context, " RETURNING",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_target_list(query->returningList, context, NULL);
	}
}


/* ----------
 * get_update_query_targetlist_def			- Parse back an UPDATE targetlist
 * ----------
 */
static void
get_update_query_targetlist_def(Query *query, List *targetList,
								deparse_context *context, RangeTblEntry *rte)
{
	StringInfo	buf = context->buf;
	ListCell   *l;
	ListCell   *next_ma_cell;
	int			remaining_ma_columns;
	const char *sep;
	SubLink    *cur_ma_sublink;
	List	   *ma_sublinks;

	/*
	 * Prepare to deal with MULTIEXPR assignments: collect the source SubLinks
	 * into a list.  We expect them to appear, in ID order, in resjunk tlist
	 * entries.
	 */
	ma_sublinks = NIL;
	if (query->hasSubLinks)		/* else there can't be any */
	{
		foreach(l, targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(l);

			if (tle->resjunk && IsA(tle->expr, SubLink))
			{
				SubLink    *sl = (SubLink *) tle->expr;

				if (sl->subLinkType == MULTIEXPR_SUBLINK)
				{
					ma_sublinks = lappend(ma_sublinks, sl);
					Assert(sl->subLinkId == list_length(ma_sublinks));
				}
			}
		}
	}
	next_ma_cell = list_head(ma_sublinks);
	cur_ma_sublink = NULL;
	remaining_ma_columns = 0;

	/* Add the comma separated list of 'attname = value' */
	sep = "";
	foreach(l, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);
		Node	   *expr;

		if (tle->resjunk)
			continue;			/* ignore junk entries */

		/* Emit separator (OK whether we're in multiassignment or not) */
		appendStringInfoString(buf, sep);
		sep = ", ";

		/*
		 * Check to see if we're starting a multiassignment group: if so,
		 * output a left paren.
		 */
		if (next_ma_cell != NULL && cur_ma_sublink == NULL)
		{
			/*
			 * We must dig down into the expr to see if it's a PARAM_MULTIEXPR
			 * Param.  That could be buried under FieldStores and ArrayRefs
			 * (cf processIndirection()), and underneath those there could be
			 * an implicit type coercion.
			 */
			expr = (Node *) tle->expr;
			while (expr)
			{
				if (IsA(expr, FieldStore))
				{
					FieldStore *fstore = (FieldStore *) expr;

					expr = (Node *) linitial(fstore->newvals);
				}
				else if (IsA(expr, ArrayRef))
				{
					ArrayRef   *aref = (ArrayRef *) expr;

					if (aref->refassgnexpr == NULL)
						break;
					expr = (Node *) aref->refassgnexpr;
				}
				else
					break;
			}
			expr = strip_implicit_coercions(expr);

			if (expr && IsA(expr, Param) &&
				((Param *) expr)->paramkind == PARAM_MULTIEXPR)
			{
				cur_ma_sublink = (SubLink *) lfirst(next_ma_cell);
				next_ma_cell = lnext(next_ma_cell);
				remaining_ma_columns = count_nonjunk_tlist_entries(
						  ((Query *) cur_ma_sublink->subselect)->targetList);
				Assert(((Param *) expr)->paramid ==
					   ((cur_ma_sublink->subLinkId << 16) | 1));
				appendStringInfoChar(buf, '(');
			}
		}

		/*
		 * Put out name of target column; look in the catalogs, not at
		 * tle->resname, since resname will fail to track RENAME.
		 */
		appendStringInfoString(buf,
						quote_identifier(get_relid_attribute_name(rte->relid,
															   tle->resno)));

		/*
		 * Print any indirection needed (subfields or subscripts), and strip
		 * off the top-level nodes representing the indirection assignments.
		 */
		expr = processIndirection((Node *) tle->expr, context);

		/*
		 * If we're in a multiassignment, skip printing anything more, unless
		 * this is the last column; in which case, what we print should be the
		 * sublink, not the Param.
		 */
		if (cur_ma_sublink != NULL)
		{
			if (--remaining_ma_columns > 0)
				continue;		/* not the last column of multiassignment */
			appendStringInfoChar(buf, ')');
			expr = (Node *) cur_ma_sublink;
			cur_ma_sublink = NULL;
		}

		appendStringInfoString(buf, " = ");

		get_rule_expr(expr, context, false);
	}
}


/* ----------
 * get_delete_query_def			- Parse back a DELETE parsetree
 * ----------
 */
static void
get_delete_query_def(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *rte;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/*
	 * Start the query with DELETE FROM relname
	 */
	rte = rt_fetch(query->resultRelation, query->rtable);
	Assert(rte->rtekind == RTE_RELATION);
	if (PRETTY_INDENT(context))
	{
		appendStringInfoChar(buf, ' ');
		context->indentLevel += PRETTYINDENT_STD;
	}
	appendStringInfo(buf, "DELETE FROM %s%s",
					 only_marker(rte),
					 generate_relation_name(rte->relid, NIL));
	if (rte->alias != NULL)
		appendStringInfo(buf, " %s",
						 quote_identifier(rte->alias->aliasname));

	/* Add the USING clause if given */
	get_from_clause(query, " USING ", context);

	/* Add a WHERE clause if given */
	if (query->jointree->quals != NULL)
	{
		appendContextKeyword(context, " WHERE ",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_rule_expr(query->jointree->quals, context, false);
	}

	/* Add RETURNING if present */
	if (query->returningList)
	{
		appendContextKeyword(context, " RETURNING",
							 -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
		get_target_list(query->returningList, context, NULL);
	}
}


/* ----------
 * get_utility_query_def			- Parse back a UTILITY parsetree
 * ----------
 */
static void
get_utility_query_def(Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;

	if (query->utilityStmt && IsA(query->utilityStmt, NotifyStmt))
	{
		NotifyStmt *stmt = (NotifyStmt *) query->utilityStmt;

		appendContextKeyword(context, "",
							 0, PRETTYINDENT_STD, 1);
		appendStringInfo(buf, "NOTIFY %s",
						 quote_identifier(stmt->conditionname));
		if (stmt->payload)
		{
			appendStringInfoString(buf, ", ");
			simple_quote_literal(buf, stmt->payload);
		}
	}
	else
	{
		/* Currently only NOTIFY utility commands can appear in rules */
		elog(ERROR, "unexpected utility statement type");
	}
}

/*
 * Display a Var appropriately.
 *
 * In some cases (currently only when recursing into an unnamed join)
 * the Var's varlevelsup has to be interpreted with respect to a context
 * above the current one; levelsup indicates the offset.
 *
 * If istoplevel is TRUE, the Var is at the top level of a SELECT's
 * targetlist, which means we need special treatment of whole-row Vars.
 * Instead of the normal "tab.*", we'll print "tab.*::typename", which is a
 * dirty hack to prevent "tab.*" from being expanded into multiple columns.
 * (The parser will strip the useless coercion, so no inefficiency is added in
 * dump and reload.)  We used to print just "tab" in such cases, but that is
 * ambiguous and will yield the wrong result if "tab" is also a plain column
 * name in the query.
 *
 * Returns the attname of the Var, or NULL if the Var has no attname (because
 * it is a whole-row Var or a subplan output reference).
 */
static char *
get_variable(Var *var, int levelsup, bool istoplevel, deparse_context *context)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *rte;
	AttrNumber	attnum;
	int			netlevelsup;
	deparse_namespace *dpns;
	deparse_columns *colinfo;
	char	   *refname;
	char	   *attname;

	/* Find appropriate nesting depth */
	netlevelsup = var->varlevelsup + levelsup;
	if (netlevelsup >= list_length(context->namespaces))
		elog(ERROR, "bogus varlevelsup: %d offset %d",
			 var->varlevelsup, levelsup);
	dpns = (deparse_namespace *) list_nth(context->namespaces,
										  netlevelsup);

	/*
	 * Try to find the relevant RTE in this rtable.  In a plan tree, it's
	 * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
	 * down into the subplans, or INDEX_VAR, which is resolved similarly. Also
	 * find the aliases previously assigned for this RTE.
	 */
	if (var->varno >= 1 && var->varno <= list_length(dpns->rtable))
	{
		rte = rt_fetch(var->varno, dpns->rtable);
		refname = (char *) list_nth(dpns->rtable_names, var->varno - 1);
		colinfo = deparse_columns_fetch(var->varno, dpns);
		attnum = var->varattno;
	}
	else
	{
		resolve_special_varno((Node *) var, context, NULL,
							  get_special_variable);
		return NULL;
	}

	/*
	 * The planner will sometimes emit Vars referencing resjunk elements of a
	 * subquery's target list (this is currently only possible if it chooses
	 * to generate a "physical tlist" for a SubqueryScan or CteScan node).
	 * Although we prefer to print subquery-referencing Vars using the
	 * subquery's alias, that's not possible for resjunk items since they have
	 * no alias.  So in that case, drill down to the subplan and print the
	 * contents of the referenced tlist item.  This works because in a plan
	 * tree, such Vars can only occur in a SubqueryScan or CteScan node, and
	 * we'll have set dpns->inner_planstate to reference the child plan node.
	 */
	if ((rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) &&
		attnum > list_length(rte->eref->colnames) &&
		dpns->inner_planstate)
	{
		TargetEntry *tle;
		deparse_namespace save_dpns;

		tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "invalid attnum %d for relation \"%s\"",
				 var->varattno, rte->eref->aliasname);

		Assert(netlevelsup == 0);
		push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

		/*
		 * Force parentheses because our caller probably assumed a Var is a
		 * simple expression.
		 */
		if (!IsA(tle->expr, Var))
			appendStringInfoChar(buf, '(');
		get_rule_expr((Node *) tle->expr, context, true);
		if (!IsA(tle->expr, Var))
			appendStringInfoChar(buf, ')');

		pop_child_plan(dpns, &save_dpns);
		return NULL;
	}

	/*
	 * If it's an unnamed join, look at the expansion of the alias variable.
	 * If it's a simple reference to one of the input vars, then recursively
	 * print the name of that var instead.  When it's not a simple reference,
	 * we have to just print the unqualified join column name.  (This can only
	 * happen with "dangerous" merged columns in a JOIN USING; we took pains
	 * previously to make the unqualified column name unique in such cases.)
	 *
	 * This wouldn't work in decompiling plan trees, because we don't store
	 * joinaliasvars lists after planning; but a plan tree should never
	 * contain a join alias variable.
	 */
	if (rte->rtekind == RTE_JOIN && rte->alias == NULL)
	{
		if (rte->joinaliasvars == NIL)
			elog(ERROR, "cannot decompile join alias var in plan tree");
		if (attnum > 0)
		{
			Var		   *aliasvar;

			aliasvar = (Var *) list_nth(rte->joinaliasvars, attnum - 1);
			/* we intentionally don't strip implicit coercions here */
			if (aliasvar && IsA(aliasvar, Var))
			{
				return get_variable(aliasvar, var->varlevelsup + levelsup,
									istoplevel, context);
			}
		}

		/*
		 * Unnamed join has no refname.  (Note: since it's unnamed, there is
		 * no way the user could have referenced it to create a whole-row Var
		 * for it.  So we don't have to cover that case below.)
		 */
		Assert(refname == NULL);
	}

	if (attnum == InvalidAttrNumber)
		attname = NULL;
	else if (attnum > 0)
	{
		/* Get column name to use from the colinfo struct */
		if (attnum > colinfo->num_cols)
			elog(ERROR, "invalid attnum %d for relation \"%s\"",
				 attnum, rte->eref->aliasname);
		attname = colinfo->colnames[attnum - 1];
		if (attname == NULL)	/* dropped column? */
			elog(ERROR, "invalid attnum %d for relation \"%s\"",
				 attnum, rte->eref->aliasname);
	}
	else
	{
		/* System column - name is fixed, get it from the catalog */
		attname = get_rte_attribute_name(rte, attnum);
	}

	if (refname && (context->varprefix || attname == NULL))
	{
		appendStringInfoString(buf, quote_identifier(refname));
		appendStringInfoChar(buf, '.');
	}
	if (attname)
		appendStringInfoString(buf, quote_identifier(attname));
	else
	{
		appendStringInfoChar(buf, '*');
		if (istoplevel)
			appendStringInfo(buf, "::%s",
							 format_type_with_typemod(var->vartype,
													  var->vartypmod));
	}

	return attname;
}

/*
 * Deparse a Var which references OUTER_VAR, INNER_VAR, or INDEX_VAR.  This
 * routine is actually a callback for get_special_varno, which handles finding
 * the correct TargetEntry.  We get the expression contained in that
 * TargetEntry and just need to deparse it, a job we can throw back on
 * get_rule_expr.
 */
static void
get_special_variable(Node *node, deparse_context *context, void *private)
{
	StringInfo	buf = context->buf;

	/*
	 * Force parentheses because our caller probably assumed a Var is a simple
	 * expression.
	 */
	if (!IsA(node, Var))
		appendStringInfoChar(buf, '(');
	get_rule_expr(node, context, true);
	if (!IsA(node, Var))
		appendStringInfoChar(buf, ')');
}

/*
 * Chase through plan references to special varnos (OUTER_VAR, INNER_VAR,
 * INDEX_VAR) until we find a real Var or some kind of non-Var node; then,
 * invoke the callback provided.
 */
static void
resolve_special_varno(Node *node, deparse_context *context, void *private,
					  void (*callback) (Node *, deparse_context *, void *))
{
	Var		   *var;
	deparse_namespace *dpns;

	/* If it's not a Var, invoke the callback. */
	if (!IsA(node, Var))
	{
		callback(node, context, private);
		return;
	}

	/* Find appropriate nesting depth */
	var = (Var *) node;
	dpns = (deparse_namespace *) list_nth(context->namespaces,
										  var->varlevelsup);

	/*
	 * It's a special RTE, so recurse.
	 */
	if (var->varno == OUTER_VAR && dpns->outer_tlist)
	{
		TargetEntry *tle;
		deparse_namespace save_dpns;

		tle = get_tle_by_resno(dpns->outer_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for OUTER_VAR var: %d", var->varattno);

		push_child_plan(dpns, dpns->outer_planstate, &save_dpns);
		resolve_special_varno((Node *) tle->expr, context, private, callback);
		pop_child_plan(dpns, &save_dpns);
		return;
	}
	else if (var->varno == INNER_VAR && dpns->inner_tlist)
	{
		TargetEntry *tle;
		deparse_namespace save_dpns;

		tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for INNER_VAR var: %d", var->varattno);

		push_child_plan(dpns, dpns->inner_planstate, &save_dpns);
		resolve_special_varno((Node *) tle->expr, context, private, callback);
		pop_child_plan(dpns, &save_dpns);
		return;
	}
	else if (var->varno == INDEX_VAR && dpns->index_tlist)
	{
		TargetEntry *tle;

		tle = get_tle_by_resno(dpns->index_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for INDEX_VAR var: %d", var->varattno);

		resolve_special_varno((Node *) tle->expr, context, private, callback);
		return;
	}
	else if (var->varno < 1 || var->varno > list_length(dpns->rtable))
		elog(ERROR, "bogus varno: %d", var->varno);

	/* Not special.  Just invoke the callback. */
	callback(node, context, private);
}

/*
 * Get the name of a field of an expression of composite type.  The
 * expression is usually a Var, but we handle other cases too.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 *
 * This is fairly straightforward when the expression has a named composite
 * type; we need only look up the type in the catalogs.  However, the type
 * could also be RECORD.  Since no actual table or view column is allowed to
 * have type RECORD, a Var of type RECORD must refer to a JOIN or FUNCTION RTE
 * or to a subquery output.  We drill down to find the ultimate defining
 * expression and attempt to infer the field name from it.  We ereport if we
 * can't determine the name.
 *
 * Similarly, a PARAM of type RECORD has to refer to some expression of
 * a determinable composite type.
 */
static const char *
get_name_for_var_field(Var *var, int fieldno,
					   int levelsup, deparse_context *context)
{
	RangeTblEntry *rte;
	AttrNumber	attnum;
	int			netlevelsup;
	deparse_namespace *dpns;
	TupleDesc	tupleDesc;
	Node	   *expr;

	/*
	 * If it's a RowExpr that was expanded from a whole-row Var, use the
	 * column names attached to it.
	 */
	if (IsA(var, RowExpr))
	{
		RowExpr    *r = (RowExpr *) var;

		if (fieldno > 0 && fieldno <= list_length(r->colnames))
			return strVal(list_nth(r->colnames, fieldno - 1));
	}

	/*
	 * If it's a Param of type RECORD, try to find what the Param refers to.
	 */
	if (IsA(var, Param))
	{
		Param	   *param = (Param *) var;
		ListCell   *ancestor_cell;

		expr = find_param_referent(param, context, &dpns, &ancestor_cell);
		if (expr)
		{
			/* Found a match, so recurse to decipher the field name */
			deparse_namespace save_dpns;
			const char *result;

			push_ancestor_plan(dpns, ancestor_cell, &save_dpns);
			result = get_name_for_var_field((Var *) expr, fieldno,
											0, context);
			pop_ancestor_plan(dpns, &save_dpns);
			return result;
		}
	}

	/*
	 * If it's a Var of type RECORD, we have to find what the Var refers to;
	 * if not, we can use get_expr_result_type. If that fails, we try
	 * lookup_rowtype_tupdesc, which will probably fail too, but will ereport
	 * an acceptable message.
	 */
	if (!IsA(var, Var) ||
		var->vartype != RECORDOID)
	{
		if (get_expr_result_type((Node *) var, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			tupleDesc = lookup_rowtype_tupdesc_copy(exprType((Node *) var),
													exprTypmod((Node *) var));
		Assert(tupleDesc);
		/* Got the tupdesc, so we can extract the field name */
		Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
		return NameStr(tupleDesc->attrs[fieldno - 1]->attname);
	}

	/* Find appropriate nesting depth */
	netlevelsup = var->varlevelsup + levelsup;
	if (netlevelsup >= list_length(context->namespaces))
		elog(ERROR, "bogus varlevelsup: %d offset %d",
			 var->varlevelsup, levelsup);
	dpns = (deparse_namespace *) list_nth(context->namespaces,
										  netlevelsup);

	/*
	 * Try to find the relevant RTE in this rtable.  In a plan tree, it's
	 * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
	 * down into the subplans, or INDEX_VAR, which is resolved similarly.
	 */
	if (var->varno >= 1 && var->varno <= list_length(dpns->rtable))
	{
		rte = rt_fetch(var->varno, dpns->rtable);
		attnum = var->varattno;
	}
	else if (var->varno == OUTER_VAR && dpns->outer_tlist)
	{
		TargetEntry *tle;
		deparse_namespace save_dpns;
		const char *result;

		tle = get_tle_by_resno(dpns->outer_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for OUTER_VAR var: %d", var->varattno);

		Assert(netlevelsup == 0);
		push_child_plan(dpns, dpns->outer_planstate, &save_dpns);

		result = get_name_for_var_field((Var *) tle->expr, fieldno,
										levelsup, context);

		pop_child_plan(dpns, &save_dpns);
		return result;
	}
	else if (var->varno == INNER_VAR && dpns->inner_tlist)
	{
		TargetEntry *tle;
		deparse_namespace save_dpns;
		const char *result;

		tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for INNER_VAR var: %d", var->varattno);

		Assert(netlevelsup == 0);
		push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

		result = get_name_for_var_field((Var *) tle->expr, fieldno,
										levelsup, context);

		pop_child_plan(dpns, &save_dpns);
		return result;
	}
	else if (var->varno == INDEX_VAR && dpns->index_tlist)
	{
		TargetEntry *tle;
		const char *result;

		tle = get_tle_by_resno(dpns->index_tlist, var->varattno);
		if (!tle)
			elog(ERROR, "bogus varattno for INDEX_VAR var: %d", var->varattno);

		Assert(netlevelsup == 0);

		result = get_name_for_var_field((Var *) tle->expr, fieldno,
										levelsup, context);

		return result;
	}
	else
	{
		elog(ERROR, "bogus varno: %d", var->varno);
		return NULL;			/* keep compiler quiet */
	}

	if (attnum == InvalidAttrNumber)
	{
		/* Var is whole-row reference to RTE, so select the right field */
		return get_rte_attribute_name(rte, fieldno);
	}

	/*
	 * This part has essentially the same logic as the parser's
	 * expandRecordVariable() function, but we are dealing with a different
	 * representation of the input context, and we only need one field name
	 * not a TupleDesc.  Also, we need special cases for finding subquery and
	 * CTE subplans when deparsing Plan trees.
	 */
	expr = (Node *) var;		/* default if we can't drill down */

	switch (rte->rtekind)
	{
		case RTE_RELATION:
		case RTE_VALUES:

			/*
			 * This case should not occur: a column of a table or values list
			 * shouldn't have type RECORD.  Fall through and fail (most
			 * likely) at the bottom.
			 */
			break;
		case RTE_SUBQUERY:
			/* Subselect-in-FROM: examine sub-select's output expr */
			{
				if (rte->subquery)
				{
					TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList,
														attnum);

					if (ste == NULL || ste->resjunk)
						elog(ERROR, "subquery %s does not have attribute %d",
							 rte->eref->aliasname, attnum);
					expr = (Node *) ste->expr;
					if (IsA(expr, Var))
					{
						/*
						 * Recurse into the sub-select to see what its Var
						 * refers to. We have to build an additional level of
						 * namespace to keep in step with varlevelsup in the
						 * subselect.
						 */
						deparse_namespace mydpns;
						const char *result;

						set_deparse_for_query(&mydpns, rte->subquery,
											  context->namespaces);

						context->namespaces = lcons(&mydpns,
													context->namespaces);

						result = get_name_for_var_field((Var *) expr, fieldno,
														0, context);

						context->namespaces =
							list_delete_first(context->namespaces);

						return result;
					}
					/* else fall through to inspect the expression */
				}
				else
				{
					/*
					 * We're deparsing a Plan tree so we don't have complete
					 * RTE entries (in particular, rte->subquery is NULL). But
					 * the only place we'd see a Var directly referencing a
					 * SUBQUERY RTE is in a SubqueryScan plan node, and we can
					 * look into the child plan's tlist instead.
					 */
					TargetEntry *tle;
					deparse_namespace save_dpns;
					const char *result;

					if (!dpns->inner_planstate)
						elog(ERROR, "failed to find plan for subquery %s",
							 rte->eref->aliasname);
					tle = get_tle_by_resno(dpns->inner_tlist, attnum);
					if (!tle)
						elog(ERROR, "bogus varattno for subquery var: %d",
							 attnum);
					Assert(netlevelsup == 0);
					push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

					result = get_name_for_var_field((Var *) tle->expr, fieldno,
													levelsup, context);

					pop_child_plan(dpns, &save_dpns);
					return result;
				}
			}
			break;
		case RTE_JOIN:
			/* Join RTE --- recursively inspect the alias variable */
			if (rte->joinaliasvars == NIL)
				elog(ERROR, "cannot decompile join alias var in plan tree");
			Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
			expr = (Node *) list_nth(rte->joinaliasvars, attnum - 1);
			Assert(expr != NULL);
			/* we intentionally don't strip implicit coercions here */
			if (IsA(expr, Var))
				return get_name_for_var_field((Var *) expr, fieldno,
											  var->varlevelsup + levelsup,
											  context);
			/* else fall through to inspect the expression */
			break;
		case RTE_FUNCTION:

			/*
			 * We couldn't get here unless a function is declared with one of
			 * its result columns as RECORD, which is not allowed.
			 */
			break;
		case RTE_CTE:
			/* CTE reference: examine subquery's output expr */
			{
				CommonTableExpr *cte = NULL;
				Index		ctelevelsup;
				ListCell   *lc;

				/*
				 * Try to find the referenced CTE using the namespace stack.
				 */
				ctelevelsup = rte->ctelevelsup + netlevelsup;
				if (ctelevelsup >= list_length(context->namespaces))
					lc = NULL;
				else
				{
					deparse_namespace *ctedpns;

					ctedpns = (deparse_namespace *)
						list_nth(context->namespaces, ctelevelsup);
					foreach(lc, ctedpns->ctes)
					{
						cte = (CommonTableExpr *) lfirst(lc);
						if (strcmp(cte->ctename, rte->ctename) == 0)
							break;
					}
				}
				if (lc != NULL)
				{
					Query	   *ctequery = (Query *) cte->ctequery;
					TargetEntry *ste = get_tle_by_resno(GetCTETargetList(cte),
														attnum);

					if (ste == NULL || ste->resjunk)
						elog(ERROR, "subquery %s does not have attribute %d",
							 rte->eref->aliasname, attnum);
					expr = (Node *) ste->expr;
					if (IsA(expr, Var))
					{
						/*
						 * Recurse into the CTE to see what its Var refers to.
						 * We have to build an additional level of namespace
						 * to keep in step with varlevelsup in the CTE.
						 * Furthermore it could be an outer CTE, so we may
						 * have to delete some levels of namespace.
						 */
						List	   *save_nslist = context->namespaces;
						List	   *new_nslist;
						deparse_namespace mydpns;
						const char *result;

						set_deparse_for_query(&mydpns, ctequery,
											  context->namespaces);

						new_nslist = list_copy_tail(context->namespaces,
													ctelevelsup);
						context->namespaces = lcons(&mydpns, new_nslist);

						result = get_name_for_var_field((Var *) expr, fieldno,
														0, context);

						context->namespaces = save_nslist;

						return result;
					}
					/* else fall through to inspect the expression */
				}
				else
				{
					/*
					 * We're deparsing a Plan tree so we don't have a CTE
					 * list.  But the only place we'd see a Var directly
					 * referencing a CTE RTE is in a CteScan plan node, and we
					 * can look into the subplan's tlist instead.
					 */
					TargetEntry *tle;
					deparse_namespace save_dpns;
					const char *result;

					if (!dpns->inner_planstate)
						elog(ERROR, "failed to find plan for CTE %s",
							 rte->eref->aliasname);
					tle = get_tle_by_resno(dpns->inner_tlist, attnum);
					if (!tle)
						elog(ERROR, "bogus varattno for subquery var: %d",
							 attnum);
					Assert(netlevelsup == 0);
					push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

					result = get_name_for_var_field((Var *) tle->expr, fieldno,
													levelsup, context);

					pop_child_plan(dpns, &save_dpns);
					return result;
				}
			}
			break;
	}

	/*
	 * We now have an expression we can't expand any more, so see if
	 * get_expr_result_type() can do anything with it.  If not, pass to
	 * lookup_rowtype_tupdesc() which will probably fail, but will give an
	 * appropriate error message while failing.
	 */
	if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr),
												exprTypmod(expr));
	Assert(tupleDesc);
	/* Got the tupdesc, so we can extract the field name */
	Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
	return NameStr(tupleDesc->attrs[fieldno - 1]->attname);
}

/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpns_p and *ancestor_cell_p
 * appropriately for calling push_ancestor_plan().  If no referent can be
 * found, return NULL.
 */
static Node *
find_param_referent(Param *param, deparse_context *context,
					deparse_namespace **dpns_p, ListCell **ancestor_cell_p)
{
	/* Initialize output parameters to prevent compiler warnings */
	*dpns_p = NULL;
	*ancestor_cell_p = NULL;

	/*
	 * If it's a PARAM_EXEC parameter, look for a matching NestLoopParam or
	 * SubPlan argument.  This will necessarily be in some ancestor of the
	 * current expression's PlanState.
	 */
	if (param->paramkind == PARAM_EXEC)
	{
		deparse_namespace *dpns;
		PlanState  *child_ps;
		bool		in_same_plan_level;
		ListCell   *lc;

		dpns = (deparse_namespace *) linitial(context->namespaces);
		child_ps = dpns->planstate;
		in_same_plan_level = true;

		foreach(lc, dpns->ancestors)
		{
			PlanState  *ps = (PlanState *) lfirst(lc);
			ListCell   *lc2;

			/*
			 * NestLoops transmit params to their inner child only; also, once
			 * we've crawled up out of a subplan, this couldn't possibly be
			 * the right match.
			 */
			if (IsA(ps, NestLoopState) &&
				child_ps == innerPlanState(ps) &&
				in_same_plan_level)
			{
				NestLoop   *nl = (NestLoop *) ps->plan;

				foreach(lc2, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc2);

					if (nlp->paramno == param->paramid)
					{
						/* Found a match, so return it */
						*dpns_p = dpns;
						*ancestor_cell_p = lc;
						return (Node *) nlp->paramval;
					}
				}
			}

			/*
			 * Check to see if we're crawling up from a subplan.
			 */
			foreach(lc2, ps->subPlan)
			{
				SubPlanState *sstate = (SubPlanState *) lfirst(lc2);
				SubPlan    *subplan = (SubPlan *) sstate->xprstate.expr;
				ListCell   *lc3;
				ListCell   *lc4;

				if (child_ps != sstate->planstate)
					continue;

				/* Matched subplan, so check its arguments */
				forboth(lc3, subplan->parParam, lc4, subplan->args)
				{
					int			paramid = lfirst_int(lc3);
					Node	   *arg = (Node *) lfirst(lc4);

					if (paramid == param->paramid)
					{
						/* Found a match, so return it */
						*dpns_p = dpns;
						*ancestor_cell_p = lc;
						return arg;
					}
				}

				/* Keep looking, but we are emerging from a subplan. */
				in_same_plan_level = false;
				break;
			}

			/*
			 * Likewise check to see if we're emerging from an initplan.
			 * Initplans never have any parParams, so no need to search that
			 * list, but we need to know if we should reset
			 * in_same_plan_level.
			 */
			foreach(lc2, ps->initPlan)
			{
				SubPlanState *sstate = (SubPlanState *) lfirst(lc2);

				if (child_ps != sstate->planstate)
					continue;

				/* No parameters to be had here. */
				Assert(((SubPlan *) sstate->xprstate.expr)->parParam == NIL);

				/* Keep looking, but we are emerging from an initplan. */
				in_same_plan_level = false;
				break;
			}

			/* No luck, crawl up to next ancestor */
			child_ps = ps;
		}
	}

	/* No referent found */
	return NULL;
}

/*
 * Display a Param appropriately.
 */
static void
get_parameter(Param *param, deparse_context *context)
{
	Node	   *expr;
	deparse_namespace *dpns;
	ListCell   *ancestor_cell;

	/*
	 * If it's a PARAM_EXEC parameter, try to locate the expression from which
	 * the parameter was computed.  Note that failing to find a referent isn't
	 * an error, since the Param might well be a subplan output rather than an
	 * input.
	 */
	expr = find_param_referent(param, context, &dpns, &ancestor_cell);
	if (expr)
	{
		/* Found a match, so print it */
		deparse_namespace save_dpns;
		bool		save_varprefix;
		bool		need_paren;

		/* Switch attention to the ancestor plan node */
		push_ancestor_plan(dpns, ancestor_cell, &save_dpns);

		/*
		 * Force prefixing of Vars, since they won't belong to the relation
		 * being scanned in the original plan node.
		 */
		save_varprefix = context->varprefix;
		context->varprefix = true;

		/*
		 * A Param's expansion is typically a Var, Aggref, or upper-level
		 * Param, which wouldn't need extra parentheses.  Otherwise, insert
		 * parens to ensure the expression looks atomic.
		 */
		need_paren = !(IsA(expr, Var) ||
					   IsA(expr, Aggref) ||
					   IsA(expr, Param));
		if (need_paren)
			appendStringInfoChar(context->buf, '(');

		get_rule_expr(expr, context, false);

		if (need_paren)
			appendStringInfoChar(context->buf, ')');

		context->varprefix = save_varprefix;

		pop_ancestor_plan(dpns, &save_dpns);

		return;
	}

	/*
	 * Not PARAM_EXEC, or couldn't find referent: just print $N.
	 */
	appendStringInfo(context->buf, "$%d", param->paramid);
}

/*
 * get_simple_binary_op_name
 *
 * helper function for isSimpleNode
 * will return single char binary operator name, or NULL if it's not
 */
static const char *
get_simple_binary_op_name(OpExpr *expr)
{
	List	   *args = expr->args;

	if (list_length(args) == 2)
	{
		/* binary operator */
		Node	   *arg1 = (Node *) linitial(args);
		Node	   *arg2 = (Node *) lsecond(args);
		const char *op;

		op = generate_operator_name(expr->opno, exprType(arg1), exprType(arg2));
		if (strlen(op) == 1)
			return op;
	}
	return NULL;
}


/*
 * isSimpleNode - check if given node is simple (doesn't need parenthesizing)
 *
 *	true   : simple in the context of parent node's type
 *	false  : not simple
 */
static bool
isSimpleNode(Node *node, Node *parentNode, int prettyFlags)
{
	if (!node)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_CurrentOfExpr:
			/* single words: always simple */
			return true;

		case T_ArrayRef:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_XmlExpr:
		case T_NullIfExpr:
		case T_Aggref:
		case T_WindowFunc:
		case T_FuncExpr:
			/* function-like: name(..) or name[..] */
			return true;

			/* CASE keywords act as parentheses */
		case T_CaseExpr:
			return true;

		case T_FieldSelect:

			/*
			 * appears simple since . has top precedence, unless parent is
			 * T_FieldSelect itself!
			 */
			return (IsA(parentNode, FieldSelect) ? false : true);

		case T_FieldStore:

			/*
			 * treat like FieldSelect (probably doesn't matter)
			 */
			return (IsA(parentNode, FieldStore) ? false : true);

		case T_CoerceToDomain:
			/* maybe simple, check args */
			return isSimpleNode((Node *) ((CoerceToDomain *) node)->arg,
								node, prettyFlags);
		case T_RelabelType:
			return isSimpleNode((Node *) ((RelabelType *) node)->arg,
								node, prettyFlags);
		case T_CoerceViaIO:
			return isSimpleNode((Node *) ((CoerceViaIO *) node)->arg,
								node, prettyFlags);
		case T_ArrayCoerceExpr:
			return isSimpleNode((Node *) ((ArrayCoerceExpr *) node)->arg,
								node, prettyFlags);
		case T_ConvertRowtypeExpr:
			return isSimpleNode((Node *) ((ConvertRowtypeExpr *) node)->arg,
								node, prettyFlags);

		case T_OpExpr:
			{
				/* depends on parent node type; needs further checking */
				if (prettyFlags & PRETTYFLAG_PAREN && IsA(parentNode, OpExpr))
				{
					const char *op;
					const char *parentOp;
					bool		is_lopriop;
					bool		is_hipriop;
					bool		is_lopriparent;
					bool		is_hipriparent;

					op = get_simple_binary_op_name((OpExpr *) node);
					if (!op)
						return false;

					/* We know only the basic operators + - and * / % */
					is_lopriop = (strchr("+-", *op) != NULL);
					is_hipriop = (strchr("*/%", *op) != NULL);
					if (!(is_lopriop || is_hipriop))
						return false;

					parentOp = get_simple_binary_op_name((OpExpr *) parentNode);
					if (!parentOp)
						return false;

					is_lopriparent = (strchr("+-", *parentOp) != NULL);
					is_hipriparent = (strchr("*/%", *parentOp) != NULL);
					if (!(is_lopriparent || is_hipriparent))
						return false;

					if (is_hipriop && is_lopriparent)
						return true;	/* op binds tighter than parent */

					if (is_lopriop && is_hipriparent)
						return false;

					/*
					 * Operators are same priority --- can skip parens only if
					 * we have (a - b) - c, not a - (b - c).
					 */
					if (node == (Node *) linitial(((OpExpr *) parentNode)->args))
						return true;

					return false;
				}
				/* else do the same stuff as for T_SubLink et al. */
				/* FALL THROUGH */
			}

		case T_SubLink:
		case T_NullTest:
		case T_BooleanTest:
		case T_DistinctExpr:
			switch (nodeTag(parentNode))
			{
				case T_FuncExpr:
					{
						/* special handling for casts */
						CoercionForm type = ((FuncExpr *) parentNode)->funcformat;

						if (type == COERCE_EXPLICIT_CAST ||
							type == COERCE_IMPLICIT_CAST)
							return false;
						return true;	/* own parentheses */
					}
				case T_BoolExpr:		/* lower precedence */
				case T_ArrayRef:		/* other separators */
				case T_ArrayExpr:		/* other separators */
				case T_RowExpr:	/* other separators */
				case T_CoalesceExpr:	/* own parentheses */
				case T_MinMaxExpr:		/* own parentheses */
				case T_XmlExpr:	/* own parentheses */
				case T_NullIfExpr:		/* other separators */
				case T_Aggref:	/* own parentheses */
				case T_WindowFunc:		/* own parentheses */
				case T_CaseExpr:		/* other separators */
					return true;
				default:
					return false;
			}

		case T_BoolExpr:
			switch (nodeTag(parentNode))
			{
				case T_BoolExpr:
					if (prettyFlags & PRETTYFLAG_PAREN)
					{
						BoolExprType type;
						BoolExprType parentType;

						type = ((BoolExpr *) node)->boolop;
						parentType = ((BoolExpr *) parentNode)->boolop;
						switch (type)
						{
							case NOT_EXPR:
							case AND_EXPR:
								if (parentType == AND_EXPR || parentType == OR_EXPR)
									return true;
								break;
							case OR_EXPR:
								if (parentType == OR_EXPR)
									return true;
								break;
						}
					}
					return false;
				case T_FuncExpr:
					{
						/* special handling for casts */
						CoercionForm type = ((FuncExpr *) parentNode)->funcformat;

						if (type == COERCE_EXPLICIT_CAST ||
							type == COERCE_IMPLICIT_CAST)
							return false;
						return true;	/* own parentheses */
					}
				case T_ArrayRef:		/* other separators */
				case T_ArrayExpr:		/* other separators */
				case T_RowExpr:	/* other separators */
				case T_CoalesceExpr:	/* own parentheses */
				case T_MinMaxExpr:		/* own parentheses */
				case T_XmlExpr:	/* own parentheses */
				case T_NullIfExpr:		/* other separators */
				case T_Aggref:	/* own parentheses */
				case T_WindowFunc:		/* own parentheses */
				case T_CaseExpr:		/* other separators */
					return true;
				default:
					return false;
			}

		default:
			break;
	}
	/* those we don't know: in dubio complexo */
	return false;
}


/*
 * appendContextKeyword - append a keyword to buffer
 *
 * If prettyPrint is enabled, perform a line break, and adjust indentation.
 * Otherwise, just append the keyword.
 */
static void
appendContextKeyword(deparse_context *context, const char *str,
					 int indentBefore, int indentAfter, int indentPlus)
{
	StringInfo	buf = context->buf;

	if (PRETTY_INDENT(context))
	{
		int			indentAmount;

		context->indentLevel += indentBefore;

		/* remove any trailing spaces currently in the buffer ... */
		removeStringInfoSpaces(buf);
		/* ... then add a newline and some spaces */
		appendStringInfoChar(buf, '\n');

		if (context->indentLevel < PRETTYINDENT_LIMIT)
			indentAmount = Max(context->indentLevel, 0) + indentPlus;
		else
		{
			/*
			 * If we're indented more than PRETTYINDENT_LIMIT characters, try
			 * to conserve horizontal space by reducing the per-level
			 * indentation.  For best results the scale factor here should
			 * divide all the indent amounts that get added to indentLevel
			 * (PRETTYINDENT_STD, etc).  It's important that the indentation
			 * not grow unboundedly, else deeply-nested trees use O(N^2)
			 * whitespace; so we also wrap modulo PRETTYINDENT_LIMIT.
			 */
			indentAmount = PRETTYINDENT_LIMIT +
				(context->indentLevel - PRETTYINDENT_LIMIT) /
				(PRETTYINDENT_STD / 2);
			indentAmount %= PRETTYINDENT_LIMIT;
			/* scale/wrap logic affects indentLevel, but not indentPlus */
			indentAmount += indentPlus;
		}
		appendStringInfoSpaces(buf, indentAmount);

		appendStringInfoString(buf, str);

		context->indentLevel += indentAfter;
		if (context->indentLevel < 0)
			context->indentLevel = 0;
	}
	else
		appendStringInfoString(buf, str);
}

/*
 * removeStringInfoSpaces - delete trailing spaces from a buffer.
 *
 * Possibly this should move to stringinfo.c at some point.
 */
static void
removeStringInfoSpaces(StringInfo str)
{
	while (str->len > 0 && str->data[str->len - 1] == ' ')
		str->data[--(str->len)] = '\0';
}


/*
 * get_rule_expr_paren	- deparse expr using get_rule_expr,
 * embracing the string with parentheses if necessary for prettyPrint.
 *
 * Never embrace if prettyFlags=0, because it's done in the calling node.
 *
 * Any node that does *not* embrace its argument node by sql syntax (with
 * parentheses, non-operator keywords like CASE/WHEN/ON, or comma etc) should
 * use get_rule_expr_paren instead of get_rule_expr so parentheses can be
 * added.
 */
static void
get_rule_expr_paren(Node *node, deparse_context *context,
					bool showimplicit, Node *parentNode)
{
	bool		need_paren;

	need_paren = PRETTY_PAREN(context) &&
		!isSimpleNode(node, parentNode, context->prettyFlags);

	if (need_paren)
		appendStringInfoChar(context->buf, '(');

	get_rule_expr(node, context, showimplicit);

	if (need_paren)
		appendStringInfoChar(context->buf, ')');
}


/* ----------
 * get_rule_expr			- Parse back an expression
 *
 * Note: showimplicit determines whether we display any implicit cast that
 * is present at the top of the expression tree.  It is a passed argument,
 * not a field of the context struct, because we change the value as we
 * recurse down into the expression.  In general we suppress implicit casts
 * when the result type is known with certainty (eg, the arguments of an
 * OR must be boolean).  We display implicit casts for arguments of functions
 * and operators, since this is needed to be certain that the same function
 * or operator will be chosen when the expression is re-parsed.
 * ----------
 */
static void
get_rule_expr(Node *node, deparse_context *context,
			  bool showimplicit)
{
	StringInfo	buf = context->buf;

	if (node == NULL)
		return;

	/* Guard against excessively long or deeply-nested queries */
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	/*
	 * Each level of get_rule_expr must emit an indivisible term
	 * (parenthesized if necessary) to ensure result is reparsed into the same
	 * expression tree.  The only exception is that when the input is a List,
	 * we emit the component items comma-separated with no surrounding
	 * decoration; this is convenient for most callers.
	 */
	switch (nodeTag(node))
	{
		case T_Var:
			(void) get_variable((Var *) node, 0, false, context);
			break;

		case T_Const:
			get_const_expr((Const *) node, context, 0);
			break;

		case T_Param:
			get_parameter((Param *) node, context);
			break;

		case T_Aggref:
			get_agg_expr((Aggref *) node, context, (Aggref *) node);
			break;

		case T_GroupingFunc:
			{
				GroupingFunc *gexpr = (GroupingFunc *) node;

				appendStringInfoString(buf, "GROUPING(");
				get_rule_expr((Node *) gexpr->args, context, true);
				appendStringInfoChar(buf, ')');
			}
			break;

		case T_WindowFunc:
			get_windowfunc_expr((WindowFunc *) node, context);
			break;

		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;
				bool		need_parens;

				/*
				 * If the argument is a CaseTestExpr, we must be inside a
				 * FieldStore, ie, we are assigning to an element of an array
				 * within a composite column.  Since we already punted on
				 * displaying the FieldStore's target information, just punt
				 * here too, and display only the assignment source
				 * expression.
				 */
				if (IsA(aref->refexpr, CaseTestExpr))
				{
					Assert(aref->refassgnexpr);
					get_rule_expr((Node *) aref->refassgnexpr,
								  context, showimplicit);
					break;
				}

				/*
				 * Parenthesize the argument unless it's a simple Var or a
				 * FieldSelect.  (In particular, if it's another ArrayRef, we
				 * *must* parenthesize to avoid confusion.)
				 */
				need_parens = !IsA(aref->refexpr, Var) &&
					!IsA(aref->refexpr, FieldSelect);
				if (need_parens)
					appendStringInfoChar(buf, '(');
				get_rule_expr((Node *) aref->refexpr, context, showimplicit);
				if (need_parens)
					appendStringInfoChar(buf, ')');

				/*
				 * If there's a refassgnexpr, we want to print the node in the
				 * format "array[subscripts] := refassgnexpr".  This is not
				 * legal SQL, so decompilation of INSERT or UPDATE statements
				 * should always use processIndirection as part of the
				 * statement-level syntax.  We should only see this when
				 * EXPLAIN tries to print the targetlist of a plan resulting
				 * from such a statement.
				 */
				if (aref->refassgnexpr)
				{
					Node	   *refassgnexpr;

					/*
					 * Use processIndirection to print this node's subscripts
					 * as well as any additional field selections or
					 * subscripting in immediate descendants.  It returns the
					 * RHS expr that is actually being "assigned".
					 */
					refassgnexpr = processIndirection(node, context);
					appendStringInfoString(buf, " := ");
					get_rule_expr(refassgnexpr, context, showimplicit);
				}
				else
				{
					/* Just an ordinary array fetch, so print subscripts */
					printSubscripts(aref, context);
				}
			}
			break;

		case T_FuncExpr:
			get_func_expr((FuncExpr *) node, context, showimplicit);
			break;

		case T_NamedArgExpr:
			{
				NamedArgExpr *na = (NamedArgExpr *) node;

				appendStringInfo(buf, "%s => ", quote_identifier(na->name));
				get_rule_expr((Node *) na->arg, context, showimplicit);
			}
			break;

		case T_OpExpr:
			get_oper_expr((OpExpr *) node, context);
			break;

		case T_DistinctExpr:
			{
				DistinctExpr *expr = (DistinctExpr *) node;
				List	   *args = expr->args;
				Node	   *arg1 = (Node *) linitial(args);
				Node	   *arg2 = (Node *) lsecond(args);

				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, '(');
				get_rule_expr_paren(arg1, context, true, node);
				appendStringInfoString(buf, " IS DISTINCT FROM ");
				get_rule_expr_paren(arg2, context, true, node);
				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_NullIfExpr:
			{
				NullIfExpr *nullifexpr = (NullIfExpr *) node;

				appendStringInfoString(buf, "NULLIF(");
				get_rule_expr((Node *) nullifexpr->args, context, true);
				appendStringInfoChar(buf, ')');
			}
			break;

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;
				List	   *args = expr->args;
				Node	   *arg1 = (Node *) linitial(args);
				Node	   *arg2 = (Node *) lsecond(args);

				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, '(');
				get_rule_expr_paren(arg1, context, true, node);
				appendStringInfo(buf, " %s %s (",
								 generate_operator_name(expr->opno,
														exprType(arg1),
									  get_base_element_type(exprType(arg2))),
								 expr->useOr ? "ANY" : "ALL");
				get_rule_expr_paren(arg2, context, true, node);

				/*
				 * There's inherent ambiguity in "x op ANY/ALL (y)" when y is
				 * a bare sub-SELECT.  Since we're here, the sub-SELECT must
				 * be meant as a scalar sub-SELECT yielding an array value to
				 * be used in ScalarArrayOpExpr; but the grammar will
				 * preferentially interpret such a construct as an ANY/ALL
				 * SubLink.  To prevent misparsing the output that way, insert
				 * a dummy coercion (which will be stripped by parse analysis,
				 * so no inefficiency is added in dump and reload).  This is
				 * indeed most likely what the user wrote to get the construct
				 * accepted in the first place.
				 */
				if (IsA(arg2, SubLink) &&
					((SubLink *) arg2)->subLinkType == EXPR_SUBLINK)
					appendStringInfo(buf, "::%s",
									 format_type_with_typemod(exprType(arg2),
														  exprTypmod(arg2)));
				appendStringInfoChar(buf, ')');
				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;
				Node	   *first_arg = linitial(expr->args);
				ListCell   *arg = lnext(list_head(expr->args));

				switch (expr->boolop)
				{
					case AND_EXPR:
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, '(');
						get_rule_expr_paren(first_arg, context,
											false, node);
						while (arg)
						{
							appendStringInfoString(buf, " AND ");
							get_rule_expr_paren((Node *) lfirst(arg), context,
												false, node);
							arg = lnext(arg);
						}
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, ')');
						break;

					case OR_EXPR:
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, '(');
						get_rule_expr_paren(first_arg, context,
											false, node);
						while (arg)
						{
							appendStringInfoString(buf, " OR ");
							get_rule_expr_paren((Node *) lfirst(arg), context,
												false, node);
							arg = lnext(arg);
						}
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, ')');
						break;

					case NOT_EXPR:
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, '(');
						appendStringInfoString(buf, "NOT ");
						get_rule_expr_paren(first_arg, context,
											false, node);
						if (!PRETTY_PAREN(context))
							appendStringInfoChar(buf, ')');
						break;

					default:
						elog(ERROR, "unrecognized boolop: %d",
							 (int) expr->boolop);
				}
			}
			break;

		case T_SubLink:
			get_sublink_expr((SubLink *) node, context);
			break;

		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;

				/*
				 * We cannot see an already-planned subplan in rule deparsing,
				 * only while EXPLAINing a query plan.  We don't try to
				 * reconstruct the original SQL, just reference the subplan
				 * that appears elsewhere in EXPLAIN's result.
				 */
				if (subplan->useHashTable)
					appendStringInfo(buf, "(hashed %s)", subplan->plan_name);
				else
					appendStringInfo(buf, "(%s)", subplan->plan_name);
			}
			break;

		case T_AlternativeSubPlan:
			{
				AlternativeSubPlan *asplan = (AlternativeSubPlan *) node;
				ListCell   *lc;

				/* As above, this can only happen during EXPLAIN */
				appendStringInfoString(buf, "(alternatives: ");
				foreach(lc, asplan->subplans)
				{
					SubPlan    *splan = (SubPlan *) lfirst(lc);

					Assert(IsA(splan, SubPlan));
					if (splan->useHashTable)
						appendStringInfo(buf, "hashed %s", splan->plan_name);
					else
						appendStringInfoString(buf, splan->plan_name);
					if (lnext(lc))
						appendStringInfoString(buf, " or ");
				}
				appendStringInfoChar(buf, ')');
			}
			break;

		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;
				Node	   *arg = (Node *) fselect->arg;
				int			fno = fselect->fieldnum;
				const char *fieldname;
				bool		need_parens;

				/*
				 * Parenthesize the argument unless it's an ArrayRef or
				 * another FieldSelect.  Note in particular that it would be
				 * WRONG to not parenthesize a Var argument; simplicity is not
				 * the issue here, having the right number of names is.
				 */
				need_parens = !IsA(arg, ArrayRef) &&!IsA(arg, FieldSelect);
				if (need_parens)
					appendStringInfoChar(buf, '(');
				get_rule_expr(arg, context, true);
				if (need_parens)
					appendStringInfoChar(buf, ')');

				/*
				 * Get and print the field name.
				 */
				fieldname = get_name_for_var_field((Var *) arg, fno,
												   0, context);
				appendStringInfo(buf, ".%s", quote_identifier(fieldname));
			}
			break;

		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				bool		need_parens;

				/*
				 * There is no good way to represent a FieldStore as real SQL,
				 * so decompilation of INSERT or UPDATE statements should
				 * always use processIndirection as part of the
				 * statement-level syntax.  We should only get here when
				 * EXPLAIN tries to print the targetlist of a plan resulting
				 * from such a statement.  The plan case is even harder than
				 * ordinary rules would be, because the planner tries to
				 * collapse multiple assignments to the same field or subfield
				 * into one FieldStore; so we can see a list of target fields
				 * not just one, and the arguments could be FieldStores
				 * themselves.  We don't bother to try to print the target
				 * field names; we just print the source arguments, with a
				 * ROW() around them if there's more than one.  This isn't
				 * terribly complete, but it's probably good enough for
				 * EXPLAIN's purposes; especially since anything more would be
				 * either hopelessly confusing or an even poorer
				 * representation of what the plan is actually doing.
				 */
				need_parens = (list_length(fstore->newvals) != 1);
				if (need_parens)
					appendStringInfoString(buf, "ROW(");
				get_rule_expr((Node *) fstore->newvals, context, showimplicit);
				if (need_parens)
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_RelabelType:
			{
				RelabelType *relabel = (RelabelType *) node;
				Node	   *arg = (Node *) relabel->arg;

				if (relabel->relabelformat == COERCE_IMPLICIT_CAST &&
					!showimplicit)
				{
					/* don't show the implicit cast */
					get_rule_expr_paren(arg, context, false, node);
				}
				else
				{
					get_coercion_expr(arg, context,
									  relabel->resulttype,
									  relabel->resulttypmod,
									  node);
				}
			}
			break;

		case T_CoerceViaIO:
			{
				CoerceViaIO *iocoerce = (CoerceViaIO *) node;
				Node	   *arg = (Node *) iocoerce->arg;

				if (iocoerce->coerceformat == COERCE_IMPLICIT_CAST &&
					!showimplicit)
				{
					/* don't show the implicit cast */
					get_rule_expr_paren(arg, context, false, node);
				}
				else
				{
					get_coercion_expr(arg, context,
									  iocoerce->resulttype,
									  -1,
									  node);
				}
			}
			break;

		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acoerce = (ArrayCoerceExpr *) node;
				Node	   *arg = (Node *) acoerce->arg;

				if (acoerce->coerceformat == COERCE_IMPLICIT_CAST &&
					!showimplicit)
				{
					/* don't show the implicit cast */
					get_rule_expr_paren(arg, context, false, node);
				}
				else
				{
					get_coercion_expr(arg, context,
									  acoerce->resulttype,
									  acoerce->resulttypmod,
									  node);
				}
			}
			break;

		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;
				Node	   *arg = (Node *) convert->arg;

				if (convert->convertformat == COERCE_IMPLICIT_CAST &&
					!showimplicit)
				{
					/* don't show the implicit cast */
					get_rule_expr_paren(arg, context, false, node);
				}
				else
				{
					get_coercion_expr(arg, context,
									  convert->resulttype, -1,
									  node);
				}
			}
			break;

		case T_CollateExpr:
			{
				CollateExpr *collate = (CollateExpr *) node;
				Node	   *arg = (Node *) collate->arg;

				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, '(');
				get_rule_expr_paren(arg, context, showimplicit, node);
				appendStringInfo(buf, " COLLATE %s",
								 generate_collation_name(collate->collOid));
				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;
				ListCell   *temp;

				appendContextKeyword(context, "CASE",
									 0, PRETTYINDENT_VAR, 0);
				if (caseexpr->arg)
				{
					appendStringInfoChar(buf, ' ');
					get_rule_expr((Node *) caseexpr->arg, context, true);
				}
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(temp);
					Node	   *w = (Node *) when->expr;

					if (caseexpr->arg)
					{
						/*
						 * The parser should have produced WHEN clauses of the
						 * form "CaseTestExpr = RHS", possibly with an
						 * implicit coercion inserted above the CaseTestExpr.
						 * For accurate decompilation of rules it's essential
						 * that we show just the RHS.  However in an
						 * expression that's been through the optimizer, the
						 * WHEN clause could be almost anything (since the
						 * equality operator could have been expanded into an
						 * inline function).  If we don't recognize the form
						 * of the WHEN clause, just punt and display it as-is.
						 */
						if (IsA(w, OpExpr))
						{
							List	   *args = ((OpExpr *) w)->args;

							if (list_length(args) == 2 &&
								IsA(strip_implicit_coercions(linitial(args)),
									CaseTestExpr))
								w = (Node *) lsecond(args);
						}
					}

					if (!PRETTY_INDENT(context))
						appendStringInfoChar(buf, ' ');
					appendContextKeyword(context, "WHEN ",
										 0, 0, 0);
					get_rule_expr(w, context, false);
					appendStringInfoString(buf, " THEN ");
					get_rule_expr((Node *) when->result, context, true);
				}
				if (!PRETTY_INDENT(context))
					appendStringInfoChar(buf, ' ');
				appendContextKeyword(context, "ELSE ",
									 0, 0, 0);
				get_rule_expr((Node *) caseexpr->defresult, context, true);
				if (!PRETTY_INDENT(context))
					appendStringInfoChar(buf, ' ');
				appendContextKeyword(context, "END",
									 -PRETTYINDENT_VAR, 0, 0);
			}
			break;

		case T_CaseTestExpr:
			{
				/*
				 * Normally we should never get here, since for expressions
				 * that can contain this node type we attempt to avoid
				 * recursing to it.  But in an optimized expression we might
				 * be unable to avoid that (see comments for CaseExpr).  If we
				 * do see one, print it as CASE_TEST_EXPR.
				 */
				appendStringInfoString(buf, "CASE_TEST_EXPR");
			}
			break;

		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;

				appendStringInfoString(buf, "ARRAY[");
				get_rule_expr((Node *) arrayexpr->elements, context, true);
				appendStringInfoChar(buf, ']');

				/*
				 * If the array isn't empty, we assume its elements are
				 * coerced to the desired type.  If it's empty, though, we
				 * need an explicit coercion to the array type.
				 */
				if (arrayexpr->elements == NIL)
					appendStringInfo(buf, "::%s",
					  format_type_with_typemod(arrayexpr->array_typeid, -1));
			}
			break;

		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				TupleDesc	tupdesc = NULL;
				ListCell   *arg;
				int			i;
				char	   *sep;

				/*
				 * If it's a named type and not RECORD, we may have to skip
				 * dropped columns and/or claim there are NULLs for added
				 * columns.
				 */
				if (rowexpr->row_typeid != RECORDOID)
				{
					tupdesc = lookup_rowtype_tupdesc(rowexpr->row_typeid, -1);
					Assert(list_length(rowexpr->args) <= tupdesc->natts);
				}

				/*
				 * SQL99 allows "ROW" to be omitted when there is more than
				 * one column, but for simplicity we always print it.
				 */
				appendStringInfoString(buf, "ROW(");
				sep = "";
				i = 0;
				foreach(arg, rowexpr->args)
				{
					Node	   *e = (Node *) lfirst(arg);

					if (tupdesc == NULL ||
						!tupdesc->attrs[i]->attisdropped)
					{
						appendStringInfoString(buf, sep);
						/* Whole-row Vars need special treatment here */
						get_rule_expr_toplevel(e, context, true);
						sep = ", ";
					}
					i++;
				}
				if (tupdesc != NULL)
				{
					while (i < tupdesc->natts)
					{
						if (!tupdesc->attrs[i]->attisdropped)
						{
							appendStringInfoString(buf, sep);
							appendStringInfoString(buf, "NULL");
							sep = ", ";
						}
						i++;
					}

					ReleaseTupleDesc(tupdesc);
				}
				appendStringInfoChar(buf, ')');
				if (rowexpr->row_format == COERCE_EXPLICIT_CAST)
					appendStringInfo(buf, "::%s",
						  format_type_with_typemod(rowexpr->row_typeid, -1));
			}
			break;

		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				ListCell   *arg;
				char	   *sep;

				/*
				 * SQL99 allows "ROW" to be omitted when there is more than
				 * one column, but for simplicity we always print it.
				 */
				appendStringInfoString(buf, "(ROW(");
				sep = "";
				foreach(arg, rcexpr->largs)
				{
					Node	   *e = (Node *) lfirst(arg);

					appendStringInfoString(buf, sep);
					get_rule_expr(e, context, true);
					sep = ", ";
				}

				/*
				 * We assume that the name of the first-column operator will
				 * do for all the rest too.  This is definitely open to
				 * failure, eg if some but not all operators were renamed
				 * since the construct was parsed, but there seems no way to
				 * be perfect.
				 */
				appendStringInfo(buf, ") %s ROW(",
						  generate_operator_name(linitial_oid(rcexpr->opnos),
										   exprType(linitial(rcexpr->largs)),
										 exprType(linitial(rcexpr->rargs))));
				sep = "";
				foreach(arg, rcexpr->rargs)
				{
					Node	   *e = (Node *) lfirst(arg);

					appendStringInfoString(buf, sep);
					get_rule_expr(e, context, true);
					sep = ", ";
				}
				appendStringInfoString(buf, "))");
			}
			break;

		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesceexpr = (CoalesceExpr *) node;

				appendStringInfoString(buf, "COALESCE(");
				get_rule_expr((Node *) coalesceexpr->args, context, true);
				appendStringInfoChar(buf, ')');
			}
			break;

		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;

				switch (minmaxexpr->op)
				{
					case IS_GREATEST:
						appendStringInfoString(buf, "GREATEST(");
						break;
					case IS_LEAST:
						appendStringInfoString(buf, "LEAST(");
						break;
				}
				get_rule_expr((Node *) minmaxexpr->args, context, true);
				appendStringInfoChar(buf, ')');
			}
			break;

		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;
				bool		needcomma = false;
				ListCell   *arg;
				ListCell   *narg;
				Const	   *con;

				switch (xexpr->op)
				{
					case IS_XMLCONCAT:
						appendStringInfoString(buf, "XMLCONCAT(");
						break;
					case IS_XMLELEMENT:
						appendStringInfoString(buf, "XMLELEMENT(");
						break;
					case IS_XMLFOREST:
						appendStringInfoString(buf, "XMLFOREST(");
						break;
					case IS_XMLPARSE:
						appendStringInfoString(buf, "XMLPARSE(");
						break;
					case IS_XMLPI:
						appendStringInfoString(buf, "XMLPI(");
						break;
					case IS_XMLROOT:
						appendStringInfoString(buf, "XMLROOT(");
						break;
					case IS_XMLSERIALIZE:
						appendStringInfoString(buf, "XMLSERIALIZE(");
						break;
					case IS_DOCUMENT:
						break;
				}
				if (xexpr->op == IS_XMLPARSE || xexpr->op == IS_XMLSERIALIZE)
				{
					if (xexpr->xmloption == XMLOPTION_DOCUMENT)
						appendStringInfoString(buf, "DOCUMENT ");
					else
						appendStringInfoString(buf, "CONTENT ");
				}
				if (xexpr->name)
				{
					appendStringInfo(buf, "NAME %s",
									 quote_identifier(map_xml_name_to_sql_identifier(xexpr->name)));
					needcomma = true;
				}
				if (xexpr->named_args)
				{
					if (xexpr->op != IS_XMLFOREST)
					{
						if (needcomma)
							appendStringInfoString(buf, ", ");
						appendStringInfoString(buf, "XMLATTRIBUTES(");
						needcomma = false;
					}
					forboth(arg, xexpr->named_args, narg, xexpr->arg_names)
					{
						Node	   *e = (Node *) lfirst(arg);
						char	   *argname = strVal(lfirst(narg));

						if (needcomma)
							appendStringInfoString(buf, ", ");
						get_rule_expr((Node *) e, context, true);
						appendStringInfo(buf, " AS %s",
										 quote_identifier(map_xml_name_to_sql_identifier(argname)));
						needcomma = true;
					}
					if (xexpr->op != IS_XMLFOREST)
						appendStringInfoChar(buf, ')');
				}
				if (xexpr->args)
				{
					if (needcomma)
						appendStringInfoString(buf, ", ");
					switch (xexpr->op)
					{
						case IS_XMLCONCAT:
						case IS_XMLELEMENT:
						case IS_XMLFOREST:
						case IS_XMLPI:
						case IS_XMLSERIALIZE:
							/* no extra decoration needed */
							get_rule_expr((Node *) xexpr->args, context, true);
							break;
						case IS_XMLPARSE:
							Assert(list_length(xexpr->args) == 2);

							get_rule_expr((Node *) linitial(xexpr->args),
										  context, true);

							con = (Const *) lsecond(xexpr->args);
							Assert(IsA(con, Const));
							Assert(!con->constisnull);
							if (DatumGetBool(con->constvalue))
								appendStringInfoString(buf,
													 " PRESERVE WHITESPACE");
							else
								appendStringInfoString(buf,
													   " STRIP WHITESPACE");
							break;
						case IS_XMLROOT:
							Assert(list_length(xexpr->args) == 3);

							get_rule_expr((Node *) linitial(xexpr->args),
										  context, true);

							appendStringInfoString(buf, ", VERSION ");
							con = (Const *) lsecond(xexpr->args);
							if (IsA(con, Const) &&
								con->constisnull)
								appendStringInfoString(buf, "NO VALUE");
							else
								get_rule_expr((Node *) con, context, false);

							con = (Const *) lthird(xexpr->args);
							Assert(IsA(con, Const));
							if (con->constisnull)
								 /* suppress STANDALONE NO VALUE */ ;
							else
							{
								switch (DatumGetInt32(con->constvalue))
								{
									case XML_STANDALONE_YES:
										appendStringInfoString(buf,
														 ", STANDALONE YES");
										break;
									case XML_STANDALONE_NO:
										appendStringInfoString(buf,
														  ", STANDALONE NO");
										break;
									case XML_STANDALONE_NO_VALUE:
										appendStringInfoString(buf,
													", STANDALONE NO VALUE");
										break;
									default:
										break;
								}
							}
							break;
						case IS_DOCUMENT:
							get_rule_expr_paren((Node *) xexpr->args, context, false, node);
							break;
					}

				}
				if (xexpr->op == IS_XMLSERIALIZE)
					appendStringInfo(buf, " AS %s",
									 format_type_with_typemod(xexpr->type,
															  xexpr->typmod));
				if (xexpr->op == IS_DOCUMENT)
					appendStringInfoString(buf, " IS DOCUMENT");
				else
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;

				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, '(');
				get_rule_expr_paren((Node *) ntest->arg, context, true, node);

				/*
				 * For scalar inputs, we prefer to print as IS [NOT] NULL,
				 * which is shorter and traditional.  If it's a rowtype input
				 * but we're applying a scalar test, must print IS [NOT]
				 * DISTINCT FROM NULL to be semantically correct.
				 */
				if (ntest->argisrow ||
					!type_is_rowtype(exprType((Node *) ntest->arg)))
				{
					switch (ntest->nulltesttype)
					{
						case IS_NULL:
							appendStringInfoString(buf, " IS NULL");
							break;
						case IS_NOT_NULL:
							appendStringInfoString(buf, " IS NOT NULL");
							break;
						default:
							elog(ERROR, "unrecognized nulltesttype: %d",
								 (int) ntest->nulltesttype);
					}
				}
				else
				{
					switch (ntest->nulltesttype)
					{
						case IS_NULL:
							appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL");
							break;
						case IS_NOT_NULL:
							appendStringInfoString(buf, " IS DISTINCT FROM NULL");
							break;
						default:
							elog(ERROR, "unrecognized nulltesttype: %d",
								 (int) ntest->nulltesttype);
					}
				}
				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;

				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, '(');
				get_rule_expr_paren((Node *) btest->arg, context, false, node);
				switch (btest->booltesttype)
				{
					case IS_TRUE:
						appendStringInfoString(buf, " IS TRUE");
						break;
					case IS_NOT_TRUE:
						appendStringInfoString(buf, " IS NOT TRUE");
						break;
					case IS_FALSE:
						appendStringInfoString(buf, " IS FALSE");
						break;
					case IS_NOT_FALSE:
						appendStringInfoString(buf, " IS NOT FALSE");
						break;
					case IS_UNKNOWN:
						appendStringInfoString(buf, " IS UNKNOWN");
						break;
					case IS_NOT_UNKNOWN:
						appendStringInfoString(buf, " IS NOT UNKNOWN");
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) btest->booltesttype);
				}
				if (!PRETTY_PAREN(context))
					appendStringInfoChar(buf, ')');
			}
			break;

		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;
				Node	   *arg = (Node *) ctest->arg;

				if (ctest->coercionformat == COERCE_IMPLICIT_CAST &&
					!showimplicit)
				{
					/* don't show the implicit cast */
					get_rule_expr(arg, context, false);
				}
				else
				{
					get_coercion_expr(arg, context,
									  ctest->resulttype,
									  ctest->resulttypmod,
									  node);
				}
			}
			break;

		case T_CoerceToDomainValue:
			appendStringInfoString(buf, "VALUE");
			break;

		case T_SetToDefault:
			appendStringInfoString(buf, "DEFAULT");
			break;

		case T_CurrentOfExpr:
			{
				CurrentOfExpr *cexpr = (CurrentOfExpr *) node;

				if (cexpr->cursor_name)
					appendStringInfo(buf, "CURRENT OF %s",
									 quote_identifier(cexpr->cursor_name));
				else
					appendStringInfo(buf, "CURRENT OF $%d",
									 cexpr->cursor_param);
			}
			break;

		case T_InferenceElem:
			{
				InferenceElem *iexpr = (InferenceElem *) node;
				bool		save_varprefix;
				bool		need_parens;

				/*
				 * InferenceElem can only refer to target relation, so a
				 * prefix is not useful, and indeed would cause parse errors.
				 */
				save_varprefix = context->varprefix;
				context->varprefix = false;

				/*
				 * Parenthesize the element unless it's a simple Var or a bare
				 * function call.  Follows pg_get_indexdef_worker().
				 */
				need_parens = !IsA(iexpr->expr, Var);
				if (IsA(iexpr->expr, FuncExpr) &&
					((FuncExpr *) iexpr->expr)->funcformat ==
					COERCE_EXPLICIT_CALL)
					need_parens = false;

				if (need_parens)
					appendStringInfoChar(buf, '(');
				get_rule_expr((Node *) iexpr->expr,
							  context, false);
				if (need_parens)
					appendStringInfoChar(buf, ')');

				context->varprefix = save_varprefix;

				if (iexpr->infercollid)
					appendStringInfo(buf, " COLLATE %s",
								generate_collation_name(iexpr->infercollid));

				/* Add the operator class name, if not default */
				if (iexpr->inferopclass)
				{
					Oid			inferopclass = iexpr->inferopclass;
					Oid			inferopcinputtype = get_opclass_input_type(iexpr->inferopclass);

					get_opclass_name(inferopclass, inferopcinputtype, buf);
				}
			}
			break;

		case T_List:
			{
				char	   *sep;
				ListCell   *l;

				sep = "";
				foreach(l, (List *) node)
				{
					appendStringInfoString(buf, sep);
					get_rule_expr((Node *) lfirst(l), context, showimplicit);
					sep = ", ";
				}
			}
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
			break;
	}
}

/*
 * get_rule_expr_toplevel		- Parse back a toplevel expression
 *
 * Same as get_rule_expr(), except that if the expr is just a Var, we pass
 * istoplevel = true not false to get_variable().  This causes whole-row Vars
 * to get printed with decoration that will prevent expansion of "*".
 * We need to use this in contexts such as ROW() and VALUES(), where the
 * parser would expand "foo.*" appearing at top level.  (In principle we'd
 * use this in get_target_list() too, but that has additional worries about
 * whether to print AS, so it needs to invoke get_variable() directly anyway.)
 */
static void
get_rule_expr_toplevel(Node *node, deparse_context *context,
					   bool showimplicit)
{
	if (node && IsA(node, Var))
		(void) get_variable((Var *) node, 0, true, context);
	else
		get_rule_expr(node, context, showimplicit);
}


/*
 * get_oper_expr			- Parse back an OpExpr node
 */
static void
get_oper_expr(OpExpr *expr, deparse_context *context)
{
	StringInfo	buf = context->buf;
	Oid			opno = expr->opno;
	List	   *args = expr->args;

	if (!PRETTY_PAREN(context))
		appendStringInfoChar(buf, '(');
	if (list_length(args) == 2)
	{
		/* binary operator */
		Node	   *arg1 = (Node *) linitial(args);
		Node	   *arg2 = (Node *) lsecond(args);

		get_rule_expr_paren(arg1, context, true, (Node *) expr);
		appendStringInfo(buf, " %s ",
						 generate_operator_name(opno,
												exprType(arg1),
												exprType(arg2)));
		get_rule_expr_paren(arg2, context, true, (Node *) expr);
	}
	else
	{
		/* unary operator --- but which side? */
		Node	   *arg = (Node *) linitial(args);
		HeapTuple	tp;
		Form_pg_operator optup;

		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for operator %u", opno);
		optup = (Form_pg_operator) GETSTRUCT(tp);
		switch (optup->oprkind)
		{
			case 'l':
				appendStringInfo(buf, "%s ",
								 generate_operator_name(opno,
														InvalidOid,
														exprType(arg)));
				get_rule_expr_paren(arg, context, true, (Node *) expr);
				break;
			case 'r':
				get_rule_expr_paren(arg, context, true, (Node *) expr);
				appendStringInfo(buf, " %s",
								 generate_operator_name(opno,
														exprType(arg),
														InvalidOid));
				break;
			default:
				elog(ERROR, "bogus oprkind: %d", optup->oprkind);
		}
		ReleaseSysCache(tp);
	}
	if (!PRETTY_PAREN(context))
		appendStringInfoChar(buf, ')');
}

/*
 * get_func_expr			- Parse back a FuncExpr node
 */
static void
get_func_expr(FuncExpr *expr, deparse_context *context,
			  bool showimplicit)
{
	StringInfo	buf = context->buf;
	Oid			funcoid = expr->funcid;
	Oid			argtypes[FUNC_MAX_ARGS];
	int			nargs;
	List	   *argnames;
	bool		use_variadic;
	ListCell   *l;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument --- unless caller wants to see implicit coercions.
	 */
	if (expr->funcformat == COERCE_IMPLICIT_CAST && !showimplicit)
	{
		get_rule_expr_paren((Node *) linitial(expr->args), context,
							false, (Node *) expr);
		return;
	}

	/*
	 * If the function call came from a cast, then show the first argument
	 * plus an explicit cast operation.
	 */
	if (expr->funcformat == COERCE_EXPLICIT_CAST ||
		expr->funcformat == COERCE_IMPLICIT_CAST)
	{
		Node	   *arg = linitial(expr->args);
		Oid			rettype = expr->funcresulttype;
		int32		coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) expr, &coercedTypmod);

		get_coercion_expr(arg, context,
						  rettype, coercedTypmod,
						  (Node *) expr);

		return;
	}

	/*
	 * Normal function: display as proname(args).  First we need to extract
	 * the argument datatypes.
	 */
	if (list_length(expr->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("too many arguments")));
	nargs = 0;
	argnames = NIL;
	foreach(l, expr->args)
	{
		Node	   *arg = (Node *) lfirst(l);

		if (IsA(arg, NamedArgExpr))
			argnames = lappend(argnames, ((NamedArgExpr *) arg)->name);
		argtypes[nargs] = exprType(arg);
		nargs++;
	}

	appendStringInfo(buf, "%s(",
					 generate_function_name(funcoid, nargs,
											argnames, argtypes,
											expr->funcvariadic,
											&use_variadic,
											context->special_exprkind));
	nargs = 0;
	foreach(l, expr->args)
	{
		if (nargs++ > 0)
			appendStringInfoString(buf, ", ");
		if (use_variadic && lnext(l) == NULL)
			appendStringInfoString(buf, "VARIADIC ");
		get_rule_expr((Node *) lfirst(l), context, true);
	}
	appendStringInfoChar(buf, ')');
}

/*
 * get_agg_expr			- Parse back an Aggref node
 */
static void
get_agg_expr(Aggref *aggref, deparse_context *context,
			 Aggref *original_aggref)
{
	StringInfo	buf = context->buf;
	Oid			argtypes[FUNC_MAX_ARGS];
	int			nargs;
	bool		use_variadic;

	/*
	 * For a combining aggregate, we look up and deparse the corresponding
	 * partial aggregate instead.  This is necessary because our input
	 * argument list has been replaced; the new argument list always has just
	 * one element, which will point to a partial Aggref that supplies us with
	 * transition states to combine.
	 */
	if (DO_AGGSPLIT_COMBINE(aggref->aggsplit))
	{
		TargetEntry *tle = linitial(aggref->args);

		Assert(list_length(aggref->args) == 1);
		Assert(IsA(tle, TargetEntry));
		resolve_special_varno((Node *) tle->expr, context, original_aggref,
							  get_agg_combine_expr);
		return;
	}

	/*
	 * Mark as PARTIAL, if appropriate.  We look to the original aggref so as
	 * to avoid printing this when recursing from the code just above.
	 */
	if (DO_AGGSPLIT_SKIPFINAL(original_aggref->aggsplit))
		appendStringInfoString(buf, "PARTIAL ");

	/* Extract the argument types as seen by the parser */
	nargs = get_aggregate_argtypes(aggref, argtypes);

	/* Print the aggregate name, schema-qualified if needed */
	appendStringInfo(buf, "%s(%s",
					 generate_function_name(aggref->aggfnoid, nargs,
											NIL, argtypes,
											aggref->aggvariadic,
											&use_variadic,
											context->special_exprkind),
					 (aggref->aggdistinct != NIL) ? "DISTINCT " : "");

	if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
	{
		/*
		 * Ordered-set aggregates do not use "*" syntax.  Also, we needn't
		 * worry about inserting VARIADIC.  So we can just dump the direct
		 * args as-is.
		 */
		Assert(!aggref->aggvariadic);
		get_rule_expr((Node *) aggref->aggdirectargs, context, true);
		Assert(aggref->aggorder != NIL);
		appendStringInfoString(buf, ") WITHIN GROUP (ORDER BY ");
		get_rule_orderby(aggref->aggorder, aggref->args, false, context);
	}
	else
	{
		/* aggstar can be set only in zero-argument aggregates */
		if (aggref->aggstar)
			appendStringInfoChar(buf, '*');
		else
		{
			ListCell   *l;
			int			i;

			i = 0;
			foreach(l, aggref->args)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(l);
				Node	   *arg = (Node *) tle->expr;

				Assert(!IsA(arg, NamedArgExpr));
				if (tle->resjunk)
					continue;
				if (i++ > 0)
					appendStringInfoString(buf, ", ");
				if (use_variadic && i == nargs)
					appendStringInfoString(buf, "VARIADIC ");
				get_rule_expr(arg, context, true);
			}
		}

		if (aggref->aggorder != NIL)
		{
			appendStringInfoString(buf, " ORDER BY ");
			get_rule_orderby(aggref->aggorder, aggref->args, false, context);
		}
	}

	if (aggref->aggfilter != NULL)
	{
		appendStringInfoString(buf, ") FILTER (WHERE ");
		get_rule_expr((Node *) aggref->aggfilter, context, false);
	}

	appendStringInfoChar(buf, ')');
}

/*
 * This is a helper function for get_agg_expr().  It's used when we deparse
 * a combining Aggref; resolve_special_varno locates the corresponding partial
 * Aggref and then calls this.
 */
static void
get_agg_combine_expr(Node *node, deparse_context *context, void *private)
{
	Aggref	   *aggref;
	Aggref	   *original_aggref = private;

	if (!IsA(node, Aggref))
		elog(ERROR, "combining Aggref does not point to an Aggref");

	aggref = (Aggref *) node;
	get_agg_expr(aggref, context, original_aggref);
}

/*
 * get_windowfunc_expr	- Parse back a WindowFunc node
 */
static void
get_windowfunc_expr(WindowFunc *wfunc, deparse_context *context)
{
	StringInfo	buf = context->buf;
	Oid			argtypes[FUNC_MAX_ARGS];
	int			nargs;
	List	   *argnames;
	ListCell   *l;

	if (list_length(wfunc->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("too many arguments")));
	nargs = 0;
	argnames = NIL;
	foreach(l, wfunc->args)
	{
		Node	   *arg = (Node *) lfirst(l);

		if (IsA(arg, NamedArgExpr))
			argnames = lappend(argnames, ((NamedArgExpr *) arg)->name);
		argtypes[nargs] = exprType(arg);
		nargs++;
	}

	appendStringInfo(buf, "%s(",
					 generate_function_name(wfunc->winfnoid, nargs,
											argnames, argtypes,
											false, NULL,
											context->special_exprkind));
	/* winstar can be set only in zero-argument aggregates */
	if (wfunc->winstar)
		appendStringInfoChar(buf, '*');
	else
		get_rule_expr((Node *) wfunc->args, context, true);

	if (wfunc->aggfilter != NULL)
	{
		appendStringInfoString(buf, ") FILTER (WHERE ");
		get_rule_expr((Node *) wfunc->aggfilter, context, false);
	}

	appendStringInfoString(buf, ") OVER ");

	foreach(l, context->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);

		if (wc->winref == wfunc->winref)
		{
			if (wc->name)
				appendStringInfoString(buf, quote_identifier(wc->name));
			else
				get_rule_windowspec(wc, context->windowTList, context);
			break;
		}
	}
	if (l == NULL)
	{
		if (context->windowClause)
			elog(ERROR, "could not find window clause for winref %u",
				 wfunc->winref);

		/*
		 * In EXPLAIN, we don't have window context information available, so
		 * we have to settle for this:
		 */
		appendStringInfoString(buf, "(?)");
	}
}

/* ----------
 * get_coercion_expr
 *
 *	Make a string representation of a value coerced to a specific type
 * ----------
 */
static void
get_coercion_expr(Node *arg, deparse_context *context,
				  Oid resulttype, int32 resulttypmod,
				  Node *parentNode)
{
	StringInfo	buf = context->buf;

	/*
	 * Since parse_coerce.c doesn't immediately collapse application of
	 * length-coercion functions to constants, what we'll typically see in
	 * such cases is a Const with typmod -1 and a length-coercion function
	 * right above it.  Avoid generating redundant output. However, beware of
	 * suppressing casts when the user actually wrote something like
	 * 'foo'::text::char(3).
	 *
	 * Note: it might seem that we are missing the possibility of needing to
	 * print a COLLATE clause for such a Const.  However, a Const could only
	 * have nondefault collation in a post-constant-folding tree, in which the
	 * length coercion would have been folded too.  See also the special
	 * handling of CollateExpr in coerce_to_target_type(): any collation
	 * marking will be above the coercion node, not below it.
	 */
	if (arg && IsA(arg, Const) &&
		((Const *) arg)->consttype == resulttype &&
		((Const *) arg)->consttypmod == -1)
	{
		/* Show the constant without normal ::typename decoration */
		get_const_expr((Const *) arg, context, -1);
	}
	else
	{
		if (!PRETTY_PAREN(context))
			appendStringInfoChar(buf, '(');
		get_rule_expr_paren(arg, context, false, parentNode);
		if (!PRETTY_PAREN(context))
			appendStringInfoChar(buf, ')');
	}
	appendStringInfo(buf, "::%s",
					 format_type_with_typemod(resulttype, resulttypmod));
}

/* ----------
 * get_const_expr
 *
 *	Make a string representation of a Const
 *
 * showtype can be -1 to never show "::typename" decoration, or +1 to always
 * show it, or 0 to show it only if the constant wouldn't be assumed to be
 * the right type by default.
 *
 * If the Const's collation isn't default for its type, show that too.
 * We mustn't do this when showtype is -1 (since that means the caller will
 * print "::typename", and we can't put a COLLATE clause in between).  It's
 * caller's responsibility that collation isn't missed in such cases.
 * ----------
 */
static void
get_const_expr(Const *constval, deparse_context *context, int showtype)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		needlabel = false;

	if (constval->constisnull)
	{
		/*
		 * Always label the type of a NULL constant to prevent misdecisions
		 * about type when reparsing.
		 */
		appendStringInfoString(buf, "NULL");
		if (showtype >= 0)
		{
			appendStringInfo(buf, "::%s",
							 format_type_with_typemod(constval->consttype,
													  constval->consttypmod));
			get_const_collation(constval, context);
		}
		return;
	}

	getTypeOutputInfo(constval->consttype,
					  &typoutput, &typIsVarlena);

	extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	switch (constval->consttype)
	{
		case INT4OID:

			/*
			 * INT4 can be printed without any decoration, unless it is
			 * negative; in that case print it as '-nnn'::integer to ensure
			 * that the output will re-parse as a constant, not as a constant
			 * plus operator.  In most cases we could get away with printing
			 * (-nnn) instead, because of the way that gram.y handles negative
			 * literals; but that doesn't work for INT_MIN, and it doesn't
			 * seem that much prettier anyway.
			 */
			if (extval[0] != '-')
				appendStringInfoString(buf, extval);
			else
			{
				appendStringInfo(buf, "'%s'", extval);
				needlabel = true;		/* we must attach a cast */
			}
			break;

		case NUMERICOID:

			/*
			 * NUMERIC can be printed without quotes if it looks like a float
			 * constant (not an integer, and not Infinity or NaN) and doesn't
			 * have a leading sign (for the same reason as for INT4).
			 */
			if (isdigit((unsigned char) extval[0]) &&
				strcspn(extval, "eE.") != strlen(extval))
			{
				appendStringInfoString(buf, extval);
			}
			else
			{
				appendStringInfo(buf, "'%s'", extval);
				needlabel = true;		/* we must attach a cast */
			}
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(buf, "B'%s'", extval);
			break;

		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;

		default:
			simple_quote_literal(buf, extval);
			break;
	}

	pfree(extval);

	if (showtype < 0)
		return;

	/*
	 * For showtype == 0, append ::typename unless the constant will be
	 * implicitly typed as the right type when it is read in.
	 *
	 * XXX this code has to be kept in sync with the behavior of the parser,
	 * especially make_const.
	 */
	switch (constval->consttype)
	{
		case BOOLOID:
		case UNKNOWNOID:
			/* These types can be left unlabeled */
			needlabel = false;
			break;
		case INT4OID:
			/* We determined above whether a label is needed */
			break;
		case NUMERICOID:

			/*
			 * Float-looking constants will be typed as numeric, which we
			 * checked above; but if there's a nondefault typmod we need to
			 * show it.
			 */
			needlabel |= (constval->consttypmod >= 0);
			break;
		default:
			needlabel = true;
			break;
	}
	if (needlabel || showtype > 0)
		appendStringInfo(buf, "::%s",
						 format_type_with_typemod(constval->consttype,
												  constval->consttypmod));

	get_const_collation(constval, context);
}

/*
 * helper for get_const_expr: append COLLATE if needed
 */
static void
get_const_collation(Const *constval, deparse_context *context)
{
	StringInfo	buf = context->buf;

	if (OidIsValid(constval->constcollid))
	{
		Oid			typcollation = get_typcollation(constval->consttype);

		if (constval->constcollid != typcollation)
		{
			appendStringInfo(buf, " COLLATE %s",
							 generate_collation_name(constval->constcollid));
		}
	}
}

/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */
static void
simple_quote_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 * We form the string literal according to the prevailing setting of
	 * standard_conforming_strings; we never use E''. User is responsible for
	 * making sure result is used correctly.
	 */
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, !standard_conforming_strings))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}


/* ----------
 * get_sublink_expr			- Parse back a sublink
 * ----------
 */
static void
get_sublink_expr(SubLink *sublink, deparse_context *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = (Query *) (sublink->subselect);
	char	   *opname = NULL;
	bool		need_paren;

	if (sublink->subLinkType == ARRAY_SUBLINK)
		appendStringInfoString(buf, "ARRAY(");
	else
		appendStringInfoChar(buf, '(');

	/*
	 * Note that we print the name of only the first operator, when there are
	 * multiple combining operators.  This is an approximation that could go
	 * wrong in various scenarios (operators in different schemas, renamed
	 * operators, etc) but there is not a whole lot we can do about it, since
	 * the syntax allows only one operator to be shown.
	 */
	if (sublink->testexpr)
	{
		if (IsA(sublink->testexpr, OpExpr))
		{
			/* single combining operator */
			OpExpr	   *opexpr = (OpExpr *) sublink->testexpr;

			get_rule_expr(linitial(opexpr->args), context, true);
			opname = generate_operator_name(opexpr->opno,
											exprType(linitial(opexpr->args)),
											exprType(lsecond(opexpr->args)));
		}
		else if (IsA(sublink->testexpr, BoolExpr))
		{
			/* multiple combining operators, = or <> cases */
			char	   *sep;
			ListCell   *l;

			appendStringInfoChar(buf, '(');
			sep = "";
			foreach(l, ((BoolExpr *) sublink->testexpr)->args)
			{
				OpExpr	   *opexpr = (OpExpr *) lfirst(l);

				Assert(IsA(opexpr, OpExpr));
				appendStringInfoString(buf, sep);
				get_rule_expr(linitial(opexpr->args), context, true);
				if (!opname)
					opname = generate_operator_name(opexpr->opno,
											exprType(linitial(opexpr->args)),
											exprType(lsecond(opexpr->args)));
				sep = ", ";
			}
			appendStringInfoChar(buf, ')');
		}
		else if (IsA(sublink->testexpr, RowCompareExpr))
		{
			/* multiple combining operators, < <= > >= cases */
			RowCompareExpr *rcexpr = (RowCompareExpr *) sublink->testexpr;

			appendStringInfoChar(buf, '(');
			get_rule_expr((Node *) rcexpr->largs, context, true);
			opname = generate_operator_name(linitial_oid(rcexpr->opnos),
											exprType(linitial(rcexpr->largs)),
										  exprType(linitial(rcexpr->rargs)));
			appendStringInfoChar(buf, ')');
		}
		else
			elog(ERROR, "unrecognized testexpr type: %d",
				 (int) nodeTag(sublink->testexpr));
	}

	need_paren = true;

	switch (sublink->subLinkType)
	{
		case EXISTS_SUBLINK:
			appendStringInfoString(buf, "EXISTS ");
			break;

		case ANY_SUBLINK:
			if (strcmp(opname, "=") == 0)		/* Represent = ANY as IN */
				appendStringInfoString(buf, " IN ");
			else
				appendStringInfo(buf, " %s ANY ", opname);
			break;

		case ALL_SUBLINK:
			appendStringInfo(buf, " %s ALL ", opname);
			break;

		case ROWCOMPARE_SUBLINK:
			appendStringInfo(buf, " %s ", opname);
			break;

		case EXPR_SUBLINK:
		case MULTIEXPR_SUBLINK:
		case ARRAY_SUBLINK:
			need_paren = false;
			break;

		case CTE_SUBLINK:		/* shouldn't occur in a SubLink */
		default:
			elog(ERROR, "unrecognized sublink type: %d",
				 (int) sublink->subLinkType);
			break;
	}

	if (need_paren)
		appendStringInfoChar(buf, '(');

	get_query_def(query, buf, context->namespaces, NULL,
				  context->prettyFlags, context->wrapColumn,
				  context->indentLevel);

	if (need_paren)
		appendStringInfoString(buf, "))");
	else
		appendStringInfoChar(buf, ')');
}


/* ----------
 * get_from_clause			- Parse back a FROM clause
 *
 * "prefix" is the keyword that denotes the start of the list of FROM
 * elements. It is FROM when used to parse back SELECT and UPDATE, but
 * is USING when parsing back DELETE.
 * ----------
 */
static void
get_from_clause(Query *query, const char *prefix, deparse_context *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *l;

	/*
	 * We use the query's jointree as a guide to what to print.  However, we
	 * must ignore auto-added RTEs that are marked not inFromCl. (These can
	 * only appear at the top level of the jointree, so it's sufficient to
	 * check here.)  This check also ensures we ignore the rule pseudo-RTEs
	 * for NEW and OLD.
	 */
	foreach(l, query->jointree->fromlist)
	{
		Node	   *jtnode = (Node *) lfirst(l);

		if (IsA(jtnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jtnode)->rtindex;
			RangeTblEntry *rte = rt_fetch(varno, query->rtable);

			if (!rte->inFromCl)
				continue;
		}

		if (first)
		{
			appendContextKeyword(context, prefix,
								 -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
			first = false;

			get_from_clause_item(jtnode, query, context);
		}
		else
		{
			StringInfoData itembuf;

			appendStringInfoString(buf, ", ");

			/*
			 * Put the new FROM item's text into itembuf so we can decide
			 * after we've got it whether or not it needs to go on a new line.
			 */
			initStringInfo(&itembuf);
			context->buf = &itembuf;

			get_from_clause_item(jtnode, query, context);

			/* Restore context's output buffer */
			context->buf = buf;

			/* Consider line-wrapping if enabled */
			if (PRETTY_INDENT(context) && context->wrapColumn >= 0)
			{
				/* Does the new item start with a new line? */
				if (itembuf.len > 0 && itembuf.data[0] == '\n')
				{
					/* If so, we shouldn't add anything */
					/* instead, remove any trailing spaces currently in buf */
					removeStringInfoSpaces(buf);
				}
				else
				{
					char	   *trailing_nl;

					/* Locate the start of the current line in the buffer */
					trailing_nl = strrchr(buf->data, '\n');
					if (trailing_nl == NULL)
						trailing_nl = buf->data;
					else
						trailing_nl++;

					/*
					 * Add a newline, plus some indentation, if the new item
					 * would cause an overflow.
					 */
					if (strlen(trailing_nl) + itembuf.len > context->wrapColumn)
						appendContextKeyword(context, "", -PRETTYINDENT_STD,
											 PRETTYINDENT_STD,
											 PRETTYINDENT_VAR);
				}
			}

			/* Add the new item */
			appendStringInfoString(buf, itembuf.data);

			/* clean up */
			pfree(itembuf.data);
		}
	}
}

static void
get_from_clause_item(Node *jtnode, Query *query, deparse_context *context)
{
	StringInfo	buf = context->buf;
	deparse_namespace *dpns = (deparse_namespace *) linitial(context->namespaces);

	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;
		RangeTblEntry *rte = rt_fetch(varno, query->rtable);
		char	   *refname = get_rtable_name(varno, context);
		deparse_columns *colinfo = deparse_columns_fetch(varno, dpns);
		RangeTblFunction *rtfunc1 = NULL;
		bool		printalias;

		if (rte->lateral)
			appendStringInfoString(buf, "LATERAL ");

		/* Print the FROM item proper */
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				/* Normal relation RTE */
				appendStringInfo(buf, "%s%s",
								 only_marker(rte),
								 generate_relation_name(rte->relid,
														context->namespaces));
				break;
			case RTE_SUBQUERY:
				/* Subquery RTE */
				appendStringInfoChar(buf, '(');
				get_query_def(rte->subquery, buf, context->namespaces, NULL,
							  context->prettyFlags, context->wrapColumn,
							  context->indentLevel);
				appendStringInfoChar(buf, ')');
				break;
			case RTE_FUNCTION:
				/* Function RTE */
				rtfunc1 = (RangeTblFunction *) linitial(rte->functions);

				/*
				 * Omit ROWS FROM() syntax for just one function, unless it
				 * has both a coldeflist and WITH ORDINALITY. If it has both,
				 * we must use ROWS FROM() syntax to avoid ambiguity about
				 * whether the coldeflist includes the ordinality column.
				 */
				if (list_length(rte->functions) == 1 &&
					(rtfunc1->funccolnames == NIL || !rte->funcordinality))
				{
					get_rule_expr(rtfunc1->funcexpr, context, true);
					/* we'll print the coldeflist below, if it has one */
				}
				else
				{
					bool		all_unnest;
					ListCell   *lc;

					/*
					 * If all the function calls in the list are to unnest,
					 * and none need a coldeflist, then collapse the list back
					 * down to UNNEST(args).  (If we had more than one
					 * built-in unnest function, this would get more
					 * difficult.)
					 *
					 * XXX This is pretty ugly, since it makes not-terribly-
					 * future-proof assumptions about what the parser would do
					 * with the output; but the alternative is to emit our
					 * nonstandard ROWS FROM() notation for what might have
					 * been a perfectly spec-compliant multi-argument
					 * UNNEST().
					 */
					all_unnest = true;
					foreach(lc, rte->functions)
					{
						RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

						if (!IsA(rtfunc->funcexpr, FuncExpr) ||
							((FuncExpr *) rtfunc->funcexpr)->funcid != F_ARRAY_UNNEST ||
							rtfunc->funccolnames != NIL)
						{
							all_unnest = false;
							break;
						}
					}

					if (all_unnest)
					{
						List	   *allargs = NIL;

						foreach(lc, rte->functions)
						{
							RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);
							List	   *args = ((FuncExpr *) rtfunc->funcexpr)->args;

							allargs = list_concat(allargs, list_copy(args));
						}

						appendStringInfoString(buf, "UNNEST(");
						get_rule_expr((Node *) allargs, context, true);
						appendStringInfoChar(buf, ')');
					}
					else
					{
						int			funcno = 0;

						appendStringInfoString(buf, "ROWS FROM(");
						foreach(lc, rte->functions)
						{
							RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

							if (funcno > 0)
								appendStringInfoString(buf, ", ");
							get_rule_expr(rtfunc->funcexpr, context, true);
							if (rtfunc->funccolnames != NIL)
							{
								/* Reconstruct the column definition list */
								appendStringInfoString(buf, " AS ");
								get_from_clause_coldeflist(rtfunc,
														   NULL,
														   context);
							}
							funcno++;
						}
						appendStringInfoChar(buf, ')');
					}
					/* prevent printing duplicate coldeflist below */
					rtfunc1 = NULL;
				}
				if (rte->funcordinality)
					appendStringInfoString(buf, " WITH ORDINALITY");
				break;
			case RTE_VALUES:
				/* Values list RTE */
				appendStringInfoChar(buf, '(');
				get_values_def(rte->values_lists, context);
				appendStringInfoChar(buf, ')');
				break;
			case RTE_CTE:
				appendStringInfoString(buf, quote_identifier(rte->ctename));
				break;
			default:
				elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
				break;
		}

		/* Print the relation alias, if needed */
		printalias = false;
		if (rte->alias != NULL)
		{
			/* Always print alias if user provided one */
			printalias = true;
		}
		else if (colinfo->printaliases)
		{
			/* Always print alias if we need to print column aliases */
			printalias = true;
		}
		else if (rte->rtekind == RTE_RELATION)
		{
			/*
			 * No need to print alias if it's same as relation name (this
			 * would normally be the case, but not if set_rtable_names had to
			 * resolve a conflict).
			 */
			if (strcmp(refname, get_relation_name(rte->relid)) != 0)
				printalias = true;
		}
		else if (rte->rtekind == RTE_FUNCTION)
		{
			/*
			 * For a function RTE, always print alias.  This covers possible
			 * renaming of the function and/or instability of the
			 * FigureColname rules for things that aren't simple functions.
			 * Note we'd need to force it anyway for the columndef list case.
			 */
			printalias = true;
		}
		else if (rte->rtekind == RTE_VALUES)
		{
			/* Alias is syntactically required for VALUES */
			printalias = true;
		}
		else if (rte->rtekind == RTE_CTE)
		{
			/*
			 * No need to print alias if it's same as CTE name (this would
			 * normally be the case, but not if set_rtable_names had to
			 * resolve a conflict).
			 */
			if (strcmp(refname, rte->ctename) != 0)
				printalias = true;
		}
		if (printalias)
			appendStringInfo(buf, " %s", quote_identifier(refname));

		/* Print the column definitions or aliases, if needed */
		if (rtfunc1 && rtfunc1->funccolnames != NIL)
		{
			/* Reconstruct the columndef list, which is also the aliases */
			get_from_clause_coldeflist(rtfunc1, colinfo, context);
		}
		else
		{
			/* Else print column aliases as needed */
			get_column_alias_list(colinfo, context);
		}

		/* Tablesample clause must go after any alias */
		if (rte->rtekind == RTE_RELATION && rte->tablesample)
			get_tablesample_def(rte->tablesample, context);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		deparse_columns *colinfo = deparse_columns_fetch(j->rtindex, dpns);
		bool		need_paren_on_right;

		need_paren_on_right = PRETTY_PAREN(context) &&
			!IsA(j->rarg, RangeTblRef) &&
			!(IsA(j->rarg, JoinExpr) &&((JoinExpr *) j->rarg)->alias != NULL);

		if (!PRETTY_PAREN(context) || j->alias != NULL)
			appendStringInfoChar(buf, '(');

		get_from_clause_item(j->larg, query, context);

		switch (j->jointype)
		{
			case JOIN_INNER:
				if (j->quals)
					appendContextKeyword(context, " JOIN ",
										 -PRETTYINDENT_STD,
										 PRETTYINDENT_STD,
										 PRETTYINDENT_JOIN);
				else
					appendContextKeyword(context, " CROSS JOIN ",
										 -PRETTYINDENT_STD,
										 PRETTYINDENT_STD,
										 PRETTYINDENT_JOIN);
				break;
			case JOIN_LEFT:
				appendContextKeyword(context, " LEFT JOIN ",
									 -PRETTYINDENT_STD,
									 PRETTYINDENT_STD,
									 PRETTYINDENT_JOIN);
				break;
			case JOIN_FULL:
				appendContextKeyword(context, " FULL JOIN ",
									 -PRETTYINDENT_STD,
									 PRETTYINDENT_STD,
									 PRETTYINDENT_JOIN);
				break;
			case JOIN_RIGHT:
				appendContextKeyword(context, " RIGHT JOIN ",
									 -PRETTYINDENT_STD,
									 PRETTYINDENT_STD,
									 PRETTYINDENT_JOIN);
				break;
			default:
				elog(ERROR, "unrecognized join type: %d",
					 (int) j->jointype);
		}

		if (need_paren_on_right)
			appendStringInfoChar(buf, '(');
		get_from_clause_item(j->rarg, query, context);
		if (need_paren_on_right)
			appendStringInfoChar(buf, ')');

		if (j->usingClause)
		{
			ListCell   *lc;
			bool		first = true;

			appendStringInfoString(buf, " USING (");
			/* Use the assigned names, not what's in usingClause */
			foreach(lc, colinfo->usingNames)
			{
				char	   *colname = (char *) lfirst(lc);

				if (first)
					first = false;
				else
					appendStringInfoString(buf, ", ");
				appendStringInfoString(buf, quote_identifier(colname));
			}
			appendStringInfoChar(buf, ')');
		}
		else if (j->quals)
		{
			appendStringInfoString(buf, " ON ");
			if (!PRETTY_PAREN(context))
				appendStringInfoChar(buf, '(');
			get_rule_expr(j->quals, context, false);
			if (!PRETTY_PAREN(context))
				appendStringInfoChar(buf, ')');
		}

		if (!PRETTY_PAREN(context) || j->alias != NULL)
			appendStringInfoChar(buf, ')');

		/* Yes, it's correct to put alias after the right paren ... */
		if (j->alias != NULL)
		{
			appendStringInfo(buf, " %s",
							 quote_identifier(j->alias->aliasname));
			get_column_alias_list(colinfo, context);
		}
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * get_column_alias_list - print column alias list for an RTE
 *
 * Caller must already have printed the relation's alias name.
 */
static void
get_column_alias_list(deparse_columns *colinfo, deparse_context *context)
{
	StringInfo	buf = context->buf;
	int			i;
	bool		first = true;

	/* Don't print aliases if not needed */
	if (!colinfo->printaliases)
		return;

	for (i = 0; i < colinfo->num_new_cols; i++)
	{
		char	   *colname = colinfo->new_colnames[i];

		if (first)
		{
			appendStringInfoChar(buf, '(');
			first = false;
		}
		else
			appendStringInfoString(buf, ", ");
		appendStringInfoString(buf, quote_identifier(colname));
	}
	if (!first)
		appendStringInfoChar(buf, ')');
}

/*
 * get_from_clause_coldeflist - reproduce FROM clause coldeflist
 *
 * When printing a top-level coldeflist (which is syntactically also the
 * relation's column alias list), use column names from colinfo.  But when
 * printing a coldeflist embedded inside ROWS FROM(), we prefer to use the
 * original coldeflist's names, which are available in rtfunc->funccolnames.
 * Pass NULL for colinfo to select the latter behavior.
 *
 * The coldeflist is appended immediately (no space) to buf.  Caller is
 * responsible for ensuring that an alias or AS is present before it.
 */
static void
get_from_clause_coldeflist(RangeTblFunction *rtfunc,
						   deparse_columns *colinfo,
						   deparse_context *context)
{
	StringInfo	buf = context->buf;
	ListCell   *l1;
	ListCell   *l2;
	ListCell   *l3;
	ListCell   *l4;
	int			i;

	appendStringInfoChar(buf, '(');

	/* there's no forfour(), so must chase one list the hard way */
	i = 0;
	l4 = list_head(rtfunc->funccolnames);
	forthree(l1, rtfunc->funccoltypes,
			 l2, rtfunc->funccoltypmods,
			 l3, rtfunc->funccolcollations)
	{
		Oid			atttypid = lfirst_oid(l1);
		int32		atttypmod = lfirst_int(l2);
		Oid			attcollation = lfirst_oid(l3);
		char	   *attname;

		if (colinfo)
			attname = colinfo->colnames[i];
		else
			attname = strVal(lfirst(l4));

		Assert(attname);		/* shouldn't be any dropped columns here */

		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%s %s",
						 quote_identifier(attname),
						 format_type_with_typemod(atttypid, atttypmod));
		if (OidIsValid(attcollation) &&
			attcollation != get_typcollation(atttypid))
			appendStringInfo(buf, " COLLATE %s",
							 generate_collation_name(attcollation));

		l4 = lnext(l4);
		i++;
	}

	appendStringInfoChar(buf, ')');
}

/*
 * get_tablesample_def			- print a TableSampleClause
 */
static void
get_tablesample_def(TableSampleClause *tablesample, deparse_context *context)
{
	StringInfo	buf = context->buf;
	Oid			argtypes[1];
	int			nargs;
	ListCell   *l;

	/*
	 * We should qualify the handler's function name if it wouldn't be
	 * resolved by lookup in the current search path.
	 */
	argtypes[0] = INTERNALOID;
	appendStringInfo(buf, " TABLESAMPLE %s (",
					 generate_function_name(tablesample->tsmhandler, 1,
											NIL, argtypes,
											false, NULL, EXPR_KIND_NONE));

	nargs = 0;
	foreach(l, tablesample->args)
	{
		if (nargs++ > 0)
			appendStringInfoString(buf, ", ");
		get_rule_expr((Node *) lfirst(l), context, false);
	}
	appendStringInfoChar(buf, ')');

	if (tablesample->repeatable != NULL)
	{
		appendStringInfoString(buf, " REPEATABLE (");
		get_rule_expr((Node *) tablesample->repeatable, context, false);
		appendStringInfoChar(buf, ')');
	}
}

/*
 * get_opclass_name			- fetch name of an index operator class
 *
 * The opclass name is appended (after a space) to buf.
 *
 * Output is suppressed if the opclass is the default for the given
 * actual_datatype.  (If you don't want this behavior, just pass
 * InvalidOid for actual_datatype.)
 */
static void
get_opclass_name(Oid opclass, Oid actual_datatype,
				 StringInfo buf)
{
	HeapTuple	ht_opc;
	Form_pg_opclass opcrec;
	char	   *opcname;
	char	   *nspname;

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);

	if (!OidIsValid(actual_datatype) ||
		GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass)
	{
		/* Okay, we need the opclass name.  Do we need to qualify it? */
		opcname = NameStr(opcrec->opcname);
		if (OpclassIsVisible(opclass))
			appendStringInfo(buf, " %s", quote_identifier(opcname));
		else
		{
			nspname = get_namespace_name(opcrec->opcnamespace);
			appendStringInfo(buf, " %s.%s",
							 quote_identifier(nspname),
							 quote_identifier(opcname));
		}
	}
	ReleaseSysCache(ht_opc);
}

/*
 * processIndirection - take care of array and subfield assignment
 *
 * We strip any top-level FieldStore or assignment ArrayRef nodes that
 * appear in the input, printing them as decoration for the base column
 * name (which we assume the caller just printed).  Return the subexpression
 * that's to be assigned.
 */
static Node *
processIndirection(Node *node, deparse_context *context)
{
	StringInfo	buf = context->buf;

	for (;;)
	{
		if (node == NULL)
			break;
		if (IsA(node, FieldStore))
		{
			FieldStore *fstore = (FieldStore *) node;
			Oid			typrelid;
			char	   *fieldname;

			/* lookup tuple type */
			typrelid = get_typ_typrelid(fstore->resulttype);
			if (!OidIsValid(typrelid))
				elog(ERROR, "argument type %s of FieldStore is not a tuple type",
					 format_type_be(fstore->resulttype));

			/*
			 * Print the field name.  There should only be one target field in
			 * stored rules.  There could be more than that in executable
			 * target lists, but this function cannot be used for that case.
			 */
			Assert(list_length(fstore->fieldnums) == 1);
			fieldname = get_relid_attribute_name(typrelid,
											linitial_int(fstore->fieldnums));
			appendStringInfo(buf, ".%s", quote_identifier(fieldname));

			/*
			 * We ignore arg since it should be an uninteresting reference to
			 * the target column or subcolumn.
			 */
			node = (Node *) linitial(fstore->newvals);
		}
		else if (IsA(node, ArrayRef))
		{
			ArrayRef   *aref = (ArrayRef *) node;

			if (aref->refassgnexpr == NULL)
				break;
			printSubscripts(aref, context);

			/*
			 * We ignore refexpr since it should be an uninteresting reference
			 * to the target column or subcolumn.
			 */
			node = (Node *) aref->refassgnexpr;
		}
		else
			break;
	}

	return node;
}

static void
printSubscripts(ArrayRef *aref, deparse_context *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lowlist_item;
	ListCell   *uplist_item;

	lowlist_item = list_head(aref->reflowerindexpr);	/* could be NULL */
	foreach(uplist_item, aref->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');
		if (lowlist_item)
		{
			/* If subexpression is NULL, get_rule_expr prints nothing */
			get_rule_expr((Node *) lfirst(lowlist_item), context, false);
			appendStringInfoChar(buf, ':');
			lowlist_item = lnext(lowlist_item);
		}
		/* If subexpression is NULL, get_rule_expr prints nothing */
		get_rule_expr((Node *) lfirst(uplist_item), context, false);
		appendStringInfoChar(buf, ']');
	}
}

/*
 * quote_identifier			- Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char *
quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;

	/*
	 * would like to use <ctype.h> macros here, but they might yield unwanted
	 * locale-specific results...
	 */
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}

	if (quote_all_identifiers)
		safe = false;

	if (safe)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
		const ScanKeyword *keyword = ScanKeywordLookup(ident,
													   ScanKeywords,
													   NumScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			safe = false;
	}

	if (safe)
		return ident;			/* no change needed */

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';

	return result;
}

/*
 * quote_qualified_identifier	- Quote a possibly-qualified identifier
 *
 * Return a name of the form qualifier.ident, or just ident if qualifier
 * is NULL, quoting each component if necessary.  The result is palloc'd.
 */
char *
quote_qualified_identifier(const char *qualifier,
						   const char *ident)
{
	StringInfoData buf;

	initStringInfo(&buf);
	if (qualifier)
		appendStringInfo(&buf, "%s.", quote_identifier(qualifier));
	appendStringInfoString(&buf, quote_identifier(ident));
	return buf.data;
}

/*
 * get_relation_name
 *		Get the unqualified name of a relation specified by OID
 *
 * This differs from the underlying get_rel_name() function in that it will
 * throw error instead of silently returning NULL if the OID is bad.
 */
static char *
get_relation_name(Oid relid)
{
	char	   *relname = get_rel_name(relid);

	if (!relname)
		elog(ERROR, "cache lookup failed for relation %u", relid);
	return relname;
}

/*
 * generate_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 *
 * If namespaces isn't NIL, it must be a list of deparse_namespace nodes.
 * We will forcibly qualify the relation name if it equals any CTE name
 * visible in the namespace list.
 */
static char *
generate_relation_name(Oid relid, List *namespaces)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	bool		need_qual;
	ListCell   *nslist;
	char	   *relname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	/* Check for conflicting CTE name */
	need_qual = false;
	foreach(nslist, namespaces)
	{
		deparse_namespace *dpns = (deparse_namespace *) lfirst(nslist);
		ListCell   *ctlist;

		foreach(ctlist, dpns->ctes)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(ctlist);

			if (strcmp(cte->ctename, relname) == 0)
			{
				need_qual = true;
				break;
			}
		}
		if (need_qual)
			break;
	}

	/* Otherwise, qualify the name if not visible in search path */
	if (!need_qual)
		need_qual = !RelationIsVisible(relid);

	if (need_qual)
		nspname = get_namespace_name(reltup->relnamespace);
	else
		nspname = NULL;

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}

/*
 * generate_qualified_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * As above, but unconditionally schema-qualify the name.
 */
static char *
generate_qualified_relation_name(Oid relid)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *relname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);
	if (!nspname)
		elog(ERROR, "cache lookup failed for namespace %u",
			 reltup->relnamespace);

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}

/*
 * generate_function_name
 *		Compute the name to display for a function specified by OID,
 *		given that it is being called with the specified actual arg names and
 *		types.  (Those matter because of ambiguous-function resolution rules.)
 *
 * If we're dealing with a potentially variadic function (in practice, this
 * means a FuncExpr or Aggref, not some other way of calling a function), then
 * has_variadic must specify whether variadic arguments have been merged,
 * and *use_variadic_p will be set to indicate whether to print VARIADIC in
 * the output.  For non-FuncExpr cases, has_variadic should be FALSE and
 * use_variadic_p can be NULL.
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
static char *
generate_function_name(Oid funcid, int nargs, List *argnames, Oid *argtypes,
					   bool has_variadic, bool *use_variadic_p,
					   ParseExprKind special_exprkind)
{
	char	   *result;
	HeapTuple	proctup;
	Form_pg_proc procform;
	char	   *proname;
	bool		use_variadic;
	char	   *nspname;
	FuncDetailCode p_result;
	Oid			p_funcid;
	Oid			p_rettype;
	bool		p_retset;
	int			p_nvargs;
	Oid			p_vatype;
	Oid		   *p_true_typeids;
	bool		force_qualify = false;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);
	proname = NameStr(procform->proname);

	/*
	 * Due to parser hacks to avoid needing to reserve CUBE, we need to force
	 * qualification in some special cases.
	 */
	if (special_exprkind == EXPR_KIND_GROUP_BY)
	{
		if (strcmp(proname, "cube") == 0 || strcmp(proname, "rollup") == 0)
			force_qualify = true;
	}

	/*
	 * Determine whether VARIADIC should be printed.  We must do this first
	 * since it affects the lookup rules in func_get_detail().
	 *
	 * Currently, we always print VARIADIC if the function has a merged
	 * variadic-array argument.  Note that this is always the case for
	 * functions taking a VARIADIC argument type other than VARIADIC ANY.
	 *
	 * In principle, if VARIADIC wasn't originally specified and the array
	 * actual argument is deconstructable, we could print the array elements
	 * separately and not print VARIADIC, thus more nearly reproducing the
	 * original input.  For the moment that seems like too much complication
	 * for the benefit, and anyway we do not know whether VARIADIC was
	 * originally specified if it's a non-ANY type.
	 */
	if (use_variadic_p)
	{
		/* Parser should not have set funcvariadic unless fn is variadic */
		Assert(!has_variadic || OidIsValid(procform->provariadic));
		use_variadic = has_variadic;
		*use_variadic_p = use_variadic;
	}
	else
	{
		Assert(!has_variadic);
		use_variadic = false;
	}

	/*
	 * The idea here is to schema-qualify only if the parser would fail to
	 * resolve the correct function given the unqualified func name with the
	 * specified argtypes and VARIADIC flag.  But if we already decided to
	 * force qualification, then we can skip the lookup and pretend we didn't
	 * find it.
	 */
	if (!force_qualify)
		p_result = func_get_detail(list_make1(makeString(proname)),
								   NIL, argnames, nargs, argtypes,
								   !use_variadic, true,
								   &p_funcid, &p_rettype,
								   &p_retset, &p_nvargs, &p_vatype,
								   &p_true_typeids, NULL);
	else
	{
		p_result = FUNCDETAIL_NOTFOUND;
		p_funcid = InvalidOid;
	}

	if ((p_result == FUNCDETAIL_NORMAL ||
		 p_result == FUNCDETAIL_AGGREGATE ||
		 p_result == FUNCDETAIL_WINDOWFUNC) &&
		p_funcid == funcid)
		nspname = NULL;
	else
		nspname = get_namespace_name(procform->pronamespace);

	result = quote_qualified_identifier(nspname, proname);

	ReleaseSysCache(proctup);

	return result;
}

/*
 * generate_operator_name
 *		Compute the name to display for an operator specified by OID,
 *		given that it is being called with the specified actual arg types.
 *		(Arg types matter because of ambiguous-operator resolution rules.
 *		Pass InvalidOid for unused arg of a unary operator.)
 *
 * The result includes all necessary quoting and schema-prefixing,
 * plus the OPERATOR() decoration needed to use a qualified operator name
 * in an expression.
 */
static char *
generate_operator_name(Oid operid, Oid arg1, Oid arg2)
{
	StringInfoData buf;
	HeapTuple	opertup;
	Form_pg_operator operform;
	char	   *oprname;
	char	   *nspname;
	Operator	p_result;

	initStringInfo(&buf);

	opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
	if (!HeapTupleIsValid(opertup))
		elog(ERROR, "cache lookup failed for operator %u", operid);
	operform = (Form_pg_operator) GETSTRUCT(opertup);
	oprname = NameStr(operform->oprname);

	/*
	 * The idea here is to schema-qualify only if the parser would fail to
	 * resolve the correct operator given the unqualified op name with the
	 * specified argtypes.
	 */
	switch (operform->oprkind)
	{
		case 'b':
			p_result = oper(NULL, list_make1(makeString(oprname)), arg1, arg2,
							true, -1);
			break;
		case 'l':
			p_result = left_oper(NULL, list_make1(makeString(oprname)), arg2,
								 true, -1);
			break;
		case 'r':
			p_result = right_oper(NULL, list_make1(makeString(oprname)), arg1,
								  true, -1);
			break;
		default:
			elog(ERROR, "unrecognized oprkind: %d", operform->oprkind);
			p_result = NULL;	/* keep compiler quiet */
			break;
	}

	if (p_result != NULL && oprid(p_result) == operid)
		nspname = NULL;
	else
	{
		nspname = get_namespace_name(operform->oprnamespace);
		appendStringInfo(&buf, "OPERATOR(%s.", quote_identifier(nspname));
	}

	appendStringInfoString(&buf, oprname);

	if (nspname)
		appendStringInfoChar(&buf, ')');

	if (p_result != NULL)
		ReleaseSysCache(p_result);

	ReleaseSysCache(opertup);

	return buf.data;
}

/*
 * generate_collation_name
 *		Compute the name to display for a collation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
char *
generate_collation_name(Oid collid)
{
	HeapTuple	tp;
	Form_pg_collation colltup;
	char	   *collname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for collation %u", collid);
	colltup = (Form_pg_collation) GETSTRUCT(tp);
	collname = NameStr(colltup->collname);

	if (!CollationIsVisible(collid))
		nspname = get_namespace_name(colltup->collnamespace);
	else
		nspname = NULL;

	result = quote_qualified_identifier(nspname, collname);

	ReleaseSysCache(tp);

	return result;
}

/*
 * Given a C string, produce a TEXT datum.
 *
 * We assume that the input was palloc'd and may be freed.
 */
static text *
string_to_text(char *str)
{
	text	   *result;

	result = cstring_to_text(str);
	pfree(str);
	return result;
}

/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */
static char *
flatten_reloptions(Oid relid)
{
	char	   *result = NULL;
	HeapTuple	tuple;
	Datum		reloptions;
	bool		isnull;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	reloptions = SysCacheGetAttr(RELOID, tuple,
								 Anum_pg_class_reloptions, &isnull);
	if (!isnull)
	{
		StringInfoData buf;
		Datum	   *options;
		int			noptions;
		int			i;

		initStringInfo(&buf);

		deconstruct_array(DatumGetArrayTypeP(reloptions),
						  TEXTOID, -1, false, 'i',
						  &options, NULL, &noptions);

		for (i = 0; i < noptions; i++)
		{
			char	   *option = TextDatumGetCString(options[i]);
			char	   *name;
			char	   *separator;
			char	   *value;

			/*
			 * Each array element should have the form name=value.  If the "="
			 * is missing for some reason, treat it like an empty value.
			 */
			name = option;
			separator = strchr(option, '=');
			if (separator)
			{
				*separator = '\0';
				value = separator + 1;
			}
			else
				value = "";

			if (i > 0)
				appendStringInfoString(&buf, ", ");
			appendStringInfo(&buf, "%s=", quote_identifier(name));

			/*
			 * In general we need to quote the value; but to avoid unnecessary
			 * clutter, do not quote if it is an identifier that would not
			 * need quoting.  (We could also allow numbers, but that is a bit
			 * trickier than it looks --- for example, are leading zeroes
			 * significant?  We don't want to assume very much here about what
			 * custom reloptions might mean.)
			 */
			if (quote_identifier(value) == value)
				appendStringInfoString(&buf, value);
			else
				simple_quote_literal(&buf, value);

			pfree(option);
		}

		result = buf.data;
	}

	ReleaseSysCache(tuple);

	return result;
}
