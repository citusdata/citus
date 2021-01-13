#include "postgres.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "commands/view.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/pg_cimv.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/security_utils.h"
#include "distributed/sequence_utils.h"
#include "distributed/coordinator_protocol.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/cimv.h"

#define PARTIAL_AGG_FNNAME "partial_agg"
#define COMBINE_AGG_FNNAME "combine_agg"
#define FINALIZE_AGG_FNNAME "finalize_agg"

typedef struct
{
	bool isCimv;
	bool insertOnlyCapture;
	char *schedule;
	List *storageOptions;
} MatViewCreateOptions;

typedef struct
{
	const CreateTableAsStmt *stmt;
	MatViewCreateOptions *createOptions;
	Form_pg_cimv formCimv;
	RangeVar *baseTableName;
	RangeVar *matTableName;
	RangeVar *landingTableName;
	RangeVar *userViewName;
	RangeVar *refreshViewName;
	RangeVar *baseTableNameQuoted;
	RangeVar *matTableNameQuoted;
	RangeVar *landingTableNameQuoted;
	RangeVar *userViewNameQuoted;
	RangeVar *refreshViewNameQuoted;
	List *targetListEntries;
	List *groupTargetListEntries;
	List *aggTargetListEntries;
	CitusTableCacheEntry *citusTable;
	TargetEntry *partitionColumn;
	bool supportsDelete;
	char* prefix;
	int prefixId;
} CimvCreate;

static void CreateCimv(CimvCreate *cimvCreate);
static void CreateMatTable(CimvCreate *cimvCreate, bool isLandingZone);
static void CreateIndexOnMatTable(CimvCreate *cimvCreate);
static void DistributeTable(CimvCreate *cimvCreate, RangeVar *tableName);
static void CreateUserView(CimvCreate *cimvCreate);
static void CreateRefreshView(CimvCreate *cimvCreate);
static void CreateDataChangeTriggerFunction(CimvCreate *cimvCreate);
static void CreateCronJob(CimvCreate *cimvCreate);
static void DataChangeTriggerFunctionAppendInsertDelete(CimvCreate *cimvCreate, StringInfo
														buf, bool isInsert);
static void DataChangeTriggerFunctionAppendErrorOnDelete(CimvCreate *cimvCreate,
														 StringInfo buf);
static void AppendOnConflict(CimvCreate *cimvCreate, StringInfo buf, bool isInsert);
static void CreateDataChangeTriggers(CimvCreate *cimvCreate);
static void CreateDataChangeTrigger(CimvCreate *cimvCreate, int triggerEvent);
static bool ValidateCimv(CimvCreate *cimvCreate);

static void ValidateAgg(Aggref *agg, bool *supportsDelete);
static void AddCountAgg(Query *query, bool isInsert);
static Node * PartializeAggs(Node *node, void *context);
static CimvCreate * InitializeCimvCreate(const CreateTableAsStmt *stmt,
										 MatViewCreateOptions *createOptions);
static MatViewCreateOptions * GetMatViewCreateOptions(const CreateTableAsStmt *stmt);

static ObjectAddress DefineVirtualRelation(RangeVar *relation, List *tlist,
										   Query *viewParse);
static Oid CitusFunctionOidWithSignature(char *functionName, int numargs, Oid *argtypes);
static Oid PartialAggOid(void);
static void AppendQuotedLiteral(StringInfo buf, const char *val);
static void AppendStringInfoFunction(StringInfo buf, Oid fnoid);
static Oid AggregateFunctionOid(const char *functionName, Oid inputType);
static char* CIMVTriggerFuncName(int prefixId, const char* relname);
static char* CIMVInternalPrefix(const RangeVar* baseTable, int prefixId);
static void AlterTableOwner(RangeVar* tableName, char* ownerName);


extern Datum trigf(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(trigf);

Datum
trigf(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger* trigger = trigdata->tg_trigger;
    TupleDesc   tupdesc;
    HeapTuple   rettuple;
    char       *when;
    bool        checknull = false;
    bool        isnull;
    int         ret, i;


    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "trigf: not called by trigger manager");

    /* tuple to return to executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        rettuple = trigdata->tg_newtuple;
    else
        rettuple = trigdata->tg_trigtuple;

	if (TRIGGER_FOR_INSERT(trigger->tgtype)) {

	}

    /* check for null values */
    if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)
        && TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        checknull = true;

    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        when = "before";
    else
        when = "after ";

    tupdesc = trigdata->tg_relation->rd_att;

    /* connect to SPI manager */
    if ((ret = SPI_connect()) < 0)
        elog(INFO, "trigf (fired %s): SPI_connect returned %d", when, ret);

    /* get number of rows in table */
    ret = SPI_exec("SELECT count(*) FROM ttest", 0);

    if (ret < 0)
        elog(NOTICE, "trigf (fired %s): SPI_exec returned %d", when, ret);

    /* count(*) returns int8, so be careful to convert */
    i = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc,
                                    1,
                                    &isnull));

    elog (INFO, "trigf (fired %s): there are %d rows in ttest", when, i);

    SPI_finish();

    if (checknull)
    {
        SPI_getbinval(rettuple, tupdesc, 1, &isnull);
        if (isnull)
            rettuple = NULL;
    }

    return PointerGetDatum(rettuple);
}


bool
ProcessCreateMaterializedViewStmt(const CreateTableAsStmt *stmt, const char *query_string,
								  PlannedStmt *pstmt)
{
	bool stmtHandled = false;
	CimvCreate *cimvCreate = NULL;

	if (stmt->relkind != OBJECT_MATVIEW)
	{
		return stmtHandled;
	}

	MatViewCreateOptions *options = GetMatViewCreateOptions(stmt);

	if (options->isCimv)
	{
		cimvCreate = InitializeCimvCreate(stmt, options);

		ValidateCimv(cimvCreate);
		CreateCimv(cimvCreate);
		stmtHandled = true;
	}

	if (options != NULL)
	{
		pfree(options);
	}
	if (cimvCreate != NULL)
	{
		pfree(cimvCreate);
	}

	return stmtHandled;
}


static void
CreateCimv(CimvCreate *cimvCreate)
{
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	CreateMatTable(cimvCreate, false);

	if (cimvCreate->createOptions->schedule != NULL)
	{
		CreateMatTable(cimvCreate, true);
		CreateCronJob(cimvCreate);
	}

	CreateUserView(cimvCreate);
	CreateRefreshView(cimvCreate);
	CreateDataChangeTriggerFunction(cimvCreate);
	CreateDataChangeTriggers(cimvCreate);
	InsertIntoPgCimv(cimvCreate->formCimv);

	if (SPI_finish() != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed");
	}

	if (!cimvCreate->stmt->into->skipData)
	{
		RefreshCimv(cimvCreate->formCimv, cimvCreate->stmt->into->skipData, true);
	}

}


static void
CreateMatTable(CimvCreate *cimvCreate, bool isLandingZone)
{
	RangeVar *table = isLandingZone ? cimvCreate->landingTableName :
					  cimvCreate->matTableName;

	/* postgres/src/backend/commands/createas.c create_ctas_internal*/
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	CreateStmt *create = makeNode(CreateStmt);
	create->relation = table;
	create->tableElts = NIL;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = cimvCreate->createOptions->storageOptions;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = cimvCreate->stmt->into->tableSpaceName;
	create->if_not_exists = false;
#if PG_VERSION_NUM >= PG_VERSION_12
	create->accessMethod = cimvCreate->stmt->into->accessMethod;
#endif

	ColumnDef *col;
	TargetEntry *tle;
	foreach_ptr(tle, cimvCreate->targetListEntries)
	{
		if (IsA(tle->expr, Aggref))
		{
			col = makeColumnDef(tle->resname,
								BYTEAOID,
								-1,
								InvalidOid);
			create->tableElts = lappend(create->tableElts, col);
		}
		else
		{
			col = makeColumnDef(tle->resname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));
			create->tableElts = lappend(create->tableElts, col);
		}
	}

	col = makeColumnDef("__count__",
						INT8OID,
						-1,
						InvalidOid);
	create->tableElts = lappend(create->tableElts, col);

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 */
	Oid tableOid = DefineRelation(create, RELKIND_RELATION, GetUserId(), NULL,
								  NULL).objectId;

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(tableOid, toast_options);

	if (isLandingZone)
	{
		cimvCreate->formCimv->landingtable = tableOid;
	}
	else
	{
		cimvCreate->formCimv->mattable = tableOid;
		CreateIndexOnMatTable(cimvCreate);
	}

	if (cimvCreate->citusTable != NULL)
	{
		DistributeTable(cimvCreate, table);
	}
}


static void
CreateIndexOnMatTable(CimvCreate *cimvCreate)
{
	StringInfo indexName = makeStringInfo();
	appendStringInfo(indexName, "%s_uidx", cimvCreate->matTableName->relname);

	IndexStmt *indexcreate = makeNode(IndexStmt);
	indexcreate->isconstraint = true;
	indexcreate->unique = true;
	indexcreate->accessMethod = DEFAULT_INDEX_TYPE;
	indexcreate->idxname = indexName->data;
	indexcreate->relation = cimvCreate->matTableName;
	indexcreate->tableSpace = cimvCreate->stmt->into->tableSpaceName;
	indexcreate->indexParams = NIL;
	indexcreate->indexIncludingParams = NIL;

	TargetEntry *tle;
	foreach_ptr(tle, cimvCreate->groupTargetListEntries)
	{
		IndexElem *iparam = makeNode(IndexElem);
		iparam->name = tle->resname;
		iparam->indexcolname = NULL;
		iparam->collation = NIL;
		iparam->opclass = NIL;
		iparam->ordering = SORTBY_DEFAULT;
		iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
		indexcreate->indexParams = lappend(indexcreate->indexParams, iparam);
	}

	DefineIndex(cimvCreate->formCimv->mattable,
				indexcreate,
				InvalidOid, /* indexRelationId */
				InvalidOid, /* parentIndexId */
				InvalidOid, /* parentConstraintId */
				false,    /* is_alter_table */
				false,    /* check_rights */
				false,    /* check_not_in_use */
				false,    /* skip_build */
				false);  /* quiet */

	IndexElem *ie;
	foreach_ptr(ie, indexcreate->indexParams)
	{
		pfree(ie);
	}
	pfree(indexcreate);
	pfree(indexName->data);
}


static void
DistributeTable(CimvCreate *cimvCreate, RangeVar *tableName)
{
	StringInfoData querybuf;
	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					 "SELECT * FROM create_distributed_table($param$%s.%s$param$, $param$%s$param$, colocate_with => $param$%s.%s$param$);",
					 tableName->schemaname,
					 tableName->relname,
					 cimvCreate->partitionColumn->resname,
					 cimvCreate->baseTableName->schemaname,
					 cimvCreate->baseTableName->relname);

	if (SPI_execute(querybuf.data, false, 0) != SPI_OK_SELECT)
	{
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	pfree(querybuf.data);
}

static void AlterTableOwner(RangeVar* tableName, char* ownerName) {
	StringInfoData querybuf;
	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					 "ALTER TABLE %s.%s OWNER TO %s;",
					 tableName->schemaname ? tableName->schemaname : "public",
					 tableName->relname,
					 ownerName);

	if (SPI_execute(querybuf.data, false, 0) != SPI_OK_UTILITY)
	{
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	pfree(querybuf.data);
}


static void
CreateUserView(CimvCreate *cimvCreate)
{
	StringInfoData queryText;
	initStringInfo(&queryText);

	bool unionWithLandingTable = cimvCreate->createOptions->schedule != NULL;

	appendStringInfoString(&queryText, "SELECT ");

	bool isFirst = true;
	TargetEntry *tle;
	foreach_ptr(tle, cimvCreate->targetListEntries)
	{
		if (isFirst)
		{
			isFirst = false;
		}
		else
		{
			appendStringInfoString(&queryText, ", ");
		}

		const char *colName = quote_identifier(tle->resname);
		if (IsA(tle->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, tle->expr);
			appendStringInfo(&queryText, "%s(", FINALIZE_AGG_FNNAME);
			AppendStringInfoFunction(&queryText, aggref->aggfnoid);
			appendStringInfo(&queryText, ", %s, null::%s) AS %s",
							 colName,
							 format_type_with_typemod(aggref->aggtype, exprTypmod(
														  (Node *) aggref)),
							 colName);
		}
		else
		{
			appendStringInfo(&queryText, "%s", colName);
		}
	}

	if (unionWithLandingTable)
	{
		appendStringInfo(&queryText,
						 " FROM (SELECT * FROM %s.%s UNION ALL SELECT * FROM %s.%s) __union__",
						 cimvCreate->matTableNameQuoted->schemaname,
						 cimvCreate->matTableNameQuoted->relname,
						 cimvCreate->landingTableNameQuoted->schemaname,
						 cimvCreate->landingTableNameQuoted->relname);
	}
	else
	{
		appendStringInfo(&queryText, " FROM %s.%s",
						 cimvCreate->matTableNameQuoted->schemaname,
						 cimvCreate->matTableNameQuoted->relname);
	}

	appendStringInfoString(&queryText, " GROUP BY ");

	isFirst = true;
	foreach_ptr(tle, cimvCreate->groupTargetListEntries)
	{
		if (isFirst)
		{
			isFirst = false;
		}
		else
		{
			appendStringInfoString(&queryText, ", ");
		}
		appendStringInfo(&queryText, "%s", quote_identifier(tle->resname));
	}

	appendStringInfoString(&queryText, " HAVING sum(__count__) > 0");

	List *rawStmts = raw_parser(queryText.data);
	Assert(list_length(rawStmts) == 1);
	RawStmt *rawStmt = linitial(rawStmts);
	Query *query = parse_analyze(rawStmt, queryText.data, InvalidOid, 0, NULL);

	cimvCreate->formCimv->userview = DefineVirtualRelation(cimvCreate->userViewName,
														   query->targetList,
														   query).objectId;
	pfree(queryText.data);
}


static void
CreateRefreshView(CimvCreate *cimvCreate)
{
	Query *query = (Query *) copyObject(cimvCreate->stmt->into->viewQuery);
	int inverse = 0;
	query->targetList = (List *) PartializeAggs((Node *) query->targetList, &inverse);
	AddCountAgg(query, true);

	StringInfoData querybuf;
	initStringInfo(&querybuf);

	pg_get_query_def(query, &querybuf);
	List *rawStmts = raw_parser(querybuf.data);
	Assert(list_length(rawStmts) == 1);
	RawStmt *rawStmt = linitial(rawStmts);
	pfree(query);
	query = parse_analyze(rawStmt, querybuf.data, InvalidOid, 0, NULL);
	cimvCreate->formCimv->refreshview = DefineVirtualRelation(cimvCreate->refreshViewName,
															  query->targetList,
															  query).objectId;

	pfree(query);
	pfree(querybuf.data);
}


static void
CreateCronJob(CimvCreate *cimvCreate)
{
	StringInfoData queryText;
	initStringInfo(&queryText);

	appendStringInfo(&queryText,
					 "SELECT cron.schedule($cron_schedule$%s$cron_schedule$, $cron_schedule$",
					 cimvCreate->createOptions->schedule);

	appendStringInfo(&queryText, "WITH __del__ AS (DELETE FROM %s.%s RETURNING *) ",
					 cimvCreate->landingTableNameQuoted->schemaname,
					 cimvCreate->landingTableNameQuoted->relname);

	appendStringInfo(&queryText, "INSERT INTO %s.%s AS __mat__ ",
					 cimvCreate->matTableNameQuoted->schemaname,
					 cimvCreate->matTableNameQuoted->relname);

	appendStringInfoString(&queryText, "SELECT ");

	bool isFirst = true;
	TargetEntry *tle;
	foreach_ptr(tle, cimvCreate->targetListEntries)
	{
		if (isFirst)
		{
			isFirst = false;
		}
		else
		{
			appendStringInfoString(&queryText, ", ");
		}


		const char *colname = quote_identifier(tle->resname);
		if (IsA(tle->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, tle->expr);
			appendStringInfo(&queryText, "%s(", COMBINE_AGG_FNNAME);
			AppendStringInfoFunction(&queryText, aggref->aggfnoid);
			appendStringInfo(&queryText, ", %s) AS %s", colname, colname);
		}
		else
		{
			appendStringInfo(&queryText, "%s", colname);
		}
	}

	appendStringInfo(&queryText, ", sum(__count__) AS __count__ FROM __del__ GROUP BY ");

	isFirst = true;
	foreach_ptr(tle, cimvCreate->groupTargetListEntries)
	{
		if (isFirst)
		{
			isFirst = false;
		}
		else
		{
			appendStringInfoString(&queryText, ", ");
		}
		appendStringInfo(&queryText, "%s", quote_identifier(tle->resname));
	}

	AppendOnConflict(cimvCreate, &queryText, true);

	appendStringInfoString(&queryText, ";$cron_schedule$);"); /* TODO: delete where count = 0 */

	/* CREATE */
	if (SPI_execute(queryText.data, false, 0) != SPI_OK_SELECT)
	{
		elog(ERROR, "SPI_exec failed: %s", queryText.data);
	}

	if (SPI_processed != 1 || SPI_tuptable->tupdesc->natts != 1)
	{
		elog(ERROR, "Error creating schedule");
	}

	bool isNull;
	cimvCreate->formCimv->jobid =
		DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc,
									1,
									&isNull));

	if (isNull || cimvCreate->formCimv->jobid < 1)
	{
		elog(ERROR, "Error creating schedule");
	}

	pfree(queryText.data);
}


static void
CreateDataChangeTriggerFunction(CimvCreate *cimvCreate)
{
	StringInfoData buf;
	initStringInfo(&buf);

	bool isCitusTable = cimvCreate->citusTable != NULL;

	if (isCitusTable)
	{
		appendStringInfo(&buf,
						 "SELECT * FROM run_command_on_workers($cmd$CREATE FUNCTION %s.%s() RETURNS trigger AS $$ BEGIN RETURN null; END; $$ LANGUAGE plpgsql$cmd$)",
						 quote_identifier(NameStr(
											  cimvCreate->formCimv->triggerfnnamespace)),
						 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnname)));

		if (SPI_execute(buf.data, false, 0) != SPI_OK_SELECT)
		{
			elog(ERROR, "SPI_exec failed: %s", buf.data);
		}

		resetStringInfo(&buf);
	}

	appendStringInfo(&buf,
					 "CREATE OR REPLACE FUNCTION %s.%s() RETURNS TRIGGER AS $trigger_function$ BEGIN\n",
					 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnnamespace)),
					 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnname)));

	/* INSERT */
	appendStringInfoString(&buf,
						   "IF (TG_OP = $inside_trigger_function$INSERT$inside_trigger_function$ OR TG_OP = $inside_trigger_function$UPDATE$inside_trigger_function$) THEN\n");
	DataChangeTriggerFunctionAppendInsertDelete(cimvCreate, &buf, true);
	appendStringInfoString(&buf, "END IF;\n");

	/* DELETE */
	appendStringInfoString(&buf,
						   "IF (TG_OP = $inside_trigger_function$DELETE$inside_trigger_function$ OR TG_OP = $inside_trigger_function$UPDATE$inside_trigger_function$) THEN\n");
	if (cimvCreate->supportsDelete)
	{
		DataChangeTriggerFunctionAppendInsertDelete(cimvCreate, &buf, false);
	}
	else
	{
		DataChangeTriggerFunctionAppendErrorOnDelete(cimvCreate, &buf);
	}

	appendStringInfoString(&buf, "END IF;\n");

	/* TRUNCATE */
	appendStringInfoString(&buf,
						   "IF (TG_OP = $inside_trigger_function$TRUNCATE$inside_trigger_function$) THEN\n");

	appendStringInfoString(&buf,
						   "EXECUTE format($exec_format$TRUNCATE TABLE %s; $exec_format$, TG_ARGV[0]);");

	/* TODO: also truncate landing table if it exists */

	appendStringInfoString(&buf, "END IF;\n");

	appendStringInfoString(&buf,
						   "RETURN NULL; END; $trigger_function$ LANGUAGE plpgsql;");

	/* CREATE */
	if (SPI_execute(buf.data, false, 0) != SPI_OK_UTILITY)
	{
		elog(ERROR, "SPI_exec failed: %s", buf.data);
	}

	if (isCitusTable)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT FROM create_distributed_function($cdfn$%s.%s()$cdfn$)",
						 quote_identifier(NameStr(
											  cimvCreate->formCimv->triggerfnnamespace)),
						 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnname)));
	}

	int expectedResult = isCitusTable ? SPI_OK_SELECT : SPI_OK_UTILITY;
	if (SPI_execute(buf.data, false, 0) != expectedResult)
	{
		elog(ERROR, "SPI_exec failed: %s", buf.data);
	}

	pfree(buf.data);
}


static void
DataChangeTriggerFunctionAppendInsertDelete(CimvCreate *cimvCreate, StringInfo buf, bool
											isInsert)
{
	StringInfoData querybuf;
	initStringInfo(&querybuf);

	Query *query = (Query *) copyObject(cimvCreate->stmt->into->viewQuery);
	int inverse = isInsert ? 0 : 1;
	query->targetList = (List *) PartializeAggs((Node *) query->targetList, &inverse);
	AddCountAgg(query, isInsert);

	RangeTblEntry *baseRte = (RangeTblEntry *) linitial(query->rtable);
	baseRte->rtekind = RTE_CTE;
	baseRte->ctename = isInsert ? "__ins__" : "__del__";

	pg_get_query_def(query, &querybuf);

	appendStringInfoString(buf, "EXECUTE format($exec_format$INSERT INTO %s AS __mat__ ");

	/* SELECT */
	appendStringInfoString(buf, querybuf.data);

	/* ON CONFLICT */
	if (cimvCreate->createOptions->schedule == NULL)
	{
		AppendOnConflict(cimvCreate, buf, isInsert);
	}

	appendStringInfoString(buf, ";\n$exec_format$, TG_ARGV[0]);");

	if (!isInsert && cimvCreate->createOptions->schedule == NULL)
	{
		appendStringInfoString(buf,
							   "EXECUTE format($exec_format$DELETE FROM %s WHERE __count__ = 0;$exec_format$, TG_ARGV[0]);");
	}

	pfree(querybuf.data);
	pfree(query);
}


static void
DataChangeTriggerFunctionAppendErrorOnDelete(CimvCreate *cimvCreate, StringInfo buf)
{
	appendStringInfo(buf,
					 "RAISE EXCEPTION $ex$MATERIALIZED VIEW '%s' on table '%s' does not support UPDATE/DELETE$ex$;\n",
					 cimvCreate->userViewNameQuoted->relname,
					 cimvCreate->baseTableNameQuoted->relname);
}


static void
AppendOnConflict(CimvCreate *cimvCreate, StringInfo buf, bool isInsert)
{
	appendStringInfoString(buf, " ON CONFLICT (");

	bool isFirst = true;
	TargetEntry *tle;
	foreach_ptr(tle, cimvCreate->groupTargetListEntries)
	{
		if (!isFirst)
		{
			appendStringInfo(buf, ", %s", quote_identifier(tle->resname));
		}
		else
		{
			appendStringInfo(buf, "%s", quote_identifier(tle->resname));
			isFirst = false;
		}
	}

	appendStringInfoString(buf,
						   ") DO UPDATE SET __count__ = __mat__.__count__ + EXCLUDED.__count__");

	isFirst = true;
	foreach_ptr(tle, cimvCreate->aggTargetListEntries)
	{
		Aggref *aggref = castNode(Aggref, tle->expr);
		const char *colname = quote_identifier(tle->resname);

		appendStringInfo(buf, ", %s = (SELECT %s(", colname, COMBINE_AGG_FNNAME);
		AppendStringInfoFunction(buf, aggref->aggfnoid);
		appendStringInfo(buf,
						 ", __val__) FROM (VALUES (EXCLUDED.%s),(__mat__.%s)) AS __agg__(__val__))",
						 colname,
						 colname);
	}
}


static void
CreateDataChangeTriggers(CimvCreate *cimvCreate)
{
	CreateDataChangeTrigger(cimvCreate, TRIGGER_EVENT_INSERT);

	if (!cimvCreate->createOptions->insertOnlyCapture)
	{
		CreateDataChangeTrigger(cimvCreate, TRIGGER_EVENT_DELETE);
		CreateDataChangeTrigger(cimvCreate, TRIGGER_EVENT_UPDATE);
		CreateDataChangeTrigger(cimvCreate, TRIGGER_EVENT_TRUNCATE);
	}
}


static void
CreateDataChangeTrigger(CimvCreate *cimvCreate, int triggerEvent)
{
	StringInfoData buf;
	initStringInfo(&buf);

	bool isCitusTable = cimvCreate->citusTable != NULL;

	char *event;
	char *referencing;
	switch (triggerEvent)
	{
		case TRIGGER_EVENT_INSERT:
			event = "INSERT";
			referencing = "REFERENCING NEW TABLE AS __ins__ ";
			break;

		case TRIGGER_EVENT_DELETE:
			event = "DELETE";
			referencing = "REFERENCING OLD TABLE AS __del__ ";
			break;

		case TRIGGER_EVENT_UPDATE:
			event = "UPDATE";
			referencing = "REFERENCING NEW TABLE AS __ins__ OLD TABLE AS __del__ ";
			break;

		case TRIGGER_EVENT_TRUNCATE:
			event = "TRUNCATE";
			referencing = "";
			break;

			/* default:
			 *    error; */
	}

	/* TODO: UPDATE [ OF column_name [, ... ] ]  */
	StringInfoData triggerName;
	initStringInfo(&triggerName);
	appendStringInfo(&triggerName, "%s_%s", NameStr(cimvCreate->formCimv->triggerfnname),
					 event);

	RangeVar *insertTable = cimvCreate->createOptions->schedule == NULL ?
							cimvCreate->matTableNameQuoted :
							cimvCreate->landingTableNameQuoted;

	if (isCitusTable)
	{
		appendStringInfo(&buf,
						 "SELECT bool_and(success), max(result) FROM run_command_on_colocated_placements($param$%s.%s$param$, $param$%s.%s$param$, $create_trigger$ ",
						 cimvCreate->baseTableNameQuoted->schemaname,
						 cimvCreate->baseTableNameQuoted->relname,
						 insertTable->schemaname,
						 insertTable->relname);

		appendStringInfo(&buf,
						 "CREATE TRIGGER %s AFTER %s ON %%s %s "
						 "FOR EACH STATEMENT EXECUTE PROCEDURE %s.%s(%%L)",
						 quote_identifier(triggerName.data),
						 event,
						 referencing,
						 quote_identifier(NameStr(
											  cimvCreate->formCimv->triggerfnnamespace)),
						 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnname)));

		appendStringInfoString(&buf, "$create_trigger$);");
	}
	else
	{
		appendStringInfo(&buf,
						 "CREATE TRIGGER %s AFTER %s ON %s.%s %s "
						 "FOR EACH STATEMENT EXECUTE PROCEDURE %s.%s('%s.%s')",
						 quote_identifier(triggerName.data),
						 event,
						 cimvCreate->baseTableNameQuoted->schemaname,
						 cimvCreate->baseTableNameQuoted->relname,
						 referencing,
						 quote_identifier(NameStr(
											  cimvCreate->formCimv->triggerfnnamespace)),
						 quote_identifier(NameStr(cimvCreate->formCimv->triggerfnname)),
						 insertTable->schemaname,
						 insertTable->relname);
	}

	int expectedResult = isCitusTable ? SPI_OK_SELECT : SPI_OK_UTILITY;
	if (SPI_execute(buf.data, false, 0) != expectedResult)
	{
		elog(ERROR, "SPI_exec failed: %s", buf.data);
	}

	if (isCitusTable)
	{
		if (SPI_tuptable != NULL && SPI_tuptable->tupdesc->natts == 2 && SPI_processed ==
			1)
		{
			SPITupleTable *tuptable = SPI_tuptable;
			TupleDesc tupdesc = tuptable->tupdesc;
			HeapTuple tuple = tuptable->vals[0];

			SPI_getvalue(tuple, tupdesc, 1);
			bool isNull;
			Datum isSuccessDatum = SPI_getbinval(tuple, tupdesc, 1, &isNull);

			if (!isNull && !DatumGetBool(isSuccessDatum))
			{
				elog(ERROR, "SPI_exec failed: %s", SPI_getvalue(tuple, tupdesc, 2));
			}
		}
	}

	pfree(buf.data);
}


static CimvCreate *
InitializeCimvCreate(const CreateTableAsStmt *stmt, MatViewCreateOptions *createOptions)
{
	CimvCreate *cimvCreate = palloc(sizeof(CimvCreate));
	cimvCreate->formCimv = palloc(sizeof(FormData_pg_cimv));
	cimvCreate->formCimv->jobid = 0;
	cimvCreate->formCimv->landingtable = InvalidOid;


	Query *query = (Query *) stmt->query;
	RangeTblEntry *baseRte = (RangeTblEntry *) linitial(query->rtable);

	cimvCreate->baseTableName = makeRangeVar(get_namespace_name(get_rel_namespace(
																baseRte->relid)),
											get_rel_name(baseRte->relid), -1);

	cimvCreate->stmt = stmt;
	cimvCreate->createOptions = createOptions;

	cimvCreate->formCimv->basetable = baseRte->relid;

	cimvCreate->prefixId = UniqueId();
	cimvCreate->prefix = CIMVInternalPrefix(cimvCreate->baseTableName, cimvCreate->prefixId);
	namestrcpy(&cimvCreate->formCimv->triggerfnnamespace, CIMV_INTERNAL_SCHEMA);
	char* funcName = CIMVTriggerFuncName(cimvCreate->prefixId, stmt->into->rel->relname);
	namestrcpy(&cimvCreate->formCimv->triggerfnname, funcName);
	StringInfo mat = makeStringInfo();
	appendStringInfo(mat, "%s_cimv_%s", cimvCreate->prefix, MATERIALIZATION_TABLE_SUFFIX);

	StringInfo rv = makeStringInfo();
	appendStringInfo(rv, "%s_cimv_%s", cimvCreate->prefix, REFRESH_VIEW_SUFFIX);

	StringInfo ld = makeStringInfo();
	appendStringInfo(ld, "%s_cimv_%s", cimvCreate->prefix, LANDING_TABLE_SUFFIX);

	cimvCreate->matTableName = makeRangeVar(CIMV_INTERNAL_SCHEMA, mat->data, -1);
	cimvCreate->userViewName = stmt->into->rel;
	cimvCreate->refreshViewName = makeRangeVar(CIMV_INTERNAL_SCHEMA, rv->data, -1);
	cimvCreate->landingTableName = makeRangeVar(CIMV_INTERNAL_SCHEMA, ld->data, -1);
	cimvCreate->targetListEntries = NIL;
	cimvCreate->groupTargetListEntries = NIL;
	cimvCreate->aggTargetListEntries = NIL;
	cimvCreate->citusTable = IsCitusTable(baseRte->relid) ? LookupCitusTableCacheEntry(
		baseRte->relid) : NULL;
	cimvCreate->partitionColumn = NULL;
	cimvCreate->supportsDelete = false;

	cimvCreate->baseTableNameQuoted = makeRangeVar(
		(char *) quote_identifier(cimvCreate->baseTableName->schemaname),
		(char *) quote_identifier(cimvCreate->baseTableName->relname), -1);
	cimvCreate->matTableNameQuoted = makeRangeVar(
		(char *) quote_identifier(cimvCreate->matTableName->schemaname),
		(char *) quote_identifier(cimvCreate->matTableName->relname), -1);
	cimvCreate->userViewNameQuoted = makeRangeVar(
		cimvCreate->userViewName->schemaname == NULL ? NULL : (char *) quote_identifier(
			cimvCreate->userViewName->schemaname),
		(char *) quote_identifier(cimvCreate->userViewName->relname), -1);
	cimvCreate->refreshViewNameQuoted = makeRangeVar(
		(char *) quote_identifier(cimvCreate->refreshViewName->schemaname),
		(char *) quote_identifier(cimvCreate->refreshViewName->relname), -1);
	cimvCreate->landingTableNameQuoted = makeRangeVar(
		(char *) quote_identifier(cimvCreate->landingTableName->schemaname),
		(char *) quote_identifier(cimvCreate->landingTableName->relname), -1);

	return cimvCreate;
}

static char* CIMVTriggerFuncName(int prefixId, const char* relname) {
	StringInfo funcName = makeStringInfo();
	appendStringInfo(funcName, "%s_%d",quote_identifier(relname), prefixId);
	return funcName->data;
}

static char* CIMVInternalPrefix(const RangeVar* baseTable, int prefixId) {

	if (baseTable->schemaname == NULL || baseTable->relname == NULL) {
		ereport(ERROR, (errmsg("unexpected state: schema name or relname not found.")));
	}

	StringInfo prefix = makeStringInfo();
	appendStringInfo(prefix, "%s_%s_%d",quote_identifier(baseTable->schemaname), 
		quote_identifier(baseTable->relname), prefixId);
	return prefix->data;
}


static MatViewCreateOptions *
GetMatViewCreateOptions(const CreateTableAsStmt *stmt)
{
	MatViewCreateOptions *result = palloc(sizeof(MatViewCreateOptions));

	result->isCimv = false;
	result->schedule = NULL;
	result->storageOptions = NIL;

	if (stmt == NULL || stmt->into == NULL || stmt->into->options == NIL)
	{
		return result;
	}

	ListCell *lc;
	foreach(lc, stmt->into->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (def->defnamespace != NULL && pg_strcasecmp(def->defnamespace,
													   CITUS_NAMESPACE) == 0)
		{
			if (pg_strcasecmp(def->defname, "cimv") == 0)
			{
				result->isCimv = defGetBoolean(def);
			}
			else if (pg_strcasecmp(def->defname, "schedule") == 0)
			{
				result->schedule = defGetString(def);
			}
			else if (pg_strcasecmp(def->defname, "insertonlycapture") == 0)
			{
				result->insertOnlyCapture = defGetBoolean(def);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid option \"%s\" in WITH clause", def->defname)));
			}
		}
		else
		{
			result->storageOptions = lappend(result->storageOptions, def);
		}
	}

	return result;
}


static ObjectAddress
DefineVirtualRelation(RangeVar *relation, List *tlist, Query *viewParse)
{
	CreateStmt *createStmt = makeNode(CreateStmt);
	TargetEntry *tle;

	/*
	 * create a list of ColumnDef nodes based on the names and types of the
	 * (non-junk) targetlist items from the view's SELECT list.
	 */
	List *attrList = NIL;
	foreach_ptr(tle, tlist)
	{
		if (!tle->resjunk)
		{
			ColumnDef *def = makeColumnDef(tle->resname,
										   exprType((Node *) tle->expr),
										   exprTypmod((Node *) tle->expr),
										   exprCollation((Node *) tle->expr));

			attrList = lappend(attrList, def);
		}
	}

	/*
	 * Set the parameters for keys/inheritance etc. All of these are
	 * uninteresting for views...
	 */
	createStmt->relation = relation;
	createStmt->tableElts = attrList;
	createStmt->inhRelations = NIL;
	createStmt->constraints = NIL;
	createStmt->options = NULL;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = NULL;
	createStmt->if_not_exists = false;

	/*
	 * Create the relation (this will error out if there's an existing
	 * view, so we don't need more code to complain if "replace" is
	 * false).
	 */
	ObjectAddress address = DefineRelation(createStmt, RELKIND_VIEW, InvalidOid, NULL,
										   NULL);
	Assert(address.objectId != InvalidOid);

	/* Make the new view relation visible */
	CommandCounterIncrement();

	/* Store the query for the view */
	StoreViewQuery(address.objectId, viewParse, false);

	CommandCounterIncrement();

	return address;
}


static bool
ValidateCimv(CimvCreate *cimvCreate)
{
	Query *query = (Query *) cimvCreate->stmt->query;

	if (cimvCreate->stmt->into->skipData && !cimvCreate->createOptions->insertOnlyCapture)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("WITH NO DATA requires 'citus.insertonlycapture' set to TRUE")));
	}

	if (query->commandType != CMD_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only SELECT queries allowed")));
	}

	if (query->cteList != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: WITH (common table expressions) not supported")));
	}

	if (query->distinctClause != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: DISTINCT not supported")));
	}

	if (query->groupingSets != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: GROUPING SETS not supported")));
	}

	if (query->hasDistinctOn)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: DISTINCT ON not supported")));
	}

	if (query->hasForUpdate)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: FOR [KEY] UPDATE/SHARE not supported")));
	}

	if (query->hasModifyingCTE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: INSERT/UPDATE/DELETE in WITH not supported")));
	}

	if (query->hasRecursive)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: WITH RECURSIVE not supported")));
	}

	if (query->hasRowSecurity)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: row-level-security (RLS) policies are not supported")));
	}

	if (query->hasSubLinks)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: subqueries are not supported")));
	}

	if (query->hasTargetSRFs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: set-returning functions not supported")));
	}

	if (query->hasWindowFuncs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: window functions (OVER) not supported")));
	}

	if (query->havingQual)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: HAVING not supported")));
	}

	if (query->limitCount != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: LIMIT not supported")));
	}

	if (query->limitOffset != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: OFFSET not supported")));
	}

	if (query->setOperations != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: set-operations (UNION/INTERSECT/EXCEPT) not supported")));
	}

	if (query->sortClause != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: ORDER BY not supported")));
	}

	if (query->groupClause == NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: at least one GROUP BY statement is required")));
	}

	if (!query->hasAggs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: at least one aggregate function is required")));
	}

	if (list_length(query->jointree->fromlist) != 1 ||
		!IsA(linitial(query->jointree->fromlist), RangeTblRef))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: materialized view can only be created on single table")));
	}

	RangeTblRef *rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
	RangeTblEntry *rte = list_nth(query->rtable, rtref->rtindex - 1);

	if (rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_PARTITIONED_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: materialized view can only be defined on a table")));
	}

	if (rte->tablesample != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "invalid SELECT query: materialized view cannot be defined on a table sample")));
	}

	if (!rte->inh)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query: 'SELECT ... FROM ONLY' not supported")));
	}

	cimvCreate->supportsDelete = true;
	TargetEntry *tle;
	foreach_ptr(tle, query->targetList)
	{
		if (contain_mutable_functions((Node *) tle))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only immutable functions are allowed.")));
		}

		if (IsA(tle->expr, Aggref))
		{
			Aggref *agg = (Aggref *) tle->expr;
			bool supportsDelete;
			ValidateAgg(agg, &supportsDelete);
			if (!supportsDelete)
			{
				cimvCreate->supportsDelete = false;
			}
			cimvCreate->aggTargetListEntries = lappend(cimvCreate->aggTargetListEntries,
													   tle);
			cimvCreate->targetListEntries = lappend(cimvCreate->targetListEntries, tle);
		}
		else if (!tle->resjunk)
		{
			if (contain_agg_clause((Node *) tle))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("only top-level aggregates are allowed.")));
			}

			if (cimvCreate->citusTable != NULL &&
				tle->resorigtbl != InvalidOid &&
				IsA(tle->expr, Var) &&
				tle->resorigcol == cimvCreate->citusTable->partitionColumn->varattno)
			{
				cimvCreate->partitionColumn = tle;
			}

			cimvCreate->groupTargetListEntries = lappend(
				cimvCreate->groupTargetListEntries, tle);
			cimvCreate->targetListEntries = lappend(cimvCreate->targetListEntries, tle);
		}
	}

	if (cimvCreate->citusTable != NULL && cimvCreate->partitionColumn == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "SELECT query needs to GROUP BY distribution column of distributed table")));
	}

	return true;
}


static void
ValidateAgg(Aggref *agg, bool *supportsDelete)
{
	if (agg->aggorder)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("aggregates with ORDER BY are not supported")));
	}

	if (agg->aggdistinct)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("aggregates with DISTINCT are not supported")));
	}

	if (agg->aggfilter)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("aggregates with FILTER are not supported")));
	}

	HeapTuple aggtuple = SearchSysCache1(AGGFNOID, agg->aggfnoid);
	if (!HeapTupleIsValid(aggtuple))
	{
		elog(ERROR, "cache lookup failed for aggregate function %u", agg->aggfnoid);
	}

	Form_pg_aggregate aggform = (Form_pg_aggregate) GETSTRUCT(aggtuple);
	if (aggform->aggkind != 'n')
	{
		ReleaseSysCache(aggtuple);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "ordered-set and hypothetical-set aggregates are not supported")));
	}

	if (aggform->aggcombinefn == InvalidOid)
	{
		ReleaseSysCache(aggtuple);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("aggregates without COMBINEFUNC are not supported")));
	}

	if (aggform->aggtranstype == INTERNALOID &&
		(aggform->aggserialfn == InvalidOid || aggform->aggdeserialfn == InvalidOid))
	{
		ReleaseSysCache(aggtuple);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "aggregates without INTERNAL stype must specify (de-)serialization functions")));
	}

	ReleaseSysCache(aggtuple);

	*supportsDelete = aggform->aggtransfn == aggform->aggmtransfn &&
					  aggform->aggminvtransfn != InvalidOid;
}


static void
AddCountAgg(Query *query, bool isInsert)
{
	Aggref *countAggregate = makeNode(Aggref);
	countAggregate->aggfnoid = AggregateFunctionOid("count", ANYOID);
	countAggregate->aggtype = get_func_rettype(countAggregate->aggfnoid);
	countAggregate->aggtranstype = InvalidOid;
	countAggregate->aggargtypes = NIL;
	countAggregate->aggsplit = AGGSPLIT_SIMPLE;
	countAggregate->aggkind = 'n';
	countAggregate->aggstar = true;

	if (isInsert)
	{
		TargetEntry *countAggregateTe = makeTargetEntry((Expr *) countAggregate,
														query->targetList->length + 1,
														"__count__", false);
		query->targetList = lappend(query->targetList, countAggregateTe);
	}
	else
	{
		OpExpr *negateOp = makeNode(OpExpr);
		negateOp->args = list_make1(countAggregate);
		negateOp->opno = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString(
														 "-")), InvalidOid, INT8OID);
		negateOp->opresulttype = INT8OID;
		negateOp->opretset = false;
		negateOp->opcollid = 0;
		negateOp->inputcollid = 0;
		set_opfuncid(negateOp);

		TargetEntry *negCountAggregateTe = makeTargetEntry((Expr *) negateOp,
														   query->targetList->length + 1,
														   "__count__", false);
		query->targetList = lappend(query->targetList, negCountAggregateTe);
	}
}


static Node *
PartializeAggs(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Aggref))
	{
		Aggref *originalAggregate = (Aggref *) node;
		Oid workerPartialId = PartialAggOid();

		Const *aggOidParam = makeConst(REGPROCEDUREOID, -1, InvalidOid, sizeof(Oid),
									   ObjectIdGetDatum(originalAggregate->aggfnoid),
									   false, true);


		List *newWorkerAggregateArgs =
			list_make1(makeTargetEntry((Expr *) aggOidParam, 1, NULL, false));

		if (list_length(originalAggregate->args) == 1)
		{
			/*
			 * Single argument case, append 'arg' to partial_agg(agg, arg).
			 * We don't wrap single argument in a row expression because
			 * it has performance implications to unwrap arguments on each
			 * SFUNC invocation.
			 */
			TargetEntry *newArg =
				copyObject((TargetEntry *) linitial(originalAggregate->args));
			newArg->resno++;
			newWorkerAggregateArgs = lappend(newWorkerAggregateArgs, newArg);
		}
		else
		{
			/*
			 * Aggregation on workers assumes a single aggregation parameter.
			 * To still be able to handle multiple parameters, we combine
			 * parameters into a single row expression, i.e., append 'ROW(...args)'
			 * to partial_agg(agg, ROW(...args)).
			 */
			RowExpr *rowExpr = makeNode(RowExpr);
			rowExpr->row_typeid = RECORDOID;
			rowExpr->row_format = COERCE_EXPLICIT_CALL;
			rowExpr->location = -1;
			rowExpr->colnames = NIL;

			TargetEntry *arg = NULL;
			foreach_ptr(arg, originalAggregate->args)
			{
				rowExpr->args = lappend(rowExpr->args, copyObject(arg->expr));
			}

			newWorkerAggregateArgs =
				lappend(newWorkerAggregateArgs,
						makeTargetEntry((Expr *) rowExpr, 2, NULL, false));
		}

		Const *inverseParam = makeConst(INT4OID, -1, InvalidOid, sizeof(INT4OID),
										Int32GetDatum(*((int *) context)),
										false, true);

		newWorkerAggregateArgs =
			lappend(newWorkerAggregateArgs,
					makeTargetEntry((Expr *) inverseParam, 3, NULL, false));

		/* orker_partial_agg(agg, arg) or partial_agg(agg, ROW(...args)) */
		Aggref *newWorkerAggregate = copyObject(originalAggregate);
		newWorkerAggregate->aggfnoid = workerPartialId;
		newWorkerAggregate->aggtype = BYTEAOID;
		newWorkerAggregate->args = newWorkerAggregateArgs;
		newWorkerAggregate->aggkind = AGGKIND_NORMAL;
		newWorkerAggregate->aggtranstype = INTERNALOID;
		newWorkerAggregate->aggargtypes = lappend_oid(lcons_oid(OIDOID,
																newWorkerAggregate->
																aggargtypes), INT4OID);
		newWorkerAggregate->aggsplit = AGGSPLIT_SIMPLE;

		return (Node *) newWorkerAggregate;
	}
	return expression_tree_mutator(node, PartializeAggs, context);
}


/*
 * CitusFunctionOidWithSignature looks up a function with given input types.
 * Looks in pg_catalog schema, as this function's sole purpose is
 * support aggregate lookup.
 */
static Oid
CitusFunctionOidWithSignature(char *functionName, int numargs, Oid *argtypes)
{
	List *aggregateName = list_make2(makeString("pg_catalog"), makeString(functionName));
	FuncCandidateList clist = FuncnameGetCandidates(aggregateName, numargs, NIL, false,
													false, true);

	for (; clist; clist = clist->next)
	{
		if (memcmp(clist->args, argtypes, numargs * sizeof(Oid)) == 0)
		{
			return clist->oid;
		}
	}

	ereport(ERROR, (errmsg("no matching oid for function: %s", functionName)));
	return InvalidOid;
}


/*
 * PartialAggOid looks up oid of pg_catalog.partial_agg
 */
static Oid
PartialAggOid()
{
	Oid argtypes[] = {
		OIDOID,
		ANYELEMENTOID,
		INT4OID
	};

	return CitusFunctionOidWithSignature(PARTIAL_AGG_FNNAME, 3, argtypes);
}


static void
AppendQuotedLiteral(StringInfo buf, const char *val)
{
	/*
	 * We form the string literal according to the prevailing setting of
	 * standard_conforming_strings; we never use E''. User is responsible for
	 * making sure result is used correctly.
	 */
	appendStringInfoChar(buf, '\'');
	for (const char *valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		if (SQL_STR_DOUBLE(ch, !standard_conforming_strings))
		{
			appendStringInfoChar(buf, ch);
		}
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}


static void
AppendStringInfoFunction(StringInfo buf, Oid fnoid)
{
	Oid typoutput;
	bool typIsVarlena;

	getTypeOutputInfo(REGPROCEDUREOID,
					  &typoutput, &typIsVarlena);

	char *extval = OidOutputFunctionCall(typoutput, ObjectIdGetDatum(fnoid));

	AppendQuotedLiteral(buf, extval);

	pfree(extval);

	appendStringInfo(buf, "::%s",
					 format_type_with_typemod(REGPROCEDUREOID,
											  -1));
}


/* AggregateFunctionOid performs a reverse lookup on aggregate function name,
 * and returns the corresponding aggregate function oid for the given function
 * name and input type.
 */
static Oid
AggregateFunctionOid(const char *functionName, Oid inputType)
{
	Oid functionOid = InvalidOid;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation procRelation = table_open(ProcedureRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_proc_proname,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(functionName));

	SysScanDesc scanDescriptor = systable_beginscan(procRelation,
													ProcedureNameArgsNspIndexId, true,
													NULL, scanKeyCount, scanKey);

	/* loop until we find the right function */
	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(heapTuple);
		int argumentCount = procForm->pronargs;

		if (argumentCount == 1)
		{
			/* check if input type and found value type match */
			if (procForm->proargtypes.values[0] == inputType ||
				procForm->proargtypes.values[0] == ANYELEMENTOID)
			{
#if PG_VERSION_NUM < PG_VERSION_12
				functionOid = HeapTupleGetOid(heapTuple);
#else
				functionOid = procForm->oid;
#endif
				break;
			}
		}
		Assert(argumentCount <= 1);

		heapTuple = systable_getnext(scanDescriptor);
	}

	if (functionOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("no matching oid for function: %s", functionName)));
	}

	systable_endscan(scanDescriptor);
	table_close(procRelation, AccessShareLock);

	return functionOid;
}
