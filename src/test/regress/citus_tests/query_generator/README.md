## Goal of the Tool
Tool supports a simple syntax to be useful to generate queries with different join orders. Main motivation for me to create the tool was to compare results of the generated queries for different [Citus](https://github.com/citusdata/citus) tables and Postgres tables. That is why we support a basic syntax for now. It can be extended to support different queries.

## Query Generator for Postgres
Tool generates SELECT queries, whose depth can be configured, with different join orders. It also generates DDLs required for query execution.
You can also tweak configuration parameters for data inserting command generation.

## How to Run Citus Join Verification?
You can verify if Citus breaks any default PG join behaviour via `citus_compare_dist_local_joins.sh`. It creates
tables specified in config. Then, it runs generated queries on those tables and saves the results into `out/dist_queries.out`.
After running those queries for Citus tables, it creates PG tables with the same names as previous run, executes the same
queries, and saves the results into `out/local_queries.out`. In final step, it generates diff between local and distributed results.
You can see the contents of `out/local_dist_diffs` to see if there is any Citus unsupported query.

1. Create a Citus local cluster with 2 workers by using [citus_dev](https://github.com/citusdata/tools/tree/develop/citus_dev) tool
(Note: make sure you do not configure psql via .psqlrc file as it would fail the test.)
```bash
citus_dev make testCluster --destroy
```
2. Run the test,
```bash
cd src/test/regress/citus_tests/query_generator/bin
bash citus_compare_dist_local_joins.sh <username> <dbname> <coordinator_port> Optional:<seed>
```
3. See the diff content in `src/test/regress/citus_tests/query_generator/out/local_dist_diffs`

Note: `seed` can be used to reproduce a run of Citus test by generating the same queries and results via the given seed.

### Configuration
You can configure 3 different parts:

- [DDL Configuration](#ddl-configuration)
- [Data Insertion Configuration](#data-insertion-configuration)
- [Query Configuration](#query-configuration)

## DDL Configuration
Tool generates related ddl commands before generating queries.

Schema for DDL configuration:
```yaml
ddlOutFile: <string>
commonColName: <string>
targetTables: <Table[]>
  - Table:
      name: <string>
      citusType: <CitusType>
      maxAllowedUseOnQuery: <int>
      rowCount: <int>
      nullRate: <float>
      duplicateRate: <float>
      columns: <Column[]>
        - Column:
          name: <string>
          type: <string>
      distinctCopyCount: <int>
```

Explanation:
```yaml
ddlOutFile: "file to write generated DDL commands"
commonColName: "name of the column that will be used as distribution column, filter column in restrictions and target column in selections"
targetTables: "array of tables that will be used in generated queries"
  - Table:
      name: "name prefix of table"
      citusType: "citus type of table"
      maxAllowedUseOnQuery: "limits how many times table can appear in query"
      rowCount: "total # of rows that will be inserted into table"
      nullRate: "percentage of null rows in rowCount that will be inserted into table"
      duplicateRate: "percentage of duplicates in rowCount that will be inserted into table"
      columns: "array of columns in table"
        - Column:
          name: "name of column"
          type: "name of data type of column(only support 'int' now)"
      distinctCopyCount: "how many tables with the same configuration we should create(only by changing full name, still using the same name prefix)"
```


## Data Insertion Configuration
Tool generates data insertion commands if you want tables with filled data. You can configure total number of rows, what percentage of them should
be null and what percentage of them should be duplicated. For related configuration see Table schema at [DDL Configuration](#ddl-configuration). You
can also configure range of the randomly generated data. See `dataRange` at [Query Configuration](#query-configuration).

## Query Configuration
After generation of ddls and data insertion commands, the tool generates queries.

Schema for Query configuration:
```yaml
queryCount: <int>
queryOutFile: <string>
semiAntiJoin: <bool>
cartesianProduct: <bool>
limit: <bool>
orderby: <bool>
forceOrderbyWithLimit: <bool>
useAvgAtTopLevelTarget: <bool>
dataRange:
  from: <int>
  to: <int>
filterRange:
  from: <int>
  to: <int>
limitRange:
  from: <int>
  to: <int>
targetRteCount: <int>
targetCteCount: <int>
targetCteRteCount: <int>
targetJoinTypes: <JoinType[]>
targetRteTypes: <RteType[]>
targetRestrictOps: <RestrictOp[]>
```

Explanation:
```yaml
queryCount: "number of queries to generate"
queryOutFile: "file to write generated queries"
semiAntiJoin: "should we support semi joins (WHERE col IN (Subquery))"
cartesianProduct: "should we support cartesian joins"
limit: "should we support limit clause"
orderby: "should we support order by clause"
forceOrderbyWithLimit: "should we force order by when we use limit"
useAvgAtTopLevelTarget: "should we make top level query as select avg() from (subquery)"
dataRange:
  from: "starting boundary for data generation"
  to: "end boundary for data generation"
filterRange:
  from: "starting boundary for restriction clause"
  to: "end boundary for restriction clause"
limitRange:
  from: "starting boundary for limit clause"
  to: "end boundary for data limit clause"
targetRteCount: "limits how many rtes should exist in non-cte part of the query"
targetCteCount: "limits how many ctes should exist in query"
targetCteRteCount: "limits how many rtes should exist in cte part of the query"
targetJoinTypes: "supported join types"
targetRteTypes: "supported rte types"
targetRestrictOps: "supported restrict ops"
```

## Misc Configuration
Tool has some configuration options which does not suit above 3 parts.

Schema for misc configuration:
```yaml
interactiveMode: <bool>
```

Explanation:
```yaml
interactiveMode: "when true, interactively prints generated ddls and queries. Otherwise, it writes them to configured files."
```

## Supported Operations
It uses `commonColName` for any kind of target selection required for any supported query clause.

### Column Type Support
Tool currently supports only int data type, but plans to support other basic types.

### Join Support
Tool supports following joins:
```yaml
targetJoinTypes:
  - INNER
  - LEFT
  - RIGHT
  - FULL
```

### Citus Table Support
Tool supports following citus table types:
```yaml
targetTables:
  - Table:
    ...
    citusType: <one of (DISTRIBUTED || REFERENCE || POSTGRES)>
    ...
```

### Restrict Operation Support
Tool supports following restrict operations:
```yaml
targetRestrictOps:
  - LT
  - GT
  - EQ
```

### Rte Support
Tool supports following rtes:
```yaml
targetRteTypes:
  - RELATION
  - SUBQUERY
  - CTE
  - VALUES
```

## How to Generate Queries?
You have 2 different options.

- [Interactive Mode](#interactive-mode)
- [File Mode](#file-mode)

### Interactive Mode
In this mode, you will be prompted to continue generating a query. When you hit to `Enter`, it will continue generating them.
You will need to hit to `x` to exit.

1. Configure `interactiveMode: true` in config.yml,
2. Run the command shown below
```bash
cd src/test/regress/citus_tests/query_generator
python generate_queries.py
```

### File Mode
In this mode, generated ddls and queries will be written into the files configured in [config.yml](./config/config.yaml).

1. Configure `interactiveMode: false`,
2. Configure `queryCount: <total_query>`,
3. Configure `queryOutFile: <query_file_path>` and `ddlOutFile: <ddlfile_path>`
4. Run the command shown below
```bash
cd src/test/regress/citus_tests/query_generator
python generate_queries.py
```
