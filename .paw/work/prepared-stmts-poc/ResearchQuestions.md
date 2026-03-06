# Spec Research Questions: Prepared Statements POC

## Context
Prepared statements in Citus currently re-parse and re-plan queries on remote shards every time, so the benefit degrades as cluster size increases. The POC should cache plans on remote shards for fast-path (single-shard) queries. See WorkflowContext.md for full description and constraints.

## Internal Questions (answer from codebase)

1. How does Citus currently handle prepared statements from the user's perspective? When a user PREPAREs and EXECUTEs a statement on a distributed table, what is the end-to-end behavior?

2. What is a "fast-path" query in Citus? What criteria must a query meet to qualify for the fast path?

3. How does Citus send queries to worker/remote shards? Does it use the simple query protocol (PQexec) or the extended query protocol (PQexecParams/PQprepare/PQexecPrepared)?

4. How does Citus manage connections to worker nodes? Are connections pooled, cached, or opened per-query? What is their lifecycle?

5. When a prepared statement is executed multiple times in the same session, does Citus re-plan the remote query each time, or is any caching done currently?

6. What is the current query execution flow for a fast-path prepared statement? Trace from EXECUTE through to the remote shard query.

7. Does Citus currently use PQprepare / PQexecPrepared on any remote connections? If so, in what contexts?

8. How does PostgreSQL handle prepared statement plan caching locally (generic vs custom plans)? Is Citus affected by this mechanism?

## External Questions (optional, for user to fill in)

- [ ] Are there any existing benchmarks or performance measurements for prepared statements on Citus clusters of varying sizes?
- [ ] Are there any known issues or prior attempts to address this problem?
