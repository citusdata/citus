--
-- MULTI_ORDERBY_PUSHDOWN_STREAMING
--
-- Runs the sorted merge test suite (multi_orderby_pushdown.sql) twice:
-- first with the default eager-merge path, then with the streaming
-- adapter enabled via citus.enable_streaming_sorted_merge. Both runs
-- share the same setup tables and must produce identical results
-- (except for the G3 backward-scan test, where the streaming adapter's
-- forward-only cursor correctly errors on FETCH BACKWARD).
--

\i sql/setup_multi_orderby_pushdown.sql.include

-- Run 1: eager merge (default)
\i sql/multi_orderby_pushdown.sql.include

-- Run 2: streaming adapter
SET citus.enable_streaming_sorted_merge TO on;
\i sql/multi_orderby_pushdown.sql.include
RESET citus.enable_streaming_sorted_merge;

-- Cleanup
DROP TABLE sorted_merge_test;
DROP TABLE sorted_merge_events;
