--
-- MULTI_SORTED_MERGE_STREAMING
--
-- Runs the same test cases as multi_orderby_pushdown.sql but with the
-- streaming sorted merge adapter enabled via the GUC. This validates
-- that the streaming code path produces identical results to the eager
-- merge path.
--

SET citus.enable_streaming_sorted_merge TO on;

\i sql/multi_orderby_pushdown.sql

RESET citus.enable_streaming_sorted_merge;
