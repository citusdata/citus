CREATE OR REPLACE FUNCTION pg_catalog.citus_set_default_rebalance_strategy(
    name text
)
    RETURNS VOID
    STRICT
AS $$
  BEGIN
    LOCK TABLE pg_dist_rebalance_strategy IN SHARE ROW EXCLUSIVE MODE;
    IF NOT EXISTS (SELECT 1 FROM pg_dist_rebalance_strategy t WHERE t.name = $1) THEN
      RAISE EXCEPTION 'strategy with specified name does not exist';
    END IF;
    UPDATE pg_dist_rebalance_strategy SET default_strategy = false WHERE default_strategy = true;
    UPDATE pg_dist_rebalance_strategy t SET default_strategy = true WHERE t.name = $1;
  END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pg_catalog.citus_set_default_rebalance_strategy(text)
  IS 'changes the default rebalance strategy to the one with the specified name';
