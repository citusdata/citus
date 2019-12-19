-- Ensures that only a single default strategy is possible
CREATE OR REPLACE FUNCTION citus_internal.pg_dist_rebalance_strategy_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    -- citus_add_rebalance_strategy also takes out a ShareRowExclusiveLock
    LOCK TABLE pg_dist_rebalance_strategy IN SHARE ROW EXCLUSIVE MODE;

    PERFORM citus_validate_rebalance_strategy_functions(
        NEW.shard_cost_function,
        NEW.node_capacity_function,
        NEW.shard_allowed_on_node_function);

    IF NEW.default_threshold < NEW.minimum_threshold THEN
      RAISE EXCEPTION 'default_threshold cannot be smaller than minimum_threshold';
    END IF;

    IF NOT NEW.default_strategy THEN
      RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.default_strategy = OLD.default_strategy THEN
        return NEW;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_dist_rebalance_strategy WHERE default_strategy) THEN
      RAISE EXCEPTION 'there cannot be two default strategies';
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql;
