-- By default Citus makes lots of connections in the background which fill up the log
-- By tweaking these settings you can make sure you only capture packets related to what
--   you're doing
ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO -1;
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
ALTER SYSTEM set citus.enable_statistics_collection TO false;
SELECT pg_reload_conf();

CREATE OR REPLACE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

-- Add some helper functions for sending commands to mitmproxy

CREATE FUNCTION citus.mitmproxy(text) RETURNS TABLE(result text) AS $$
DECLARE
  command ALIAS FOR $1;
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text) ON COMMIT DROP;
  CREATE TEMPORARY TABLE mitmproxy_result (res text) ON COMMIT DROP;

  INSERT INTO mitmproxy_command VALUES (command);

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  RETURN QUERY SELECT * FROM mitmproxy_result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.clear_network_traffic() RETURNS void AS $$
BEGIN
  PERFORM citus.mitmproxy('recorder.reset()');
  RETURN; -- return void
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.dump_network_traffic()
RETURNS TABLE(conn int, source text, message text) AS $$
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text) ON COMMIT DROP;
  CREATE TEMPORARY TABLE mitmproxy_result (
    conn int, source text, message text
  ) ON COMMIT DROP;

  INSERT INTO mitmproxy_command VALUES ('recorder.dump()');

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  RETURN QUERY SELECT * FROM mitmproxy_result;
END;
$$ LANGUAGE plpgsql;

\c - - - :worker_2_port

-- Add some helper functions for sending commands to mitmproxy

CREATE FUNCTION citus.mitmproxy(text) RETURNS TABLE(result text) AS $$
DECLARE
  command ALIAS FOR $1;
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text) ON COMMIT DROP;
  CREATE TEMPORARY TABLE mitmproxy_result (res text) ON COMMIT DROP;

  INSERT INTO mitmproxy_command VALUES (command);

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  RETURN QUERY SELECT * FROM mitmproxy_result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.clear_network_traffic() RETURNS void AS $$
BEGIN
  PERFORM citus.mitmproxy('recorder.reset()');
  RETURN; -- return void
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.dump_network_traffic()
RETURNS TABLE(conn int, source text, message text) AS $$
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text) ON COMMIT DROP;
  CREATE TEMPORARY TABLE mitmproxy_result (
    conn int, source text, message text
  ) ON COMMIT DROP;

  INSERT INTO mitmproxy_command VALUES ('recorder.dump()');

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  RETURN QUERY SELECT * FROM mitmproxy_result;
END;
$$ LANGUAGE plpgsql;
