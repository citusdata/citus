/* citus--7.0-4--7.0-5.sql */

SET search_path = 'pg_catalog';

CREATE TYPE pg_catalog.noderole AS ENUM (
  'primary',     -- node is available and accepting writes
  'secondary',   -- node is available but only accepts reads
  'unavailable' -- node is in recovery or otherwise not usable
-- adding new values to a type inside of a transaction (such as during an ALTER EXTENSION
-- citus UPDATE) isn't allowed in PG 9.6, and only allowed in PG10 if you don't use the
-- new values inside of the same transaction. You might need to replace this type with a
-- new one and then change the column type in pg_dist_node. There's a list of
-- alternatives here:
-- https://stackoverflow.com/questions/1771543/postgresql-updating-an-enum-type/41696273
);

ALTER TABLE pg_dist_node ADD COLUMN noderole noderole NOT NULL DEFAULT 'primary';

-- we're now allowed to have more than one node per group
ALTER TABLE pg_catalog.pg_dist_node DROP CONSTRAINT pg_dist_node_groupid_unique;

-- so make sure pg_dist_shard_placement only returns writable placements
CREATE OR REPLACE VIEW pg_catalog.pg_dist_shard_placement AS
  SELECT shardid, shardstate, shardlength, nodename, nodeport, placementid
  FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
    placement.groupid = node.groupid AND node.noderole = 'primary'
  );

CREATE OR REPLACE FUNCTION citus.pg_dist_node_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    /* AddNodeMetadata also takes out a ShareRowExclusiveLock */
    LOCK TABLE pg_dist_node IN SHARE ROW EXCLUSIVE MODE;
    IF (TG_OP = 'INSERT') THEN
      IF NEW.noderole = 'primary'
          AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
                                                       noderole = 'primary' AND
                                                       nodeid <> NEW.nodeid) THEN
        RAISE EXCEPTION 'there cannot be two primary nodes in a group';
      END IF;
      RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
      IF NEW.noderole = 'primary'
           AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
                                                        noderole = 'primary' AND
                                                        nodeid <> NEW.nodeid) THEN
         RAISE EXCEPTION 'there cannot be two primary nodes in a group';
      END IF;
      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_dist_node_trigger
  BEFORE INSERT OR UPDATE ON pg_dist_node
  FOR EACH ROW EXECUTE PROCEDURE citus.pg_dist_node_trigger_func();

DROP FUNCTION master_add_node(text, integer);
CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default 0,
                                noderole noderole default 'primary',
                                OUT nodeid integer,
                                OUT groupid integer,
                                OUT nodename text,
                                OUT nodeport integer,
                                OUT noderack text,
                                OUT hasmetadata boolean,
                                OUT isactive bool,
                                OUT noderole noderole)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole)
  IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default 0,
                                         noderole noderole default 'primary',
                                         OUT nodeid integer,
                                         OUT groupid integer,
                                         OUT nodename text,
                                         OUT nodeport integer,
                                         OUT noderack text,
                                         OUT hasmetadata boolean,
                                         OUT isactive bool,
                                         OUT noderole noderole)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer,
                                             groupid integer, noderole noderole)
  IS 'prepare node by adding it to pg_dist_node';

RESET search_path;
