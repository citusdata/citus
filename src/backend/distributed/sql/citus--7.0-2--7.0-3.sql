/* citus--7.0-2--7.0-3.sql */

ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq
  RENAME TO pg_dist_placement_placementid_seq;

ALTER TABLE pg_catalog.pg_dist_shard_placement
  ALTER COLUMN placementid SET DEFAULT nextval('pg_catalog.pg_dist_placement_placementid_seq');

CREATE TABLE citus.pg_dist_placement (
  placementid BIGINT NOT NULL default nextval('pg_dist_placement_placementid_seq'::regclass),
  shardid BIGINT NOT NULL,
  shardstate INT NOT NULL,
  shardlength BIGINT NOT NULL,
  groupid INT NOT NULL
);
ALTER TABLE citus.pg_dist_placement SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_placement TO public;

CREATE INDEX pg_dist_placement_groupid_index
  ON pg_dist_placement USING btree(groupid);

CREATE INDEX pg_dist_placement_shardid_index
  ON pg_dist_placement USING btree(shardid);

CREATE UNIQUE INDEX pg_dist_placement_placementid_index
  ON pg_dist_placement USING btree(placementid);

CREATE OR REPLACE FUNCTION citus.find_groupid_for_node(text, int)
RETURNS int AS $$
DECLARE
  groupid int := (SELECT groupid FROM pg_dist_node WHERE nodename = $1 AND nodeport = $2);
BEGIN
  IF groupid IS NULL THEN
    RAISE EXCEPTION 'There is no node at "%:%"', $1, $2;
  ELSE
    RETURN groupid;
  END IF;
END;
$$ LANGUAGE plpgsql;

INSERT INTO pg_catalog.pg_dist_placement
SELECT placementid, shardid, shardstate, shardlength,
       citus.find_groupid_for_node(placement.nodename, placement.nodeport::int) AS groupid
FROM pg_dist_shard_placement placement;

DROP TRIGGER dist_placement_cache_invalidate ON pg_catalog.pg_dist_shard_placement;
CREATE TRIGGER dist_placement_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_placement
    FOR EACH ROW EXECUTE PROCEDURE master_dist_placement_cache_invalidate();

-- this should be removed when noderole is added but for now it ensures the below view
-- returns the correct results and that placements unambiguously belong to a view
ALTER TABLE pg_catalog.pg_dist_node ADD CONSTRAINT pg_dist_node_groupid_unique
  UNIQUE (groupid);

DROP TABLE pg_dist_shard_placement;
CREATE VIEW citus.pg_dist_shard_placement AS
SELECT shardid, shardstate, shardlength, nodename, nodeport, placementid
-- assumes there's only one node per group
FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
  placement.groupid = node.groupid
);
ALTER VIEW citus.pg_dist_shard_placement SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_shard_placement TO public;

-- add some triggers which make it look like pg_dist_shard_placement is still a table

ALTER VIEW pg_catalog.pg_dist_shard_placement
  ALTER placementid SET DEFAULT nextval('pg_dist_placement_placementid_seq');

CREATE OR REPLACE FUNCTION citus.pg_dist_shard_placement_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      DELETE FROM pg_dist_placement WHERE placementid = OLD.placementid;
      RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
      UPDATE pg_dist_placement
        SET shardid = NEW.shardid, shardstate = NEW.shardstate,
            shardlength = NEW.shardlength, placementid = NEW.placementid,
            groupid = citus.find_groupid_for_node(NEW.nodename, NEW.nodeport)
        WHERE placementid = OLD.placementid;
      RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
      INSERT INTO pg_dist_placement
        (placementid, shardid, shardstate, shardlength, groupid)
      VALUES (NEW.placementid, NEW.shardid, NEW.shardstate, NEW.shardlength,
        citus.find_groupid_for_node(NEW.nodename, NEW.nodeport));
      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_dist_shard_placement_trigger
  INSTEAD OF INSERT OR UPDATE OR DELETE ON pg_dist_shard_placement
  FOR EACH ROW EXECUTE PROCEDURE citus.pg_dist_shard_placement_trigger_func();
