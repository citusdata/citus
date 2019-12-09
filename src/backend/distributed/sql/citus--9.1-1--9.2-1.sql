ALTER TABLE pg_catalog.pg_dist_colocation ADD distributioncolumncollation oid;
UPDATE pg_catalog.pg_dist_colocation dc SET distributioncolumncollation = t.typcollation
	FROM pg_catalog.pg_type t WHERE t.oid = dc.distributioncolumntype;
UPDATE pg_catalog.pg_dist_colocation dc SET distributioncolumncollation = 0 WHERE distributioncolumncollation IS NULL;
ALTER TABLE pg_catalog.pg_dist_colocation ALTER COLUMN distributioncolumncollation SET NOT NULL;

DROP INDEX pg_dist_colocation_configuration_index;
-- distributioncolumntype should be listed first so that this index can be used for looking up reference tables' colocation id
CREATE INDEX pg_dist_colocation_configuration_index
ON pg_dist_colocation USING btree(distributioncolumntype, shardcount, replicationfactor, distributioncolumncollation);

