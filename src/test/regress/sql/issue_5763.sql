--
-- ISSUE_5763
--
-- Issue: DROP OWNED BY fails to drop the schemas on the workers
-- Link: https://github.com/citusdata/citus/issues/5763
--

CREATE USER issue_5763 WITH SUPERUSER;

\c - issue_5763 - :master_port
CREATE SCHEMA issue_5763_sc;

\c - postgres - :master_port
DROP OWNED BY issue_5763;

\c - issue_5763 - :master_port
CREATE SCHEMA issue_5763_sc;

\c - postgres - :master_port
DROP SCHEMA issue_5763_sc;
DROP USER issue_5763;
