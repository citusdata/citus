--
-- MULTI_MULTIUSER_AUTH
--
-- Test authentication logic
--

-- We'll test different passwords for every user/host combo

\set alice_master_pw   mug-loth-oaf-mailman-languor
\set alice_worker_1_pw ratline-belabor-fatigue-chive-esdras
\set alice_worker_2_pw boogie-chick-asterisk-nevus-sonny
\set alice_fallback_pw :alice_worker_2_pw

\set bob_master_pw     nimbly-milepost-sandbag-cucumber-marksman
\set bob_worker_1_pw   triplex-royalty-warranty-stand-cheek
\set bob_worker_2_pw   omnibus-plectrum-comet-sneezy-ensile
\set bob_fallback_pw   :bob_worker_1_pw

SELECT nodeid AS worker_1_id FROM pg_dist_node WHERE nodename = 'localhost' AND nodeport = :worker_1_port;
\gset
SELECT nodeid AS worker_2_id FROM pg_dist_node WHERE nodename = 'localhost' AND nodeport = :worker_2_port;
\gset

-- alice is a superuser so she can update own password
CREATE USER alice PASSWORD :'alice_master_pw' SUPERUSER;
CREATE USER bob   PASSWORD :'bob_master_pw';

-- note we enter a wrong password for Alice to test cache invalidation
INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES
(-1,           'alice', 'password=' || :'alice_master_pw'),
(:worker_1_id, 'alice', 'password=' || 'wrong_password'),
(0,            'alice', 'password=' || :'alice_fallback_pw'),
(-1,           'bob',   'password=' || :'bob_master_pw'),
(0,            'bob',   'password=' || :'bob_fallback_pw'),
(:worker_2_id, 'bob',   'password=' || :'bob_worker_2_pw');

\c - - - :worker_1_port
set citus.enable_ddl_propagation to off;
ALTER ROLE alice PASSWORD :'alice_worker_1_pw' SUPERUSER;
ALTER ROLE bob   PASSWORD :'bob_worker_1_pw';
reset citus.enable_ddl_propagation;

-- note the wrong password for loopbacks here; task-tracker will fail
INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES
(0, 'alice', 'password=dummy'),
(-1, 'alice', 'password=' || 'wrong_password'),
(-1, 'bob',   'password=' || :'bob_worker_1_pw'),
(0, 'bob', 'password=' || :'bob_worker_2_pw')
;

\c - - - :worker_2_port
set citus.enable_ddl_propagation to off;
ALTER ROLE alice PASSWORD :'alice_worker_2_pw' SUPERUSER;
ALTER ROLE bob   PASSWORD :'bob_worker_2_pw';
reset citus.enable_ddl_propagation;

INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES
(0, 'alice', 'password=dummy'),
(-1, 'alice', 'password=' || 'wrong_password'),
(-1, 'bob',   'password=' || :'bob_worker_2_pw'),
(0, 'bob', 'password=' || :'bob_worker_1_pw')
;

\c - - - :master_port
-- build format strings to specify PW
SELECT format('user=%s host=localhost port=%s password=%s dbname=regression',
              'alice', :master_port, :'alice_master_pw') AS alice_conninfo;
\gset
SELECT format('user=%s host=localhost port=%s password=%s dbname=regression',
              'bob', :master_port, :'bob_master_pw') AS bob_conninfo;
\gset

GRANT ALL ON TABLE lineitem, orders, lineitem, customer, nation, part, supplier TO alice, bob;

\c :alice_conninfo

-- router query (should break because of bad password)
INSERT INTO customer VALUES (12345, 'name', NULL, 5, 'phone', 123.45, 'segment', 'comment');

-- fix alice's worker1 password ...
UPDATE pg_dist_authinfo
SET authinfo = ('password=' || :'alice_worker_1_pw')
WHERE nodeid = :worker_1_id AND rolename = 'alice';

-- and try again because cache should clear, should
-- just get invalid constraint this time, no bad pw
INSERT INTO customer VALUES (12345, 'name', NULL, 5, 'phone', 123.45, 'segment', 'comment');

SELECT o_orderstatus, sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 9030
	GROUP BY o_orderstatus
	HAVING sum(l_linenumber) > 1000
	ORDER BY o_orderstatus;

-- fix worker passwords, which should invalidate task tracker caches
\c - postgres - :worker_1_port

UPDATE pg_dist_authinfo
SET authinfo = ('password=' || :'alice_worker_1_pw')
WHERE nodeid = -1 AND rolename = 'alice';

\c - postgres - :worker_2_port

UPDATE pg_dist_authinfo
SET authinfo = ('password=' || :'alice_worker_2_pw')
WHERE nodeid = -1 AND rolename = 'alice';

\c :alice_conninfo
SELECT o_orderstatus, sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 9030
	GROUP BY o_orderstatus
	HAVING sum(l_linenumber) > 1000
	ORDER BY o_orderstatus;

-- adaptive query
RESET citus.task_executor_type;
SELECT o_orderstatus, sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 9030
	GROUP BY o_orderstatus
	HAVING sum(l_linenumber) > 1000
	ORDER BY o_orderstatus;

-- create and distribute table
CREATE TABLE wonderland (id integer, name text);
SELECT create_distributed_table('wonderland', 'id');

-- copy
COPY wonderland FROM STDIN WITH (FORMAT 'csv');
1,White Rabbit
2,Mad Hatter
3,Queen of Hearts
\.

SELECT COUNT(*) FROM wonderland;

DROP TABLE wonderland;

GRANT CREATE ON SCHEMA public TO bob;

\c :bob_conninfo

-- bob can't change authinfo: not a superuser

DELETE FROM pg_dist_authinfo WHERE rolename = 'bob';

CREATE TABLE bob_lineitem (LIKE lineitem);

SELECT create_distributed_table('bob_lineitem', 'l_orderkey', 'hash');
INSERT INTO bob_lineitem SELECT * FROM lineitem;

SET citus.enable_repartition_joins TO ON;
SELECT count(*) > 1 from bob_lineitem b , lineitem l where b.l_orderkey = l.l_orderkey LIMIT 10;

SELECT COUNT(*) FROM bob_lineitem;

DROP TABLE bob_lineitem;
