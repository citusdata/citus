--
-- ISSUE_5248
--

CREATE SCHEMA issue_5248;
SET search_path TO issue_5248;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 3013000;

create table countries(
  id serial primary key
  , name text
  , code varchar(2) collate "C" unique
);
insert into countries(name, code) select 'country-'||i, i::text from generate_series(10,99) i;
select create_reference_table('countries');

create table orgs (
    id bigserial primary key
  , name text
  , created_at timestamptz default now()
);
select create_distributed_table('orgs', 'id');

create table users (
    id bigserial
  , org_id bigint references orgs(id)
  , name text
  , created_at timestamptz default now()
  , country_id int -- references countries(id)
  , score bigint generated always as (id + country_id) stored
  , primary key (org_id, id)
);
select create_distributed_table('users', 'org_id');
alter table users add constraint fk_user_country foreign key (country_id) references countries(id);

create table orders (
    id bigserial
  , org_id bigint references orgs(id)
  , user_id bigint
  , price int
  , info jsonb
  , primary key (org_id, id)
  , foreign key (org_id, user_id) references users(org_id, id)
);
select create_distributed_table('orders', 'org_id');


create table events (
    id bigserial not null
  , user_id bigint not null
  , org_id bigint not null
  , event_time timestamp not null default now()
  , event_type int not null default 0
  , payload jsonb
  , primary key (user_id, id)
);
create index event_time_idx on events using BRIN (event_time);
create index event_json_idx on events using gin(payload);
select create_distributed_table('events', 'user_id'); -- on purpose don't colocate on correctly on org_id

create table local_data(
  id bigserial primary key
  , val int default ( (random()*100)::int )
);

insert into orgs(id, name) select  i,'org-'||i from generate_series(1,10) i;
insert into users(id, name, org_id, country_id) select i,'user-'||i, i+1, (i%90)+1  from generate_series(1,5) i;
insert into orders(id, org_id, user_id, price) select i, ((i+1))+1 , i+1, i/100 from generate_series(1,2) i;
insert into events(id, org_id, user_id, event_type) select i, ((i+1))+1 , i+1, i/100 from generate_series(1,10) i;
insert into local_data(id) select generate_series(1,10);


/*
 * Test that we don't get a crash. See #5248.
 */
SELECT   subq_3.c15 AS c0,
         subq_3.c0  AS c1,
         subq_3.c15 AS c2,
         subq_0.c1  AS c3,
         pg_catalog.String_agg( Cast(
                                      (
                                      SELECT tgargs
                                      FROM   pg_catalog.pg_trigger limit 1 offset 1) AS BYTEA), Cast(
                                                                                                      (
                                                                                                      SELECT minimum_value
                                                                                                      FROM   columnar.chunk limit 1 offset 5) AS BYTEA)) OVER (partition BY subq_3.c10 ORDER BY subq_3.c12,subq_0.c2) AS c4,
         subq_0.c1                                                                                                                                                                                                    AS c5
FROM     (
                    SELECT     ref_1.address                      AS c0,
                               ref_1.error                        AS c1,
                               sample_0.NAME                      AS c2,
                               sample_2.trftosql                  AS c3
                    FROM       pg_catalog.pg_statio_all_sequences AS ref_0
                    INNER JOIN pg_catalog.pg_hba_file_rules       AS ref_1
                    ON         ((
                                                 SELECT pg_catalog.Max(aggnumdirectargs)
                                                 FROM   pg_catalog.pg_aggregate) <= ref_0.blks_hit)
                    INNER JOIN countries  AS sample_0 TABLESAMPLE system (6.4)
                    INNER JOIN local_data AS sample_1 TABLESAMPLE bernoulli (8)
                    ON         ((
                                                     true)
                               OR         (
                                                     sample_0.NAME IS NOT NULL))
                    INNER JOIN pg_catalog.pg_transform AS sample_2 TABLESAMPLE bernoulli (1.2)
                    INNER JOIN pg_catalog.pg_language  AS ref_2
                    ON         ((
                                                 SELECT shard_cost_function
                                                 FROM   pg_catalog.pg_dist_rebalance_strategy limit 1 offset 1) IS NULL)
                    RIGHT JOIN pg_catalog.pg_index AS sample_3 TABLESAMPLE system (0.3)
                    ON         ((
                                                     cast(NULL AS bpchar) ~<=~ cast(NULL AS bpchar))
                               OR         ((
                                                                EXISTS
                                                                (
                                                                       SELECT sample_3.indnkeyatts        AS c0,
                                                                              sample_2.trflang            AS c1,
                                                                              sample_2.trftype            AS c2
                                                                       FROM   pg_catalog.pg_statistic_ext AS sample_4 TABLESAMPLE bernoulli (8.6)
                                                                       WHERE  sample_2.trftype IS NOT NULL))
                                          AND        (
                                                                false)))
                    ON         (
                                          EXISTS
                                          (
                                                 SELECT sample_0.id           AS c0,
                                                        sample_3.indisprimary AS c1
                                                 FROM   orgs           AS sample_5 TABLESAMPLE system (5.3)
                                                 WHERE  false))
                    ON         (
                                          cast(NULL AS float8) >
                                          (
                                                 SELECT pg_catalog.avg(enumsortorder)
                                                 FROM   pg_catalog.pg_enum) )
                    WHERE      cast(COALESCE(
                               CASE
                                          WHEN ref_1.auth_method ~>=~ ref_1.auth_method THEN cast(NULL AS path)
                                          ELSE cast(NULL AS path)
                               END , cast(NULL AS path)) AS path) = cast(NULL AS path)) AS subq_0,
         lateral
         (
                SELECT
                       (
                              SELECT pg_catalog.stddev(total_time)
                              FROM   pg_catalog.pg_stat_user_functions) AS c0,
                       subq_0.c1                                        AS c1,
                       subq_2.c0                                        AS c2,
                       subq_0.c2                                        AS c3,
                       subq_0.c0                                        AS c4,
                       cast(COALESCE(subq_2.c0, subq_2.c0) AS text)     AS c5,
                       subq_2.c0                                        AS c6,
                       subq_2.c1                                        AS c7,
                       subq_2.c1                                        AS c8,
                       subq_2.c1                                        AS c9,
                       subq_0.c3                                        AS c10,
                       pg_catalog.pg_stat_get_db_temp_files( cast(
                                                                   (
                                                                   SELECT objoid
                                                                   FROM   pg_catalog.pg_description limit 1 offset 1) AS oid)) AS c11,
                       subq_0.c3                                                                                               AS c12,
                       subq_2.c1                                                                                               AS c13,
                       subq_0.c0                                                                                               AS c14,
                       subq_0.c3                                                                                               AS c15,
                       subq_0.c3                                                                                               AS c16,
                       subq_0.c1                                                                                               AS c17,
                       subq_0.c2                                                                                               AS c18
                FROM   (
                              SELECT subq_1.c2                        AS c0,
                                     subq_0.c3                        AS c1
                              FROM   information_schema.element_types AS ref_3,
                                     lateral
                                     (
                                            SELECT subq_0.c1            AS c0,
                                                   sample_6.info        AS c1,
                                                   subq_0.c2            AS c2,
                                                   subq_0.c3            AS c3,
                                                   sample_6.user_id     AS c5,
                                                   ref_3.collation_name AS c6
                                            FROM   orders        AS sample_6 TABLESAMPLE system (3.8)
                                            WHERE  sample_6.price = sample_6.org_id limit 58) AS subq_1
                              WHERE  (
                                            subq_1.c2 <= subq_0.c2)
                              AND    (
                                            cast(NULL AS line) ?-| cast(NULL AS line)) limit 59) AS subq_2
                WHERE  cast(COALESCE(pg_catalog.age( cast(
                                                           (
                                                           SELECT pg_catalog.max(event_time)
                                                           FROM   events) AS "timestamp")),
                       (
                              SELECT write_lag
                              FROM   pg_catalog.pg_stat_replication limit 1 offset 3) ) AS "interval") >
                       (
                              SELECT utc_offset
                              FROM   pg_catalog.pg_timezone_names limit 1 offset 4) limit 91) AS subq_3
WHERE    pg_catalog.pg_backup_stop() > cast(NULL AS record) limit 100;

SET client_min_messages TO WARNING;
DROP SCHEMA issue_5248 CASCADE;
