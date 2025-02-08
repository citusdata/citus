CREATE SCHEMA comment_migration_table;

CREATE TABLE comment_migration_table.table_1
(
    id            bigserial,
    code          varchar(200)                           not null,
    name          varchar(200),
    date_created  timestamp with time zone default NOW() not null,
    active        boolean                  default true,
    owner_id     bigint,
    CONSTRAINT table_1_pkey PRIMARY KEY (id)
)
    WITH (autovacuum_enabled = TRUE);

CREATE TABLE comment_migration_table.table_2
(
    id            bigserial,
    table_1_id    bigint ,
    description   varchar(200),
    owner_id     bigint,
    CONSTRAINT table_2_pkey PRIMARY KEY (id)
)
    WITH (autovacuum_enabled = TRUE);

SET citus.shard_replication_factor = 1;

comment on table comment_migration_table.table_1 is 'Table 1';
comment on table comment_migration_table.table_2 is 'Table 2';
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

SELECT create_reference_table('comment_migration_table.table_1');
SELECT create_distributed_table('comment_migration_table.table_2', 'id');
ALTER TABLE comment_migration_table.table_2 ADD CONSTRAINT table2_table1_fk FOREIGN KEY (table_1_id) REFERENCES comment_migration_table.table_1(id);
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

SELECT undistribute_table('comment_migration_table.table_1', cascade_via_foreign_keys=>true);
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

ALTER TABLE comment_migration_table.table_2 DROP CONSTRAINT table2_table1_fk;
SELECT create_reference_table('comment_migration_table.table_1');
SELECT create_distributed_table('comment_migration_table.table_2', 'id');
ALTER TABLE comment_migration_table.table_2 ADD CONSTRAINT table2_table1_fk FOREIGN KEY (table_1_id) REFERENCES comment_migration_table.table_1(id);
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

DROP TABLE comment_migration_table.table_2;
DROP TABLE comment_migration_table.table_1;
DROP SCHEMA comment_migration_table;
