CREATE SCHEMA comment_migration_table;

CREATE TABLE comment_migration_table.table_1
(
    id            bigserial,
    code          varchar(200)                           not null,
    name          varchar(200),
    date_created  timestamp with time zone default NOW() not null,
    active        boolean                  default true,
    tenant_id     bigint,
    CONSTRAINT table_1_pkey PRIMARY KEY (id, tenant_id)
)
    WITH (autovacuum_enabled = TRUE);

CREATE TABLE comment_migration_table.table_2
(
    id            bigserial,
    table_1_id    bigint ,
    description   varchar(200),
    tenant_id     bigint,
    CONSTRAINT table_2_pkey PRIMARY KEY (id, tenant_id)
)
    WITH (autovacuum_enabled = TRUE);

SET citus.shard_replication_factor = 1;

comment on table comment_migration_table.table_1 is 'Table 1';
comment on table comment_migration_table.table_2 is 'Table 2';
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

SELECT create_distributed_table('comment_migration_table.table_1', 'tenant_id');
SELECT create_distributed_table('comment_migration_table.table_2', 'tenant_id', colocate_with=>'comment_migration_table.table_1');
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

SELECT undistribute_table('comment_migration_table.table_1');
SELECT undistribute_table('comment_migration_table.table_2');
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

SELECT create_distributed_table('comment_migration_table.table_1', 'tenant_id');
SELECT create_distributed_table('comment_migration_table.table_2', 'tenant_id', colocate_with=>'comment_migration_table.table_1');
select obj_description('comment_migration_table.table_1'::regclass);
select obj_description('comment_migration_table.table_2'::regclass);

DROP TABLE comment_migration_table.table_2;
DROP TABLE comment_migration_table.table_1;
DROP SCHEMA comment_migration_table;
