CREATE SCHEMA comment_migration_table;

CREATE TABLE comment_migration_table.table_1
(
    id            bigserial,
    code          varchar(200)                           not null,
    name          varchar(200),
    date_created  timestamp with time zone default NOW() not null,
    active        boolean                  default true,
    CONSTRAINT table_1_pkey PRIMARY KEY (id)
)
    WITH (autovacuum_enabled = TRUE);

SET citus.shard_replication_factor = 1;

comment on table comment_migration_table.table_1 is 'Table 1';
select obj_description('comment_migration_table.table_1'::regclass);

SELECT create_distributed_table('comment_migration_table.table_1', 'id');
select obj_description('comment_migration_table.table_1'::regclass);

select undistribute_table('comment_migration_table.table_1');
select obj_description('comment_migration_table.table_1'::regclass);

SELECT create_distributed_table('comment_migration_table.table_1', 'id');
select obj_description('comment_migration_table.table_1'::regclass);

DROP TABLE comment_migration_table.table_1;
DROP SCHEMA comment_migration_table;
