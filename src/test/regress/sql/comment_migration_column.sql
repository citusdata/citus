CREATE SCHEMA comment_migration_column;

CREATE TABLE comment_migration_column.table_1
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

comment on column comment_migration_column.table_1.id is 'table id';
comment on column comment_migration_column.table_1.code is 'table code';
comment on column comment_migration_column.table_1.name is 'table name';
comment on column comment_migration_column.table_1.date_created is 'table date_created';
comment on column comment_migration_column.table_1.active is 'table active';
select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);

SELECT create_distributed_table('comment_migration_column.table_1', 'id');
select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);

select undistribute_table('comment_migration_column.table_1');
select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);

SELECT create_distributed_table('comment_migration_column.table_1', 'id');
select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);

DROP TABLE comment_migration_column.table_1;
DROP SCHEMA comment_migration_column;
