CREATE SCHEMA comment_migration_column;

CREATE TABLE comment_migration_column.table_1
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

CREATE TABLE comment_migration_column.table_2
(
    id            bigserial,
    table_1_id    bigint ,
    description   varchar(200),
    owner_id     bigint,
    CONSTRAINT table_2_pkey PRIMARY KEY (id)
)
    WITH (autovacuum_enabled = TRUE);

SET citus.shard_replication_factor = 1;

comment on column comment_migration_column.table_1.id is 'table 1 id';
comment on column comment_migration_column.table_1.code is 'table 1 code';
comment on column comment_migration_column.table_1.name is 'table 1 name';
comment on column comment_migration_column.table_1.date_created is 'table 1 date_created';
comment on column comment_migration_column.table_1.active is 'table 1 active';

comment on column comment_migration_column.table_2.id is 'table 2 id';
comment on column comment_migration_column.table_2.table_1_id is 'table 2 foreign key to table 1';
comment on column comment_migration_column.table_2.description is 'table 2 description';

select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);
select col_description('comment_migration_column.table_2'::regclass,1), col_description('comment_migration_column.table_2'::regclass,2), col_description('comment_migration_column.table_2'::regclass,3);

SELECT create_reference_table('comment_migration_column.table_1');
SELECT create_distributed_table('comment_migration_column.table_2', 'id');
ALTER TABLE comment_migration_column.table_2 ADD CONSTRAINT table2_table1_fk FOREIGN KEY (table_1_id) REFERENCES comment_migration_column.table_1(id);

SELECT undistribute_table('comment_migration_column.table_1', cascade_via_foreign_keys=>true);

ALTER TABLE comment_migration_column.table_2 DROP CONSTRAINT table2_table1_fk;
SELECT create_reference_table('comment_migration_column.table_1');
SELECT create_distributed_table('comment_migration_column.table_2', 'id');
ALTER TABLE comment_migration_column.table_2 ADD CONSTRAINT table2_table1_fk FOREIGN KEY (table_1_id) REFERENCES comment_migration_column.table_1(id);

select col_description('comment_migration_column.table_1'::regclass,1), col_description('comment_migration_column.table_1'::regclass,2), col_description('comment_migration_column.table_1'::regclass,3), col_description('comment_migration_column.table_1'::regclass,4), col_description('comment_migration_column.table_1'::regclass,5);
select col_description('comment_migration_column.table_2'::regclass,1), col_description('comment_migration_column.table_2'::regclass,2), col_description('comment_migration_column.table_2'::regclass,3);

DROP TABLE comment_migration_column.table_2;
DROP TABLE comment_migration_column.table_1;
DROP SCHEMA comment_migration_column;
