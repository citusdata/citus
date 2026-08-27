CREATE SCHEMA comment_on_table_and_column;
SET search_path TO comment_on_table_and_column;

create table tbl (a int, b text);
comment on table tbl is 'table comment';
comment on column tbl.b is 'column b comment';
select col_description('tbl'::regclass,0) as table_comment, col_description('tbl'::regclass,1) as column1_comment, col_description('tbl'::regclass,2) as column2_comment;

select create_distributed_table('tbl','a');
select col_description('tbl'::regclass,0) as table_comment, col_description('tbl'::regclass,1) as column1_comment, col_description('tbl'::regclass,2) as column2_comment;

select undistribute_table('tbl');
select col_description('tbl'::regclass,0) as table_comment, col_description('tbl'::regclass,1) as column1_comment, col_description('tbl'::regclass,2) as column2_comment;

DROP SCHEMA comment_on_table_and_column CASCADE;
