-- setup schema used for sqlsmith runs
-- source: https://gist.github.com/will/e8a1e6efd46ac82f1b61d0c0ccab1b52
CREATE SCHEMA sqlsmith_failures;
SET search_path TO sqlsmith_failures, public;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 1280000;

begin;

SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

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

-- pg12 and above support generated columns
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

-- data loading takes ~30 seconds, lets hope we can skip this for all reproductions. When
-- there is a sqlsmith failure that needs the data we can uncomment the block below.

-- insert into orgs(id, name) select  i,'org-'||i from generate_series(1,1000) i;
-- insert into users(id, name, org_id, country_id) select i,'user-'||i, i%1000+1, (i%90)+1  from generate_series(1,100000) i;
-- insert into orders(id, org_id, user_id, price) select i, ((i%100000+1)%1000)+1 , i%100000+1, i/100 from generate_series(1,1000000) i;
-- insert into events(id, org_id, user_id, event_type) select i, ((i%100000+1)%1000)+1 , i%100000+1, i/100 from generate_series(1,1000000) i;
-- insert into local_data(id) select generate_series(1,1000);

commit;

-- SQL SMITH ASSERTION FAILURE https://github.com/citusdata/citus/issues/3809
-- Root cause: pruned worker columns not projected correctly on coordinator causing an assertion in the postgres standard planner
select
    case when pg_catalog.bit_or(cast(cast(coalesce(cast(null as "bit"), cast(null as "bit")) as "bit") as "bit")) over (partition by subq_0.c3 order by subq_0.c0) <> cast(null as "bit")
        then subq_0.c3
        else subq_0.c3
    end as c0,
    30 as c1,
    subq_0.c2 as c2
from
    (select
         pg_catalog.websearch_to_tsquery(
             cast(pg_catalog.regconfigin(cast(cast(null as cstring) as cstring)) as regconfig),
             cast((select type from pg_catalog.pg_dist_object limit 1 offset 1) as text)
         ) as c0,
         sample_0.org_id as c1,
         sample_0.id as c2,
         sample_0.score as c3,
         sample_0.country_id as c4,
         sample_0.org_id as c5,
         sample_0.org_id as c6
     from
         sqlsmith_failures.users as sample_0 tablesample system (7.5)
     where sample_0.org_id is not NULL) as subq_0
where (select pg_catalog.array_agg(id) from sqlsmith_failures.countries)
          is not NULL;

-- cleanup
DROP SCHEMA sqlsmith_failures CASCADE;
