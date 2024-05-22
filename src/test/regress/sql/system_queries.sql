-- The following query retrieves the foreign key constraints of the table "pg_dist_background_job"
-- along with their details. This modification includes a fix for a null pointer exception that occurred
-- in the "HasRangeTableRef" method of "worker_shard_visibility". The issue was resolved with PR #7604.
select
    ct.conname as constraint_name,
    a.attname as column_name,
    fc.relname as foreign_table_name,
    fns.nspname as foreign_table_schema
from
    (SELECT ct.conname, ct.conrelid, ct.confrelid, ct.conkey, ct.contype,
ct.confkey, generate_subscripts(ct.conkey, 1) AS s
       FROM pg_constraint ct
    ) AS ct
    inner join pg_class c on c.oid=ct.conrelid
    inner join pg_namespace ns on c.relnamespace=ns.oid
    inner join pg_attribute a on a.attrelid=ct.conrelid and a.attnum =
ct.conkey[ct.s]
    left join pg_class fc on fc.oid=ct.confrelid
    left join pg_namespace fns on fc.relnamespace=fns.oid
    left join pg_attribute fa on fa.attrelid=ct.confrelid and fa.attnum =
ct.confkey[ct.s]
where
    ct.contype='f'
     and fc.relname='pg_dist_background_job'
    and ns.nspname='pg_catalog'
order by
    fns.nspname, fc.relname, a.attnum;
