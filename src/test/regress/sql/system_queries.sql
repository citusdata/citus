BEGIN;
SET LOCAL citus.show_shards_for_app_name_prefixes = '';
-- Describing a table reads pg_class but does not select any TOAST columns.
SELECT c.relname
FROM pg_class c LEFT JOIN pg_class toast ON c.reltoastrelid = toast.oid
WHERE c.oid = 'pg_catalog.pg_class'::regclass;

-- Both pg_class aliases can be nulled by the FULL join and again by the RIGHT join.
SELECT count(*) AS null_extended_rows
FROM (pg_class c FULL JOIN pg_class toast ON c.reltoastrelid = toast.oid)
RIGHT JOIN (VALUES (0::oid)) v(oid) ON c.oid = v.oid
WHERE c.oid IS NULL AND toast.oid IS NULL;

-- Find the foreign keys referencing pg_dist_background_job across nested FROM items.
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
END;
