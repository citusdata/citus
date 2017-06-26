-- File to create functions and helpers needed for subsequent tests

-- create a helper function to create objects on each node
CREATE FUNCTION run_command_on_master_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- The following views are intended as alternatives to \d commands, whose
-- output changed in PostgreSQL 10. In particular, they must be used any time
-- a test wishes to print out the structure of a relation, which previously
-- was safely accomplished by a \d invocation.
SELECT run_command_on_master_and_workers(
$desc_views$
CREATE VIEW table_fkey_cols AS
SELECT rc.constraint_name AS "name",
       kcu.column_name AS "column_name",
       uc_kcu.column_name AS "refd_column_name",
       format('%I.%I', kcu.table_schema, kcu.table_name)::regclass::oid AS relid,
       format('%I.%I', uc_kcu.table_schema, uc_kcu.table_name)::regclass::oid AS refd_relid
FROM information_schema.referential_constraints rc,
     information_schema.key_column_usage kcu,
     information_schema.key_column_usage uc_kcu
WHERE rc.constraint_schema = kcu.constraint_schema AND
      rc.constraint_name = kcu.constraint_name AND
      rc.unique_constraint_schema = uc_kcu.constraint_schema AND
      rc.unique_constraint_name = uc_kcu.constraint_name;

CREATE VIEW table_fkeys AS
SELECT name AS "Constraint",
       format('FOREIGN KEY (%s) REFERENCES %s(%s)',
              string_agg(DISTINCT quote_ident(column_name), ', '),
              string_agg(DISTINCT refd_relid::regclass::text, ', '),
              string_agg(DISTINCT quote_ident(refd_column_name), ', ')) AS "Definition",
       "relid"
FROM table_fkey_cols
GROUP BY (name, relid);

CREATE VIEW table_attrs AS
SELECT c.column_name AS "name",
       c.data_type AS "type",
       CASE
            WHEN character_maximum_length IS NOT NULL THEN
                 format('(%s)', character_maximum_length)
            WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL THEN
                 format('(%s,%s)', numeric_precision, numeric_scale)
            ELSE ''
       END AS "modifier",
       c.column_default AS "default",
       (NOT c.is_nullable::boolean) AS "notnull",
       format('%I.%I', c.table_schema, c.table_name)::regclass::oid AS "relid"
FROM information_schema.columns AS c
ORDER BY ordinal_position;

CREATE VIEW table_desc AS
SELECT "name" AS "Column",
       "type" || "modifier" AS "Type",
       rtrim((
           CASE "notnull"
               WHEN true THEN 'not null '
               ELSE ''
           END
       ) || (
           CASE WHEN "default" IS NULL THEN ''
               ELSE 'default ' || "default"
           END
       )) AS "Modifiers",
	   "relid"
FROM table_attrs;

CREATE VIEW table_checks AS
SELECT cc.constraint_name AS "Constraint",
       ('CHECK ' || regexp_replace(check_clause, '^\((.*)\)$', '\1')) AS "Definition",
       format('%I.%I', ccu.table_schema, ccu.table_name)::regclass::oid AS relid
FROM information_schema.check_constraints cc,
     information_schema.constraint_column_usage ccu
WHERE cc.constraint_schema = ccu.constraint_schema AND
      cc.constraint_name = ccu.constraint_name
ORDER BY cc.constraint_name ASC;
$desc_views$
);
