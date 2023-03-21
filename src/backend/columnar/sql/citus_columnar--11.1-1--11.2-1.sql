-- citus_columnar--11.1-1--11.2-1

#include "udfs/columnar_ensure_am_depends_catalog/11.2-1.sql"

DELETE FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid
    AND objid IN (select oid from pg_am where amname = 'columnar')
    AND objsubid = 0
    AND refclassid = 'pg_class'::regclass::oid
    AND refobjid IN (
        'columnar_internal.stripe_first_row_number_idx'::regclass::oid,
        'columnar_internal.chunk_group_pkey'::regclass::oid,
        'columnar_internal.chunk_pkey'::regclass::oid,
        'columnar_internal.options_pkey'::regclass::oid,
        'columnar_internal.stripe_first_row_number_idx'::regclass::oid,
        'columnar_internal.stripe_pkey'::regclass::oid
    )
    AND refobjsubid = 0
    AND deptype = 'n';
