
-- if the output of following query changes, we might need to change
-- some heap_getattr() calls to heap_deform_tuple().
SELECT attrelid::regclass, attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE atthasmissing
ORDER BY attrelid, attname;
