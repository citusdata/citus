CREATE OR REPLACE FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_group_row_limit bool DEFAULT false,
    stripe_row_limit bool DEFAULT false,
    compression bool DEFAULT false,
    compression_level bool DEFAULT false)
    RETURNS void
    LANGUAGE plpgsql AS
$alter_columnar_table_reset$
declare
  noop BOOLEAN := true;
  cmd  TEXT    := 'ALTER TABLE ' || table_name::text || ' RESET (';
begin
  if (chunk_group_row_limit) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd   || 'columnar.chunk_group_row_limit';
    noop := false;
  end if;
  if (stripe_row_limit) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.stripe_row_limit';
    noop := false;
  end if;
  if (compression) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression';
    noop := false;
  end if;
  if (compression_level) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression_level';
    noop := false;
  end if;
  cmd := cmd || ')';
  if (not noop) then
    execute cmd;
  end if;
  return;
end;
$alter_columnar_table_reset$;

COMMENT ON FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_group_row_limit bool,
    stripe_row_limit bool,
    compression bool,
    compression_level bool)
IS 'reset on or more options on a columnar table to the system defaults';
