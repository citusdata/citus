CREATE SCHEMA function_with_case;
SET search_path TO function_with_case;

-- create function
CREATE OR REPLACE FUNCTION test_err(v1 text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

begin
  return v1 || ' - ok';
END;
$function$;
do $$ declare
 lNewValues text;
 val text;
begin
 val = 'test';
 lNewValues = test_err(v1 => case when val::text = 'test'::text then 'yes' else 'no' end);
 raise notice 'lNewValues= %', lNewValues;
end;$$ ;

-- call function
SELECT test_err('test');

DROP SCHEMA function_with_case CASCADE;
