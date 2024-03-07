-- create function
CREATE OR REPLACE FUNCTION public.test_err(v1 text)
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
 lNewValues = public.test_err(v1 => case when val::text = 'test'::text then 'yes' else 'no' end);
 raise notice 'lNewValues= %', lNewValues;
end;$$ ;

-- call function
SELECT public.test_err('test');