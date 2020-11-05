
create or replace function trs_before() returns trigger language plpgsql as $$
BEGIN
  RAISE NOTICE 'BEFORE STATEMENT %', TG_OP;
  RETURN NULL;
END;
$$;

create or replace function trs_after() returns trigger language plpgsql as $$
DECLARE
   r RECORD;
BEGIN
   RAISE NOTICE 'AFTER STATEMENT %', TG_OP;
   IF (TG_OP = 'DELETE') THEN
     FOR R IN select * from old_table
     LOOP
        RAISE NOTICE '  (%)', r.i;
     END LOOP;
   ELSE
     FOR R IN select * from new_table
     LOOP
        RAISE NOTICE '  (%)', r.i;
     END LOOP;
   END IF;
   RETURN NULL;
END;
$$;

create or replace function trr_before() returns trigger language plpgsql as $$
BEGIN
   RAISE NOTICE 'BEFORE ROW %: (%)', TG_OP, NEW.i;
   RETURN NEW;
END;
$$;

create or replace function trr_after() returns trigger language plpgsql as $$
BEGIN
   RAISE NOTICE 'AFTER ROW %: (%)', TG_OP, NEW.i;
   RETURN NEW;
END;
$$;

create table test_tr(i int) using cstore_tableam;

create trigger tr_before_stmt before insert on test_tr
  for each statement execute procedure trs_before();
create trigger tr_after_stmt after insert on test_tr
  referencing new table as new_table
  for each statement execute procedure trs_after();

create trigger tr_before_row before insert on test_tr
  for each row execute procedure trr_before();

-- after triggers require TIDs, which are not supported yet
create trigger tr_after_row after insert on test_tr
  for each row execute procedure trr_after();

insert into test_tr values(1);
insert into test_tr values(2),(3),(4);

drop table test_tr;
