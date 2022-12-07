CREATE SCHEMA issue_6543;
SET search_path TO issue_6543;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 67322500;

CREATE TABLE event (
  tenant_id varchar,
  id bigint,
  primary key (tenant_id, id)
);

CREATE TABLE page (
  tenant_id varchar,
  id int,
  primary key (tenant_id, id)
);

SELECT create_distributed_table('event', 'tenant_id');
SELECT create_distributed_table('page', 'tenant_id', colocate_with => 'event');
alter table page add constraint fk21 foreign key (tenant_id, id) references event;

SET client_min_messages TO WARNING;
DROP SCHEMA issue_6543 CASCADE;
