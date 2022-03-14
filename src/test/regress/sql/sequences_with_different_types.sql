CREATE SCHEMA sequences_with_different_types;
SET search_path TO sequences_with_different_types;

CREATE TYPE two_big_ints AS (a bigint, b bigint);
-- by default, sequences get bigint type
CREATE SEQUENCE bigint_sequence_1;
CREATE SEQUENCE bigint_sequence_2 START 10000;
CREATE SEQUENCE bigint_sequence_3 INCREMENT 10;
CREATE SEQUENCE bigint_sequence_4 MINVALUE 1000000;
CREATE SEQUENCE bigint_sequence_5;
CREATE SEQUENCE bigint_sequence_8;

CREATE TABLE table_1
(
	user_id bigint,
	user_code_1 text DEFAULT (('CD'::text || lpad(nextval('bigint_sequence_1'::regclass)::text, 10, '0'::text))),
	user_code_2 text DEFAULT nextval('bigint_sequence_2'::regclass)::text,
	user_code_3 text DEFAULT (nextval('bigint_sequence_3'::regclass) + 1000)::text,
	user_code_4 float DEFAULT nextval('bigint_sequence_4'::regclass),
	user_code_5 two_big_ints DEFAULT (nextval('bigint_sequence_5'::regclass), nextval('bigint_sequence_5'::regclass)),
	user_code_8 jsonb DEFAULT to_jsonb('test'::text) || to_jsonb(nextval('bigint_sequence_8'::regclass))

);
SELECT create_distributed_table('table_1', 'user_id');

INSERT INTO table_1 VALUES (1, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT), (2, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT) RETURNING *;

\c - - - :worker_1_port
SET search_path TO sequences_with_different_types;

INSERT INTO table_1 VALUES (3, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT), (4, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT) RETURNING *;


\c - - - :master_port
SET search_path TO sequences_with_different_types;
CREATE SEQUENCE bigint_sequence_6;

CREATE TABLE table_2
(
	user_id bigint,
	user_code OID DEFAULT nextval('bigint_sequence_6'::regclass)
);
SELECT create_distributed_table('table_2', 'user_id');

-- on the coordinator, the sequence starts from 0
INSERT INTO table_2 VALUES (1, DEFAULT) RETURNING *;

\c - - - :worker_1_port
SET search_path TO sequences_with_different_types;

-- this fails because on the workers the start value of the sequence
-- is greater than the largest value of an oid
INSERT INTO table_2 VALUES (1, DEFAULT) RETURNING *;

\c - - - :master_port
SET search_path TO sequences_with_different_types;
CREATE SEQUENCE bigint_sequence_7;

CREATE TABLE table_3
(
	user_id bigint,
	user_code boolean DEFAULT ((nextval('bigint_sequence_7'::regclass)%2)::int)::boolean
);
SELECT create_distributed_table('table_3', 'user_id');

INSERT INTO table_3 VALUES (1, DEFAULT), (2, DEFAULT) RETURNING *;

\c - - - :worker_1_port
SET search_path TO sequences_with_different_types;
INSERT INTO table_3 VALUES (3, DEFAULT), (4, DEFAULT) RETURNING *;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA sequences_with_different_types CASCADE;
