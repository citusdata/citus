-- Tests related to create type

CREATE TYPE create_order_delegator_t AS (
     out_w_tax decimal(4, 4),
     out_d_tax decimal(4, 4),
     out_o_id integer,
     out_c_discount decimal(4, 4),
     out_c_last varchar(16),
     out_c_credit char(2),
     out_o_entry_d timestamp
);

\d create_order_delegator_t

\c - - - :worker_1_port
\d create_order_delegator_t
