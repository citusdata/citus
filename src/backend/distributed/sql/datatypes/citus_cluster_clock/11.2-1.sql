--
-- cluster_clock base type is a combination of
-- uint64 cluster clock logical timestamp at the commit
-- uint32 cluster clock counter(ticks with in the logical clock)
--

CREATE TYPE citus.cluster_clock;

CREATE FUNCTION pg_catalog.cluster_clock_in(cstring)
    RETURNS citus.cluster_clock
    AS 'MODULE_PATHNAME',$$cluster_clock_in$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_out(citus.cluster_clock)
    RETURNS cstring
    AS 'MODULE_PATHNAME',$$cluster_clock_out$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_recv(internal)
   RETURNS citus.cluster_clock
   AS 'MODULE_PATHNAME',$$cluster_clock_recv$$
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_send(citus.cluster_clock)
   RETURNS bytea
   AS 'MODULE_PATHNAME',$$cluster_clock_send$$
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_logical(citus.cluster_clock)
    RETURNS bigint
    AS 'MODULE_PATHNAME',$$cluster_clock_logical$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE citus.cluster_clock (
    internallength = 12, -- specifies the size of the memory block required to hold the type uint64 + uint32
    input = cluster_clock_in,
    output = cluster_clock_out,
    receive = cluster_clock_recv,
    send = cluster_clock_send
);

ALTER TYPE citus.cluster_clock SET SCHEMA pg_catalog;
COMMENT ON TYPE cluster_clock IS 'combination of (logical, counter): 42 bits + 22 bits';

--
-- Define the required operators
--
CREATE FUNCTION pg_catalog.cluster_clock_lt(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_lt$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_le(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_le$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_eq(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_eq$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_ne(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_ne$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_ge(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_ge$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_catalog.cluster_clock_gt(cluster_clock, cluster_clock) RETURNS bool
    AS 'MODULE_PATHNAME',$$cluster_clock_gt$$
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_lt,
   commutator = > , negator = >= ,
   restrict = scalarltsel, join = scalarltjoinsel
);

CREATE OPERATOR <= (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_le,
   commutator = >= , negator = > ,
   restrict = scalarlesel, join = scalarlejoinsel
);

CREATE OPERATOR = (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_eq,
   commutator = = ,
   negator = <> ,
   restrict = eqsel, join = eqjoinsel
);

CREATE OPERATOR <> (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_ne,
   commutator = <> ,
   negator = = ,
   restrict = neqsel, join = neqjoinsel
);

CREATE OPERATOR >= (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_ge,
   commutator = <= , negator = < ,
   restrict = scalargesel, join = scalargejoinsel
);

CREATE OPERATOR > (
   leftarg = cluster_clock, rightarg = cluster_clock, procedure = cluster_clock_gt,
   commutator = < , negator = <= ,
   restrict = scalargtsel, join = scalargtjoinsel
);

-- Create the support function too
CREATE FUNCTION pg_catalog.cluster_clock_cmp(cluster_clock, cluster_clock) RETURNS int4
    AS 'MODULE_PATHNAME',$$cluster_clock_cmp$$
    LANGUAGE C IMMUTABLE STRICT;

-- Define operator class to be be used by an index for type cluster_clock.
CREATE OPERATOR CLASS pg_catalog.cluster_clock_ops
    DEFAULT FOR TYPE cluster_clock USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       cluster_clock_cmp(cluster_clock, cluster_clock);

--
-- Create sequences for logical and counter fields of the type cluster_clock, to
-- be used as a storage.
--
CREATE SEQUENCE citus.pg_dist_clock_logical_seq START 1;

ALTER SEQUENCE citus.pg_dist_clock_logical_seq SET SCHEMA pg_catalog;

REVOKE UPDATE ON SEQUENCE pg_catalog.pg_dist_clock_logical_seq FROM public;
