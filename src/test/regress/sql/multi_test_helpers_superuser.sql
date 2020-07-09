-- get size of TopTransactionContext
CREATE OR REPLACE FUNCTION top_transaction_context_size() RETURNS BIGINT
LANGUAGE C STRICT VOLATILE
AS 'citus', $$top_transaction_context_size$$;
