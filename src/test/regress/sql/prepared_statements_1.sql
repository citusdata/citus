SET search_path TO "prepared statements";

PREPARE repartition_prepared(int) AS
	SELECT
		count(*)
	FROM
		repartition_prepared_test t1
			JOIN
		repartition_prepared_test t2
			USING (b)
		WHERE t1.a = $1;

EXECUTE repartition_prepared (1);

BEGIN;
	-- CREATE TABLE ... AS EXECUTE prepared_statement tests
	CREATE TEMP TABLE repartition_prepared_tmp AS EXECUTE repartition_prepared(1);
	SELECT count(*) from repartition_prepared_tmp;
ROLLBACK;
