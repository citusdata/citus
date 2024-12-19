// Isolation tests for checking the progress monitoring infrastructure
// We create three different processes, two of the type "1337" and one of type "3778"
// We utilize advisory locks to control steps of the processes
// Different locks are held for each step so that the processes stop at each step and
// we can see their progress.

setup
{
	CREATE FUNCTION create_progress(bigint, bigint)
	RETURNS void
	AS 'citus'
	LANGUAGE C STRICT;

	CREATE FUNCTION update_progress(bigint, bigint)
	RETURNS void
	AS 'citus'
	LANGUAGE C STRICT;

	CREATE FUNCTION finish_progress()
	RETURNS void
	AS 'citus'
	LANGUAGE C STRICT;

	CREATE OR REPLACE FUNCTION show_progress(bigint)
	RETURNS TABLE(step int, progress bigint)
	AS 'citus'
	LANGUAGE C STRICT;

	CREATE FUNCTION sample_operation(command_type bigint, lockid bigint, progress bigint)
		RETURNS VOID AS $$
	BEGIN
		PERFORM create_progress(command_type, 2);
		PERFORM pg_advisory_xact_lock(lockid);

		PERFORM update_progress(0, progress);
		PERFORM pg_advisory_xact_lock(lockid + 1);

		PERFORM update_progress(1, progress);
		PERFORM pg_advisory_xact_lock(lockid + 2);

		PERFORM finish_progress();
	END;
	$$ LANGUAGE 'plpgsql';
}

teardown
{
	DROP FUNCTION IF EXISTS create_progress(bigint, bigint);
	DROP FUNCTION IF EXISTS update_progress(bigint, bigint);
	DROP FUNCTION IF EXISTS finish_progress();
	DROP FUNCTION IF EXISTS show_progress(bigint);
	DROP FUNCTION IF EXISTS sample_operation(bigint, bigint, bigint);
}


session "s1"

step "s1-start-operation"
{
	SELECT sample_operation(1337, 10, -1);
}


session "s2"

step "s2-start-operation"
{
	SELECT sample_operation(1337, 20, 2);
}


session "s3"

step "s3-start-operation"
{
	SELECT sample_operation(3778, 30, 9);
}


session "lock-orchestrator"

step "take-locks"
{
	-- Locks for steps of sample operation in s1
	SELECT pg_advisory_lock(10);
	SELECT pg_advisory_lock(11);
	SELECT pg_advisory_lock(12);

	-- Locks for steps of sample operation in s2
	SELECT pg_advisory_lock(20);
	SELECT pg_advisory_lock(21);
	SELECT pg_advisory_lock(22);

	-- Locks for steps of sample operation in s3
	SELECT pg_advisory_lock(30);
	SELECT pg_advisory_lock(31);
	SELECT pg_advisory_lock(32);
}

step "release-locks-1"
{
	-- Release the locks of first steps of sample operations
	SELECT pg_advisory_unlock(10);
	SELECT pg_advisory_unlock(20);
	SELECT pg_advisory_unlock(30);
}

step "release-locks-2"
{
	-- Release the locks of second steps of sample operations
	SELECT pg_advisory_unlock(11);
	SELECT pg_advisory_unlock(21);
	SELECT pg_advisory_unlock(31);
}

step "release-locks-3"
{
	-- Release the locks of final steps of sample operations
	SELECT pg_advisory_unlock(12);
	SELECT pg_advisory_unlock(22);
	SELECT pg_advisory_unlock(32);
}


session "monitor"

step "show-progress"
{
	SELECT step, progress FROM show_progress(1337) ORDER BY 1, 2;
	SELECT step, progress FROM show_progress(3778) ORDER BY 1, 2;
}

permutation "take-locks" "s1-start-operation" "s2-start-operation" "s3-start-operation" "show-progress" "release-locks-1" "show-progress" "release-locks-2" "show-progress" "release-locks-3"
