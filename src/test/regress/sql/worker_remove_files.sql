-- Clear job directory used by previous tests

\set JobId 201010

SELECT task_tracker_cleanup_job(:JobId);
