SELECT count(*) >= 1 as coordinator_exists  FROM pg_dist_node WHERE groupid = 0 AND isactive;
