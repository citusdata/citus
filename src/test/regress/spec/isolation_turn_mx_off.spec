session "s1"

step "disable-mx-by-default"
{
  ALTER SYSTEM SET citus.enable_metadata_sync TO OFF;
}

step "reload"
{
  SELECT pg_reload_conf();
}

step "stop-metadata-sync"
{
  SELECT stop_metadata_sync_to_node(nodename, nodeport) FROM pg_dist_node WHERE isactive = 't' and noderole = 'primary';
}

permutation "disable-mx-by-default" "reload" "stop-metadata-sync"
