session "s1"

step "enable-mx-by-default"
{
  ALTER SYSTEM SET citus.enable_metadata_sync TO ON;
}

step "reload"
{
  SELECT pg_reload_conf();
}

step "start-metadata-sync"
{
  SELECT start_metadata_sync_to_node(nodename, nodeport) FROM pg_dist_node WHERE isactive = 't' and noderole = 'primary';
}

permutation "enable-mx-by-default" "reload" "start-metadata-sync"
