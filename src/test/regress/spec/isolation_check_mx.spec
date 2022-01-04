session "s1"

step "check_mx"
{
    SHOW citus.enable_metadata_sync_by_default;

    SELECT bool_and(metadatasynced) FROM pg_dist_node WHERE noderole = 'primary';
}

permutation "check_mx"
