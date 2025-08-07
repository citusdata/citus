Below table is created with Citus 12.1.7 on PG16
| Extension Name               | Works as Expected   | Notes   |
|:-----------------------------|:--------------------|:--------|
| address_standardizer         | Yes                 |         |
| address_standardizer_data_us | Yes                 |         |
| age                          | Partially           | Works fine side by side, but graph data cannot be distributed. |
| amcheck                      | Yes                 |         |
| anon                         | Partially           | Cannot anonymize distributed tables. It is possible to anonymize local tables. |
| auto_explain                 | No                  | [Issue #6448](https://github.com/citusdata/citus/issues/6448) |
| azure                        | Yes                 |         |
| azure_ai                     | Yes                 |         |
| azure_storage                | Yes                 |         |
| bloom                        | Yes                 |         |
| Btree_gin                    | Yes                 |         |
| btree_gist                   | Yes                 |         |
| citext                       | Yes                 |         |
| Citus_columnar               | Yes                 |         |
| cube                         | Yes                 |         |
| dblink                       | Yes                 |         |
| dict_int                     | Yes                 |         |
| dict_xsyn                    | Yes                 |         |
| earthdistance                | Yes                 |         |
| fuzzystrmatch                | Yes                 |         |
| hll                          | Yes                 |         |
| hstore                       | Yes                 |         |
| hypopg                       | Partially           | Hypopg can work on local tables and individual shards, however, when we create a hypothetical index on a distributed table, citus does not propagate the index creation command to worker nodes, and thus, hypothetical index is not used in explain statements.         |
| intagg                       | Yes                 |         |
| intarray                     | Yes                 |         |
| isn                          | Yes                 |         |
| lo                           | Partially           | Extension relies on triggers, but Citus does not support triggers over distributed tables |
| login_hook                   | Yes                 |         |
| ltree                        | Yes                 |         |
| oracle_fdw                   | Yes                 |         |
| orafce                       | Yes                 |         |
| pageinspect                  | Yes                 |         |
| pg_buffercache               | Yes                 |         |
| pg_cron                      | Yes                 |         |
| pg_diskann                   | Yes                 |         |
| pg_failover_slots            | To be tested        |         |
| pg_freespacemap              | Partially           | Users can set citus.override_table_visibility='off'; to get accurate calculation of free space map. |
| pg_hint_plan                 | Partially           | Works fine side by side, but hints are ignored for distributed queries |
| pg_partman                   | Yes                 |         |
| pg_prewarm                   | Partially           | In order to prewarm distributed tables, set " citus.override_table_visibility" to off, and run prewarm for each shard. This needs to be done at each node. |
| pg_repack                    | Partially           | Extension relies on triggers, but Citus does not support triggers over distributed tables. It works fine on local tables. |
| pg_squeeze                   | Partially           | It can work on local tables, but it is not aware of distributed tables. Users can set citus.override_table_visibility='off'; and then run pg_squeeze for each shard. This needs to be done at each node. |
| pg_stat_statements           | Yes                 |         |
| pg_trgm                      | Yes                 |         |
| pg_visibility                | Partially           | In order to get visibility map of a distributed table, customers can run the functions for shard tables. |
| pgaadauth                    | Yes                 |         |
| pgaudit                      | Yes                 |         |
| pgcrypto                     | Yes                 |         |
| pglogical                    | No                  |         |
| pgrowlocks                   | Partially           | It works only with individual shards, not with distributed table names. |
| pgstattuple                  | Yes                 |         |
| plpgsql                      | Yes                 |         |
| plv8                         | Yes                 |         |
| postgis                      | Yes                 |         |
| postgis_raster               | Yes                 |         |
| postgis_sfcgal               | Yes                 |         |
| postgis_tiger_geocoder       | No                  |         |
| postgis_topology             | No                  |         |
| postgres_fdw                 | Yes                 |         |
| postgres_protobuf            | Yes                 |         |
| semver                       | Yes                 |         |
| session_variable             | No                  |         |
| sslinfo                      | Yes                 |         |
| tablefunc                    | Yes                 |         |
| tdigest                      | Yes                 |         |
| tds_fdw                      | Yes                 |         |
| timescaledb                  | No                  | [Known to be incompatible with Citus](https://www.citusdata.com/blog/2021/10/22/how-to-scale-postgres-for-time-series-data-with-citus/#:~:text=Postgres%E2%80%99%20built-in%20partitioning) |
| topn                         | Yes                 |         |
| tsm_system_rows              | Yes                 |         |
| tsm_system_time              | Yes                 |         |
| unaccent                     | Yes                 |         |
| uuid-ossp                    | Yes                 |         |
| vector (aka pg_vector)       | Yes                 |         |
| wal2json                     | To be tested        |         |
| xml2                         | To be tested        |         |
