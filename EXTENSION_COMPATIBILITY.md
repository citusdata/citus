# PostgreSQL Extension Compatibility with Citus

Below table is created with Citus 15.0-1 on PG18.
| Extension Name               | Works as Expected   | Notes   |
|:-----------------------------|:--------------------|:--------|
| address_standardizer         | Yes                 |         |
| address_standardizer_data_us | Yes                 |         |
| age                          | Partially           | Works fine side by side, but graph data cannot be distributed. |
| amcheck                      | Yes                 |         |
| anon                         | Partially           | Works on local tables. Distributed INSERTs using the extension's faking functions require `anon.init()` on every worker. Anonymization functions cannot be used in distributed UPDATE statements. |
| auto_explain                 | No                  | [Issue #6448](https://github.com/citusdata/citus/issues/6448) |
| azure                        | Yes                 |         |
| azure_ai                     | Yes                 |         |
| azure_storage                | Yes                 |         |
| bloom                        | Yes                 |         |
| Btree_gin                    | Yes                 |         |
| btree_gist                   | Yes                 |         |
| citext                       | Yes                 |         |
| Citus_columnar               | Yes                 |         |
| credcheck                    | Yes                 | Enable `credcheck.encrypted_password_allowed` when adding workers because Citus propagates encrypted role passwords. |
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
| ip4r                         | Yes                 |         |
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
| pg_failover_slots            | Partially           | After failover, manually update `pg_dist_node` to point to the promoted workers. |
| pg_freespacemap              | Partially           | Users can set citus.override_table_visibility='off'; to get accurate calculation of free space map. |
| pg_hint_plan                 | Partially           | Works fine side by side, but hints are ignored for distributed queries |
| pg_ivm                       | Partially           | Works on local tables. IMMVs on distributed tables are not maintained because the extension relies on triggers. |
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
| pglogical                    | Partially           | Replicates local tables, but not reference or distributed tables. |
| pgrouting                    | Yes                 |         |
| pgrowlocks                   | Partially           | It works only with individual shards, not with distributed table names. |
| pgstattuple                  | Yes                 |         |
| plpgsql                      | Yes                 |         |
| plpgsql_check                | Yes                 |         |
| plv8                         | Yes                 |         |
| pointcloud                   | Yes                 | Works on distributed tables after manually synchronizing `pointcloud_formats` metadata to every node. |
| postgis                      | Yes                 |         |
| postgis_raster               | Yes                 |         |
| postgis_sfcgal               | Yes                 |         |
| postgis_tiger_geocoder       | Partially           | Works on local tables. Distributed queries do not work because TIGER datasets are coordinator-local and cannot be distributed using reference tables. |
| postgis_topology             | Partially           | Works on local tables and with distributed geometries on the coordinator. Topology metadata and managed tables are not propagated to workers. |
| postgres_fdw                 | Yes                 |         |
| postgres_protobuf            | Yes                 |         |
| rdkit                        | Partially           | RDKit types can be stored and queried in distributed tables, but cannot be used as distribution keys. |
| semver                       | Yes                 |         |
| session_variable             | Partially           | Works on local tables. Session variables are node-local and cannot be used directly in distributed queries. Coordinator-evaluated values can be passed via a CTE. |
| sslinfo                      | Yes                 |         |
| tablefunc                    | Yes                 |         |
| tdigest                      | Yes                 |         |
| tds_fdw                      | Yes                 |         |
| temporal_tables              | Partially           | Works on local tables. History tracking does not work on distributed tables because the extension relies on triggers. |
| timescaledb                  | No                  | [Known to be incompatible with Citus](https://www.citusdata.com/blog/2021/10/22/how-to-scale-postgres-for-time-series-data-with-citus/#:~:text=Postgres%E2%80%99%20built-in%20partitioning) |
| topn                         | Yes                 |         |
| tsm_system_rows              | Yes                 |         |
| tsm_system_time              | Yes                 |         |
| unaccent                     | Yes                 |         |
| uuid-ossp                    | Yes                 |         |
| vector (aka pg_vector)       | Yes                 |         |
| wal2json                     | Partially           | Works on local tables. For distributed tables, the coordinator slot captures only Citus transaction metadata; row changes must be decoded on each worker and use shard-table names. |
| xml2                         | Yes                 | The extension is [deprecated](https://www.postgresql.org/docs/current/xml2.html), use core SQL/XML functionality instead. |
