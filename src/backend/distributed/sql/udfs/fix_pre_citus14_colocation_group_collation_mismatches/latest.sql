CREATE OR REPLACE FUNCTION pg_catalog.fix_pre_citus14_colocation_group_collation_mismatches()
RETURNS VOID AS $func$
DECLARE
    v_colocationid oid;
    v_tables_to_move_out_grouped_by_collation json;
    v_collationid oid;
    v_tables_to_move_out oid[];
    v_table_to_move_out oid;
    v_first_table oid;
BEGIN
    SET LOCAL search_path TO pg_catalog;

    FOR v_colocationid, v_tables_to_move_out_grouped_by_collation
    IN
       WITH colocation_groups_and_tables_with_collation_mismatches AS (
           SELECT pdc.colocationid, pa.attcollation as distkeycollation, pdp.logicalrelid
             FROM pg_dist_colocation pdc
             JOIN pg_dist_partition pdp
               ON pdc.colocationid = pdp.colocationid
             JOIN pg_attribute pa
               ON pa.attrelid = pdp.logicalrelid
              AND pa.attname = column_to_column_name(pdp.logicalrelid, pdp.partkey)
                  -- column_to_column_name() returns NULL if partkey is NULL, so we're already
                  -- implicitly ignoring the tables that don't have a distribution column, such
                  -- as reference tables, but let's still explicitly discard such tables below.
            WHERE pdp.partkey IS NOT NULL
                  -- ignore the table if its distribution column collation matches the collation saved for the colocation group
              AND pdc.distributioncolumncollation != pa.attcollation
       )
       SELECT
           colocationid,
           json_object_agg(distkeycollation, rels) AS tables_to_move_out_grouped_by_collation
       FROM (
               SELECT colocationid,
                      distkeycollation,
                      array_agg(logicalrelid::oid) AS rels
                 FROM colocation_groups_and_tables_with_collation_mismatches
             GROUP BY colocationid, distkeycollation
       ) q
       GROUP BY colocationid
    LOOP
        RAISE DEBUG 'Processing colocation group with id %', v_colocationid;

        FOR v_collationid, v_tables_to_move_out
        IN
              SELECT key::oid AS collationid,
                     array_agg(elem::oid) AS tables_to_move_out
                FROM json_each(v_tables_to_move_out_grouped_by_collation) AS e(key, value),
             LATERAL json_array_elements_text(e.value) AS elem
            GROUP BY key
        LOOP
            RAISE DEBUG 'Moving out tables with collation id % from colocation group %', v_collationid, v_colocationid;

            v_first_table := NULL;

            FOR v_table_to_move_out IN SELECT unnest(v_tables_to_move_out)
            LOOP
                IF v_first_table IS NULL then
                    -- Move the first table out to start a new colocation group.
                    --
                    -- Could check if there is an appropriate colocation group to move to instead of 'none',
                    -- but this won't be super easy. Plus, even if we had such a colocation group, the user
                    -- was anyways okay with having this in a different colocation group in the first place.

                    RAISE DEBUG 'Moving out table with oid % to a new colocation group', v_table_to_move_out;
                    PERFORM update_distributed_table_colocation(v_table_to_move_out, colocate_with => 'none');

                    -- save the first table to colocate the rest of the tables with it
                    v_first_table := v_table_to_move_out;
                ELSE
                    -- Move the rest of the tables to colocate with the first table.

                    RAISE DEBUG 'Moving out table with oid % to colocate with table with oid %', v_table_to_move_out, v_first_table;
                    PERFORM update_distributed_table_colocation(v_table_to_move_out, colocate_with => v_first_table::regclass::text);
                END IF;
            END LOOP;
        END LOOP;
    END LOOP;
END;
$func$
LANGUAGE plpgsql;
COMMENT ON FUNCTION pg_catalog.fix_pre_citus14_colocation_group_collation_mismatches()
  IS 'Fix distributed tables whose colocation group collations do not match their distribution columns by moving them to new colocation groups';
