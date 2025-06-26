-- Test for CDC library path adjustment functionality
-- This test verifies that the AdjustDynamicLibraryPathForCdcDecoders function with superuser privileges
-- correctly modifies the dynamic_library_path when CDC is enabled

-- Test 1: Show initial state and reset to ensure clean state
SHOW dynamic_library_path;
SHOW citus.enable_change_data_capture;

-- Test 2: Enable CDC and verify path is set to include citus_decoders
SET citus.enable_change_data_capture = true;
SHOW dynamic_library_path;

-- Verify that the dynamic_library_path has been modified to include citus_decoders
SELECT
    CASE
        WHEN current_setting('dynamic_library_path') LIKE '%citus_decoders%'
        THEN 'CDC path correctly set'
        ELSE 'CDC path incorrectly not set'
    END AS cdc_path_status;

-- Test 3: Disable CDC and verify path remains (CDC doesn't remove the path)
SET citus.enable_change_data_capture = false;
SHOW dynamic_library_path;

-- Test 4: Edge case - function should only work when path is exactly "$libdir"
SET dynamic_library_path = '$libdir/other_path:$libdir';
SET citus.enable_change_data_capture = true;
SHOW dynamic_library_path;

-- Verify that path is unchanged with custom library path
SELECT
    CASE
        WHEN current_setting('dynamic_library_path') LIKE '%citus_decoders%'
        THEN 'CDC path incorrectly set'
        ELSE 'CDC path correctly not set'
    END AS custom_path_test;

-- Reset dynamic_library_path to default
RESET dynamic_library_path;
RESET citus.enable_change_data_capture;

-- Test 5: Verify that dynamic_library_path reset_val is overridden to $libdir/citus_decoders:$libdir
SHOW dynamic_library_path;
SHOW citus.enable_change_data_capture;


