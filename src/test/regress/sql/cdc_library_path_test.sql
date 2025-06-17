-- Test for CDC library path adjustment functionality
-- This test verifies that the AdjustDynamicLibraryPathForCdcDecoders function
-- correctly modifies the dynamic_library_path when CDC is enabled

-- Save current settings
\set VERBOSITY terse

-- Show initial dynamic_library_path
SHOW dynamic_library_path;

-- Test 1: Verify CDC is initially disabled
SHOW citus.enable_change_data_capture;

-- Test 2: Check that dynamic_library_path is default
SELECT current_setting('dynamic_library_path') AS initial_path;

-- Test 3: Enable CDC and check if library path is adjusted
SET citus.enable_change_data_capture = true;

-- Check if the path has been modified (should include citus_decoders)
SHOW dynamic_library_path;

-- Test 4: Verify the path contains citus_decoders
SELECT 
    CASE 
        WHEN current_setting('dynamic_library_path') LIKE '%citus_decoders%' 
        THEN 'CDC path correctly set'
        ELSE 'CDC path not set'
    END AS cdc_path_status;

-- Test 5: Disable CDC
SET citus.enable_change_data_capture = false;

-- Test 6: Test with custom library path
SET dynamic_library_path = '$libdir/custom_lib:$libdir';
SET citus.enable_change_data_capture = true;

-- Verify that custom paths are preserved when CDC is enabled
SHOW dynamic_library_path;

-- Test 7: Reset to defaults
RESET citus.enable_change_data_capture;
RESET dynamic_library_path;

-- Final verification
SHOW citus.enable_change_data_capture;
SHOW dynamic_library_path;

-- Test edge cases
-- Test 8: Test with empty library path
SET dynamic_library_path = '';
SET citus.enable_change_data_capture = true;
SHOW dynamic_library_path;

-- Test 9: Test with non-standard library path
SET dynamic_library_path = '/custom/path';
SET citus.enable_change_data_capture = true;
SHOW dynamic_library_path;

-- Clean up
RESET citus.enable_change_data_capture;
RESET dynamic_library_path;
