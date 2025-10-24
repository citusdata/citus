-- Test for CDC library path adjustment functionality
-- This test verifies that the AdjustDynamicLibraryPathForCdcDecoders function with non-superuser privileges
-- correctly modifies the dynamic_library_path when CDC is enabled

-- Test 1: Non-superuser with read_all_settings can see dynamic_library_path changes
CREATE USER cdc_test_user;
GRANT pg_read_all_settings TO cdc_test_user;
SET ROLE cdc_test_user;

-- Non-superuser should be able to see the current dynamic_library_path
SELECT current_setting('dynamic_library_path') AS user_visible_path;

SET citus.enable_change_data_capture = true;

SHOW citus.enable_change_data_capture;
SHOW dynamic_library_path;

-- Reset to superuser and cleanup
RESET ROLE;
DROP USER cdc_test_user;

-- Final cleanup
RESET citus.enable_change_data_capture;
RESET dynamic_library_path;

SHOW citus.enable_change_data_capture;
SHOW dynamic_library_path;
