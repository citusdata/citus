cmake_minimum_required(VERSION 3.7)

# Declare important cache variables for this build
set(CITUS_MAJORVERSION "7.4" CACHE STRING
    "Major-minor version identifying a release lineage." FORCE)

set(CITUS_PATCHLEVEL "devel" CACHE STRING
    "Patch level of this build; either an integer or 'devel'." FORCE)

set(CITUS_EDITION "Community" CACHE STRING "Edition of this build." FORCE)
set_property(CACHE CITUS_EDITION PROPERTY STRINGS Community Enterprise)

# Do some sanity checks on the above
if (NOT "${CITUS_MAJORVERSION}" MATCHES "^[0-9]+\\.[0-9]+$")
  message(FATAL_ERROR "CITUS_MAJORVERSION ('${CITUS_MAJORVERSION}') did not \
                       match the format <MAJOR>.<MINOR>.")
elseif (NOT "${CITUS_PATCHLEVEL}" MATCHES "^[0-9]+|devel$")
  message(FATAL_ERROR "CITUS_PATCHLEVEL ('${CITUS_PATCHLEVEL}') must be a \
                       nonnegative integer or 'devel'.")
elseif (NOT "${CITUS_EDITION}" MATCHES "^Community|Enterprise$")
  message(FATAL_ERROR "CITUS_EDITION ('${CITUS_EDITION}') must be 'Community' \
                      or 'Enterprise'.")
endif ()

set(CITUS_EXTRA_VERSION "" CACHE STRING
    "Information to be appended to version.")

# Set up a version for CMake and prepare our full version string
set(VERSION "${CITUS_MAJORVERSION}")
if ("${CITUS_PATCHLEVEL}" GREATER_EQUAL "0")
  string(APPEND VERSION ".${CITUS_PATCHLEVEL}")
  set(CITUS_VERSION "${VERSION}")
else ()
  set(CITUS_VERSION "${VERSION}${CITUS_PATCHLEVEL}")
endif ()

set(CITUS_VERSION "${CITUS_VERSION}${CITUS_EXTRA_VERSION}" CACHE STRING
    "Full version of this build." FORCE)

# Define the reporting URL
# TODO What is --with-reports-hostname?
set(REPORTS_BASE_URL "https://reports.citusdata.com" CACHE STRING
    "Base URL for statistics collection and update checks." FORCE)

# Actually declare our project
project(Citus VERSION "${VERSION}" DESCRIPTION "Citus distributed database" LANGUAGES C)

# Build the version string for SELECT citus_version()
set(CITUS_VERSION_STR
    "${PROJECT_NAME} ${CITUS_EDITION} ${CITUS_VERSION} on ${CMAKE_SYSTEM_NAME},")

math(EXPR BITS "8*${CMAKE_SIZEOF_VOID_P}")
string(APPEND CITUS_VERSION_STR
       " compiled by ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}, ${BITS}-bit")

set(CITUS_VERSION_STR "${CITUS_VERSION_STR}" CACHE STRING
    "Build's version number, platform, and C compiler." FORCE)

# devel builds count a zero for patch number
if (NOT (PROJECT_VERSION_PATCH))
  set(PROJECT_VERSION_PATCH "0")
endif ()

# Calculate and save numeric version representation
math(EXPR CITUS_VERSION_NUM "${PROJECT_VERSION_MAJOR} * 10000 + \
                             ${PROJECT_VERSION_MINOR} * 100 + \
                             ${PROJECT_VERSION_PATCH} * 1")

# Set a default build type if none was specified
# adapted from https://blog.kitware.com/cmake-and-the-default-build-type/
set(default_build_type "Release")

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Using default build type: '${default_build_type}'")

  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING
      "Choose the type of build." FORCE)

  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

# Attempt to find curl
# TODO This does not work on Windows installs
include(FindCURL)
set(HAVE_LIBCURL ${CURL_FOUND})

# Find pg_config if not already set, looking _only_ in PATH
find_program(PG_CONFIG NAMES "pg_config"
             DOC "Location to find pg_config (default PATH)."
             NO_CMAKE_PATH NO_CMAKE_ENVIRONMENT_PATH
             NO_CMAKE_SYSTEM_PATH NO_CMAKE_FIND_ROOT_PATH)

# Bail out early if we can't find pg_config
if (NOT PG_CONFIG)
  message(FATAL_ERROR "Could not find pg_config. Set PG_CONFIG or PATH.")
else ()
  message(STATUS "Using pg_config found at: ${PG_CONFIG}")
endif ()

# Get PostgreSQL configuration from pg_config
# TODO Make this more DRY
execute_process(COMMAND ${PG_CONFIG} --version
                OUTPUT_VARIABLE PGC_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --includedir
                OUTPUT_VARIABLE PGC_INCLUDEDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --includedir-server
                OUTPUT_VARIABLE PGC_INCLUDEDIR_SERVER
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --pkgincludedir
                OUTPUT_VARIABLE PGC_PKGINCLUDEDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --bindir
                OUTPUT_VARIABLE PGC_BINDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --cppflags
                OUTPUT_VARIABLE PGC_CPPFLAGS
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --cflags
                OUTPUT_VARIABLE PGC_CFLAGS
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --ldflags
                OUTPUT_VARIABLE PGC_LDFLAGS
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --libdir
                OUTPUT_VARIABLE PGC_LIBDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --pkglibdir
                OUTPUT_VARIABLE PGC_PKGLIBDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --sharedir
                OUTPUT_VARIABLE PGC_SHAREDIR
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --pgxs
                OUTPUT_VARIABLE PGC_PGXS
                OUTPUT_STRIP_TRAILING_WHITESPACE)

# Ensure target PostgreSQL version is known and supported
if ("${PGC_VERSION}" MATCHES "^PostgreSQL ([0-9]*).([0-9]*)(.([0-9]*))?")
  set(SUPPORTED_PGVERSIONS "9.6" "10" "11")

  set(PG_MAJORVERSION "${CMAKE_MATCH_1}")
  if (PG_MAJORVERSION LESS 10) # before ten we need to include the 'minor'
    string(APPEND PG_MAJORVERSION ".${CMAKE_MATCH_2}")
  endif ()

  if (NOT (PG_MAJORVERSION IN_LIST SUPPORTED_PGVERSIONS))
    message(FATAL_ERROR "Citus is not compatible with the detected PostgreSQL \
                         version ${PG_MAJORVERSION}.")
  else ()
    message(STATUS "Building against PostgreSQL ${PG_MAJORVERSION}")
  endif ()
else ()
  message(FATAL_ERROR "Could not detect PostgreSQL version from pg_config.")
endif ()

# Add our main library...
# TODO Do something smarter here
set(SOURCE_FILES
    "src/backend/distributed/commands/create_distributed_table.c"
    "src/backend/distributed/commands/drop_distributed_table.c"
    "src/backend/distributed/commands/multi_copy.c"
    "src/backend/distributed/commands/transmit.c"
    "src/backend/distributed/connection/connection_management.c"
    "src/backend/distributed/connection/placement_connection.c"
    "src/backend/distributed/connection/remote_commands.c"
    "src/backend/distributed/executor/citus_custom_scan.c"
    "src/backend/distributed/executor/insert_select_executor.c"
    "src/backend/distributed/executor/intermediate_results.c"
    "src/backend/distributed/executor/multi_client_executor.c"
    "src/backend/distributed/executor/multi_executor.c"
    "src/backend/distributed/executor/multi_real_time_executor.c"
    "src/backend/distributed/executor/multi_router_executor.c"
    "src/backend/distributed/executor/multi_server_executor.c"
    "src/backend/distributed/executor/multi_task_tracker_executor.c"
    "src/backend/distributed/executor/multi_utility.c"
    "src/backend/distributed/executor/subplan_execution.c"
    "src/backend/distributed/master/citus_create_restore_point.c"
    "src/backend/distributed/master/master_citus_tools.c"
    "src/backend/distributed/master/master_create_shards.c"
    "src/backend/distributed/master/master_delete_protocol.c"
    "src/backend/distributed/master/master_metadata_utility.c"
    "src/backend/distributed/master/master_modify_multiple_shards.c"
    "src/backend/distributed/master/master_node_protocol.c"
    "src/backend/distributed/master/master_repair_shards.c"
    "src/backend/distributed/master/master_split_shards.c"
    "src/backend/distributed/master/master_stage_protocol.c"
    "src/backend/distributed/master/master_truncate.c"
    "src/backend/distributed/master/worker_node_manager.c"
    "src/backend/distributed/metadata/metadata_sync.c"
    "src/backend/distributed/planner/deparse_shard_query.c"
    "src/backend/distributed/planner/distributed_planner.c"
    "src/backend/distributed/planner/insert_select_planner.c"
    "src/backend/distributed/planner/multi_explain.c"
    "src/backend/distributed/planner/multi_join_order.c"
    "src/backend/distributed/planner/multi_logical_optimizer.c"
    "src/backend/distributed/planner/multi_logical_planner.c"
    "src/backend/distributed/planner/multi_master_planner.c"
    "src/backend/distributed/planner/multi_physical_planner.c"
    "src/backend/distributed/planner/multi_router_planner.c"
    "src/backend/distributed/planner/postgres_planning_functions.c"
    "src/backend/distributed/planner/query_colocation_checker.c"
    "src/backend/distributed/planner/recursive_planning.c"
    "src/backend/distributed/planner/relation_restriction_equivalence.c"
    "src/backend/distributed/planner/shard_pruning.c"
    "src/backend/distributed/progress/multi_progress.c"
    "src/backend/distributed/relay/relay_event_utility.c"
    "src/backend/distributed/shared_library_init.c"
    "src/backend/distributed/test/colocation_utils.c"
    "src/backend/distributed/test/create_shards.c"
    "src/backend/distributed/test/deparse_shard_query.c"
    "src/backend/distributed/test/distributed_deadlock_detection.c"
    "src/backend/distributed/test/distribution_metadata.c"
    "src/backend/distributed/test/fake_fdw.c"
    "src/backend/distributed/test/generate_ddl_commands.c"
    "src/backend/distributed/test/metadata_sync.c"
    "src/backend/distributed/test/partitioning_utils.c"
    "src/backend/distributed/test/progress_utils.c"
    "src/backend/distributed/test/prune_shard_list.c"
    "src/backend/distributed/transaction/backend_data.c"
    "src/backend/distributed/transaction/distributed_deadlock_detection.c"
    "src/backend/distributed/transaction/lock_graph.c"
    "src/backend/distributed/transaction/multi_shard_transaction.c"
    "src/backend/distributed/transaction/remote_transaction.c"
    "src/backend/distributed/transaction/transaction_management.c"
    "src/backend/distributed/transaction/transaction_recovery.c"
    "src/backend/distributed/transaction/worker_transaction.c"
    "src/backend/distributed/utils/citus_clauses.c"
    "src/backend/distributed/utils/citus_copyfuncs.c"
    "src/backend/distributed/utils/citus_nodefuncs.c"
    "src/backend/distributed/utils/citus_outfuncs.c"
    "src/backend/distributed/utils/citus_readfuncs.c"
    "src/backend/distributed/utils/citus_ruleutils.c"
    "src/backend/distributed/utils/citus_version.c"
    "src/backend/distributed/utils/colocation_utils.c"
    "src/backend/distributed/utils/distribution_column.c"
    "src/backend/distributed/utils/errormessage.c"
    "src/backend/distributed/utils/hash_helpers.c"
    "src/backend/distributed/utils/listutils.c"
    "src/backend/distributed/utils/maintenanced.c"
    "src/backend/distributed/utils/metadata_cache.c"
    "src/backend/distributed/utils/multi_partitioning_utils.c"
    "src/backend/distributed/utils/multi_resowner.c"
    "src/backend/distributed/utils/node_metadata.c"
    "src/backend/distributed/utils/reference_table_utils.c"
    "src/backend/distributed/utils/resource_lock.c"
    "src/backend/distributed/utils/ruleutils_10.c"
    "src/backend/distributed/utils/ruleutils_11.c"
    "src/backend/distributed/utils/ruleutils_96.c"
    "src/backend/distributed/utils/shardinterval_utils.c"
    "src/backend/distributed/utils/statistics_collection.c"
    "src/backend/distributed/worker/task_tracker.c"
    "src/backend/distributed/worker/task_tracker_protocol.c"
    "src/backend/distributed/worker/worker_data_fetch_protocol.c"
    "src/backend/distributed/worker/worker_drop_protocol.c"
    "src/backend/distributed/worker/worker_file_access_protocol.c"
    "src/backend/distributed/worker/worker_merge_protocol.c"
    "src/backend/distributed/worker/worker_partition_protocol.c"
    "src/backend/distributed/worker/worker_truncate_trigger_protocol.c")

string(TOLOWER "${PROJECT_NAME}" EXT_NAME)
add_library("${EXT_NAME}" MODULE "${SOURCE_FILES}")

set(PGC_DATADIR PGC_SHAREDIR)  # TODO is this variable still needed?
set(DATAMODULEDIR "extension") # TODO see PGXS for possible values

# set up compiler and linker flags on *nix-y OSes
if (UNIX)
  # ... its include directories.
  target_include_directories("${EXT_NAME}"
                             PUBLIC "${PROJECT_BINARY_DIR}/src/include"
                             PUBLIC "${PROJECT_SOURCE_DIR}/src/include"
                             PUBLIC "${PGC_INCLUDEDIR}"
                             PUBLIC "${PGC_INCLUDEDIR_SERVER}"
                             PUBLIC "${PGC_PKGINCLUDEDIR}/internal")

  # get variables into list form
  separate_arguments(PGC_CFLAGS)
  separate_arguments(PGC_CPPFLAGS)
  separate_arguments(PGC_LDFLAGS)

  # Remove bad input from pg_config
  list(REMOVE_ITEM PGC_CPPFLAGS "-DFRONTEND")
  list(FILTER PGC_LDFLAGS EXCLUDE REGEX "-L\\.\\.")

  # Build LDFLAGS back into string
  foreach (FLAG ${PGC_LDFLAGS})
    string(APPEND PROJ_LDFLAGS " ${FLAG}")
  endforeach ()

  # Add the curl libraries, if any
  target_link_libraries("${EXT_NAME}" ${CURL_LIBRARIES})

  # ... and add those from pg_config.
  target_compile_options(${EXT_NAME}
                         PUBLIC ${PGC_CFLAGS}
                         PUBLIC ${PGC_CPPFLAGS})
endif ()

# TODO Add Linux section, if needed

if (APPLE)
  # Remove built-in MODULE compilation flags...
  unset(CMAKE_SHARED_MODULE_CREATE_C_FLAGS)

  # Override module linker flags to specify bundle loader
  set(CMAKE_MODULE_LINKER_FLAGS "-bundle -bundle_loader ${PGC_BINDIR}/postgres")

  # Get rid of symbol, prefix, and PIC settings; set link flags.
  set_target_properties("${EXT_NAME}" PROPERTIES PREFIX "" DEFINE_SYMBOL ""
                        POSITION_INDEPENDENT_CODE FALSE
                        LINK_FLAGS "${PROJ_LDFLAGS}")
endif ()

if (WIN32)
  target_include_directories("${EXT_NAME}" PUBLIC "${PGC_INCLUDEDIR}"
                             PUBLIC "${PGC_INCLUDEDIR_SERVER}/port/win32")

  if (MSVC)
    target_include_directories("${EXT_NAME}"
                               PUBLIC "${PGC_INCLUDEDIR_SERVER}/port/win32_msvc")
  endif ()

  target_include_directories("${EXT_NAME}"
                             PUBLIC "${PROJECT_BINARY_DIR}/src/include"
                             PUBLIC "${PROJECT_SOURCE_DIR}/src/include"
                             PUBLIC "${PGC_INCLUDEDIR_SERVER}"
                             PUBLIC "${PGC_PKGINCLUDEDIR}/internal"
                             PUBLIC "C:/Users/citus/Desktop/icu/include")

  # TODO Take more of these from PostgreSQL?
  target_compile_options("${EXT_NAME}" PUBLIC "-D_CRT_SECURE_NO_WARNINGS")

  string(APPEND CMAKE_MODULE_LINKER_FLAGS " /MANIFEST:NO")
  string(APPEND CMAKE_MODULE_LINKER_FLAGS " ${PGC_LIBDIR}/postgres.lib ws2_32.lib")
endif ()

# Look for libpq, but _only_ in the PostgreSQL libdir
find_library(LIBPQ NAMES "pq" "libpq"
             PATHS "${PGC_LIBDIR}"
             DOC "Postgres C API library."
             NO_DEFAULT_PATH)

if (NOT LIBPQ)
  message(FATAL_ERROR "Could not locate libpq.")
endif ()

target_link_libraries("${EXT_NAME}" "${LIBPQ}")

# Define our install target for the library
install(TARGETS ${EXT_NAME} DESTINATION ${PGC_PKGLIBDIR})

# Enumerate SQL update files
# TODO Do something smarter here
set(SQL_UPDATES
    "citus--5.0--5.0-1.sql"
    "citus--5.0-1--5.0-2.sql"
    "citus--5.0-2--5.1-1.sql"
    "citus--5.1-1--5.1-2.sql"
    "citus--5.1-2--5.1-3.sql"
    "citus--5.1-3--5.1-4.sql"
    "citus--5.1-4--5.1-5.sql"
    "citus--5.1-5--5.1-6.sql"
    "citus--5.1-6--5.1-7.sql"
    "citus--5.1-7--5.1-8.sql"
    "citus--5.1-8--5.2-1.sql"
    "citus--5.2-1--5.2-2.sql"
    "citus--5.2-2--5.2-3.sql"
    "citus--5.2-3--5.2-4.sql"
    "citus--5.2-4--6.0-1.sql"
    "citus--6.0-1--6.0-2.sql"
    "citus--6.0-2--6.0-3.sql"
    "citus--6.0-3--6.0-4.sql"
    "citus--6.0-4--6.0-5.sql"
    "citus--6.0-5--6.0-6.sql"
    "citus--6.0-6--6.0-7.sql"
    "citus--6.0-7--6.0-8.sql"
    "citus--6.0-8--6.0-9.sql"
    "citus--6.0-9--6.0-10.sql"
    "citus--6.0-10--6.0-11.sql"
    "citus--6.0-11--6.0-12.sql"
    "citus--6.0-12--6.0-13.sql"
    "citus--6.0-13--6.0-14.sql"
    "citus--6.0-14--6.0-15.sql"
    "citus--6.0-15--6.0-16.sql"
    "citus--6.0-16--6.0-17.sql"
    "citus--6.0-17--6.0-18.sql"
    "citus--6.0-18--6.1-1.sql"
    "citus--6.1-1--6.1-2.sql"
    "citus--6.1-2--6.1-3.sql"
    "citus--6.1-3--6.1-4.sql"
    "citus--6.1-4--6.1-5.sql"
    "citus--6.1-5--6.1-6.sql"
    "citus--6.1-6--6.1-7.sql"
    "citus--6.1-7--6.1-8.sql"
    "citus--6.1-8--6.1-9.sql"
    "citus--6.1-9--6.1-10.sql"
    "citus--6.1-10--6.1-11.sql"
    "citus--6.1-11--6.1-12.sql"
    "citus--6.1-12--6.1-13.sql"
    "citus--6.1-13--6.1-14.sql"
    "citus--6.1-14--6.1-15.sql"
    "citus--6.1-15--6.1-16.sql"
    "citus--6.1-16--6.1-17.sql"
    "citus--6.1-17--6.2-1.sql"
    "citus--6.2-1--6.2-2.sql"
    "citus--6.2-2--6.2-3.sql"
    "citus--6.2-3--6.2-4.sql"
    "citus--6.2-4--7.0-1.sql"
    "citus--7.0-1--7.0-2.sql"
    "citus--7.0-2--7.0-3.sql"
    "citus--7.0-3--7.0-4.sql"
    "citus--7.0-4--7.0-5.sql"
    "citus--7.0-5--7.0-6.sql"
    "citus--7.0-6--7.0-7.sql"
    "citus--7.0-7--7.0-8.sql"
    "citus--7.0-8--7.0-9.sql"
    "citus--7.0-9--7.0-10.sql"
    "citus--7.0-10--7.0-11.sql"
    "citus--7.0-11--7.0-12.sql"
    "citus--7.0-12--7.0-13.sql"
    "citus--7.0-13--7.0-14.sql"
    "citus--7.0-14--7.0-15.sql"
    "citus--7.0-15--7.1-1.sql"
    "citus--7.1-1--7.1-2.sql"
    "citus--7.1-2--7.1-3.sql"
    "citus--7.1-3--7.1-4.sql"
    "citus--7.1-4--7.2-1.sql"
    "citus--7.2-1--7.2-2.sql"
    "citus--7.2-2--7.2-3.sql"
    "citus--7.2-3--7.3-1.sql"
    "citus--7.3-1--7.3-2.sql"
    "citus--7.3-2--7.3-3.sql"
    "citus--7.3-3--7.4-1.sql")

# Get the final extension version, based on the last SQL update file
list(GET SQL_UPDATES -1 FINAL_UPDATE)
set(UPDATE_REGEX
    "^citus--([0-9]+\\.[0-9]+-*[0-9]*)--(([0-9]+\\.[0-9]+)-*[0-9]*).sql$")

if ("${FINAL_UPDATE}" MATCHES ${UPDATE_REGEX})
  set(CITUS_EXTENSIONVERSION "${CMAKE_MATCH_2}")

  if (NOT (CITUS_MAJORVERSION STREQUAL "${CMAKE_MATCH_3}"))
    message(FATAL_ERROR "CITUS_MAJORVERSION ('${CITUS_MAJORVERSION}') incompatible with \
                         CITUS_EXTENSIONVERSION ('${CITUS_EXTENSIONVERSION'})")
  endif ()
else ()
  message(FATAL_ERROR "Cannot parse update file name '${FINAL_UPDATE}'")
endif ()

# Generate header containing important variables from this file
set(CITUS_VERSION_H "src/include/citus_version.h")
configure_file("${CITUS_VERSION_H}.in" "${CITUS_VERSION_H}" @ONLY)

# Configure install target to directly install control file
set(EXT_CONTROL_FILE "${PROJECT_SOURCE_DIR}/src/backend/distributed/${EXT_NAME}.control")
install(FILES "${EXT_CONTROL_FILE}" DESTINATION "${PGC_SHAREDIR}/extension")

# Generate standalone extension install scripts for _every_ version.
# Do this by sequentially concatenating version scripts with previous
# standalone install scripts, parsing versions from the file names.
set(SQL_DIR "src/backend/distributed")
set(PREV_FILE "${PROJECT_BINARY_DIR}/${SQL_DIR}/citus--5.0.sql")
configure_file("${SQL_DIR}/citus.sql" "${PREV_FILE}" COPYONLY)

set(UPDATE_FILES "${PREV_FILE}")
foreach (UPDATE ${SQL_UPDATES})
  if (NOT (${UPDATE} MATCHES ${UPDATE_REGEX}))
    message(FATAL_ERROR "Cannot parse update file name '${UPDATE}'")
  endif ()

  set(START_VERSION ${CMAKE_MATCH_1})
  set(END_VERSION ${CMAKE_MATCH_2})

  set(UPDATE_FILE "${SQL_DIR}/${UPDATE}")
  set(CURR_FILE "${PROJECT_BINARY_DIR}/${SQL_DIR}/citus--${END_VERSION}.sql")

  configure_file("${PREV_FILE}" "${CURR_FILE}" COPYONLY)
  file(READ "${UPDATE_FILE}" CONTENTS)
  file(APPEND "${CURR_FILE}" "${CONTENTS}")

  list(APPEND UPDATE_FILES "${UPDATE_FILE}")
  list(APPEND UPDATE_FILES "${CURR_FILE}")

  set(PREV_FILE "${CURR_FILE}")
endforeach ()

# Create an install target for all SQL install/upgrade files
install(FILES ${UPDATE_FILES} DESTINATION "${PGC_SHAREDIR}/${DATAMODULEDIR}")
