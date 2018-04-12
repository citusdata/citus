cmake_minimum_required (VERSION 3.7)

# Set a default build type if none was specified
# adapted from https://blog.kitware.com/cmake-and-the-default-build-type/
set (default_build_type "Release")
# if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
#   set(default_build_type "Debug")
# endif()

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message (STATUS "Using default build type: '${default_build_type}'.")
	
  set (CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING
		"Choose the type of build." FORCE
	)
	
  # Set the possible values of build type for cmake-gui
  set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
		"Debug" "Release" "MinSizeRel" "RelWithDebInfo"
	)
endif (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)

# TODO do something smart with the version
project (citus
	VERSION 7.4.0
	DESCRIPTION "Citus distributed database"
	LANGUAGES C
)

# TODO make this configurable
set (PG_CONFIG "pg_config" CACHE STRING
	"Location to find pg_config for target PostgreSQL instalation (default PATH)."
)

# Get PostgreSQL configuration from pg_config
execute_process (
  COMMAND ${PG_CONFIG} --includedir
  OUTPUT_VARIABLE PGC_INCLUDEDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --includedir-server
  OUTPUT_VARIABLE PGC_INCLUDEDIR_SERVER
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --pkgincludedir
  OUTPUT_VARIABLE PGC_PKGINCLUDEDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --bindir
  OUTPUT_VARIABLE PGC_BINDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --cppflags
  OUTPUT_VARIABLE PGC_CPPFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --cflags
  OUTPUT_VARIABLE PGC_CFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --ldflags
  OUTPUT_VARIABLE PGC_LDFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --libdir
  OUTPUT_VARIABLE PGC_LIBDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --pkglibdir
  OUTPUT_VARIABLE PGC_PKGLIBDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --sharedir
  OUTPUT_VARIABLE PGC_SHAREDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process (
  COMMAND ${PG_CONFIG} --pgxs
  OUTPUT_VARIABLE PGC_PGXS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Add our main library...
add_library ("${PROJECT_NAME}" MODULE
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
  "src/backend/distributed/worker/worker_truncate_trigger_protocol.c"
)

set (PGC_DATADIR PGC_SHAREDIR)
set (DATAMODULEDIR "extension") # TODO should be configurable and be 'contrib' sometimes

# set up compiler and linker flags on *nix-y OSes
if (UNIX)
# ... its include directories.
	target_include_directories ("${PROJECT_NAME}"
    PUBLIC "${PROJECT_SOURCE_DIR}/src/include"
		PUBLIC "${PROJECT_BINARY_DIR}/src/include"
		PUBLIC "${PGC_INCLUDEDIR_SERVER}"
		PUBLIC "${PGC_PKGINCLUDEDIR}/internal"
	)

	# get variables into list form
	separate_arguments (PGC_CFLAGS)
	separate_arguments (PGC_CPPFLAGS)
	separate_arguments (PGC_LDFLAGS)

	# Remove bad input from pg_config
	list (REMOVE_ITEM PGC_CPPFLAGS "-DFRONTEND")
	list (FILTER PGC_LDFLAGS EXCLUDE REGEX -L\\.\\.)

	# Build LDFLAGS back into string
	foreach (FLAG ${PGC_LDFLAGS})
		string (APPEND PROJ_LDFLAGS " ${FLAG}")
	endforeach (FLAG ${PGC_LDFLAGS})

  string (APPEND PROJ_LDFLAGS " -L${PGC_LIBDIR} -lpq")

  include (FindCURL)
  target_link_libraries ("${PROJECT_NAME}" ${CURL_LIBRARIES})

	# ... and add those from pg_config.
	target_compile_options (${PROJECT_NAME} PUBLIC ${PGC_CFLAGS}
											PUBLIC ${PGC_CPPFLAGS})
endif (UNIX)

if (APPLE)
	# Remove built-in MODULE compilation flags...
	unset (CMAKE_SHARED_MODULE_CREATE_C_FLAGS)

	# Override module linker flags to specify bundle loader
	set (CMAKE_MODULE_LINKER_FLAGS "-bundle -bundle_loader ${PGC_BINDIR}/postgres")

	# Get rid of symbol, prefix, and PIC settings; set link flags.
	set_target_properties ("${PROJECT_NAME}" PROPERTIES
		PREFIX ""
		DEFINE_SYMBOL ""
		POSITION_INDEPENDENT_CODE FALSE
		LINK_FLAGS "${PROJ_LDFLAGS}"
	)
endif (APPLE)

if (WIN32)
# ... its include directories.
	target_include_directories ("${PROJECT_NAME}"
		PUBLIC "${PROJECT_BINARY_DIR}"
		PUBLIC "${PGC_INCLUDEDIR}"
		PUBLIC "${PGC_INCLUDEDIR_SERVER}"
		PUBLIC "${PGC_PKGINCLUDEDIR}/internal"
		PUBLIC "${PGC_INCLUDEDIR_SERVER}/port/win32"
	)

	target_compile_options("${PROJECT_NAME}" PUBLIC "-D_CRT_SECURE_NO_WARNINGS")

	set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} /MANIFEST:NO ${PGC_LIBDIR}/postgres.lib ws2_32.lib")

	if (MSVC)
		include_directories("${PGC_INCLUDEDIR_SERVER}/port/win32_msvc")
	endif (MSVC)
endif (WIN32)

install (
  TARGETS ${PROJECT_NAME}
  DESTINATION ${PGC_PKGLIBDIR}
)

set (EXT_CONTROL_FILE "${PROJECT_SOURCE_DIR}/src/backend/distributed/${PROJECT_NAME}.control")
# configure_file (${EXT_CONTROL_FILE}.in ${EXT_CONTROL_FILE} @ONLY)

install (
	FILES "${EXT_CONTROL_FILE}"
	DESTINATION "${PGC_SHAREDIR}/extension"
)

set (SQL_FILE_NAME "src/backend/distributed/${PROJECT_NAME}--7.4-1.sql")
configure_file ("${SQL_FILE_NAME}" "${SQL_FILE_NAME}" COPYONLY)

install (
	FILES "${SQL_FILE_NAME}"
	DESTINATION "${PGC_SHAREDIR}/${DATAMODULEDIR}"
)

get_filename_component (PGC_BASEDIR "${PGC_PGXS}" DIRECTORY)
get_filename_component (PGC_BASEDIR "${PGC_BASEDIR}" DIRECTORY)
get_filename_component (PGC_BASEDIR "${PGC_BASEDIR}" DIRECTORY)

if (WIN32)
	find_program (PG_REGRESS pg_regress
		HINTS "${PGC_BASEDIR}/src/test/regress/" "${PGC_BINDIR}"
		NO_DEFAULT_PATH)
else ()
	set (PG_REGRESS "${PGC_BASEDIR}/src/test/regress/pg_regress")
endif (WIN32)

set (REGRESS_OPTS
	   --dbname=contrib_regression
     --inputdir="${PROJECT_SOURCE_DIR}/test"
     --outputdir="${PROJECT_BINARY_DIR}/test"
	   --load-language=plpgsql
		 --bindir="${PGC_BINDIR}")
set (REGRESS "base36")

# installchecklocal tests against an existing postgres instance
add_custom_target (installcheck
	COMMAND ${CMAKE_COMMAND}
	--build .
	--target install
	--config $<CONFIGURATION>
  COMMAND ${PG_REGRESS}
	${REGRESS_OPTS}
	${REGRESS}
  USES_TERMINAL
)

set_property (DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
	results
	regression.diffs
	regression.out
)
