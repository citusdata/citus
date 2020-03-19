cmake_minimum_required(VERSION 3.1)
project(safestring)

option(BUILD_UNITTESTS "Build also project unit-tests" OFF)

if (NOT DEFINED BUILD_OPT_DEFAULT)
  set (BUILD_OPT_DEFAULT YES CACHE PATH "Build option default")
endif (NOT DEFINED BUILD_OPT_DEFAULT)

option(BUILD_MEMCMP "Build memcmp* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_MEMCPY "Build memcpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_MEMMOVE "Build memmove* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_MEMSET "Build memset* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_MEMZERO "Build memzero* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STPCPY "Build stpcpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STPNCPY "Build stpncpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCASECMP "Build strcasecmp* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCASESTR "Build strcasestr* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCAT "Build strcat* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCMPFLD "Build strcmpfld* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCMP "Build strcmp* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCPYFLDIN "Build strcpyfldin* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCPYFLDOUT "Build strcpyfldout* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCPYFLD "Build strcpyfld* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCPY "Build strcpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRCSPN "Build strcspn* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRFIRSTCHAR "Build strfirstchar* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRFIRSTDIFF "Build strfirstdiff* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRFIRSTSAME "Build strfirstsame* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISALPHANUMERIC "Build strisalphanumeric* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISASCII "Build strisascii* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISDIGIT "Build strisdigit* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISHEX "Build strishex* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISLOWERCASE "Build strislowercase* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISMIXEDCASE "Build strismixedcase* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISPASSWORD "Build strispassword* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRISUPPERCASE "Build strisuppercase* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRLASTCHAR "Build strlastchar* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRLASTDIFF "Build strlastdiff* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRLASTSAME "Build strlastsame* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRLJUSTIFY "Build strljustify* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRNCAT "Build strncat* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRNCPY "Build strncpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRNLEN "Build strnlen* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRNTERMINATE "Build strnterminate* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRPBRK "Build strpbrk* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRPREFIX "Build strprefix* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRREMOVEWS "Build strremovews* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRSPN "Build strspn* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRSTR "Build strstr* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRTOK "Build strtok* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRTOLOWERCASE "Build strtolowercase* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRTOUPPERCASE "Build strtouppercase* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_STRZERO "Build strzero* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCPCPY "Build wcpcpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCSCAT "Build wcscat* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCSCPY "Build wcscpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCSNCAT "Build wcsncat* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCSNCPY "Build wcsncpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WCSNLEN "Build wcsnlen* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WMEMCMP "Build wmemcmp* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WMEMCPY "Build wmemcpy* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WMEMMOVE "Build wmemmove* functions" ${BUILD_OPT_DEFAULT})
option(BUILD_WMEMSET "Build wmemset* functions" ${BUILD_OPT_DEFAULT})

set(SOURCES
    safeclib/abort_handler_s.c
    safeclib/ignore_handler_s.c
    $<$<BOOL:${BUILD_MEMCMP}>:safeclib/memcmp16_s.c>
    $<$<BOOL:${BUILD_MEMCMP}>:safeclib/memcmp32_s.c>
    $<$<BOOL:${BUILD_MEMCMP}>:safeclib/memcmp_s.c>
    $<$<BOOL:${BUILD_MEMCPY}>:safeclib/memcpy16_s.c>
    $<$<BOOL:${BUILD_MEMCPY}>:safeclib/memcpy32_s.c>
    $<$<BOOL:${BUILD_MEMCPY}>:safeclib/memcpy_s.c>
    $<$<BOOL:${BUILD_MEMMOVE}>:safeclib/memmove16_s.c>
    $<$<BOOL:${BUILD_MEMMOVE}>:safeclib/memmove32_s.c>
    $<$<BOOL:${BUILD_MEMMOVE}>:safeclib/memmove_s.c>
    safeclib/mem_primitives_lib.c
    $<$<BOOL:${BUILD_MEMSET}>:safeclib/memset16_s.c>
    $<$<BOOL:${BUILD_MEMSET}>:safeclib/memset32_s.c>
    $<$<BOOL:${BUILD_MEMSET}>:safeclib/memset_s.c>
    $<$<BOOL:${BUILD_MEMZERO}>:safeclib/memzero16_s.c>
    $<$<BOOL:${BUILD_MEMZERO}>:safeclib/memzero32_s.c>
    $<$<BOOL:${BUILD_MEMZERO}>:safeclib/memzero_s.c>
    safeclib/safe_mem_constraint.c
    safeclib/safe_str_constraint.c
    safeclib/snprintf_support.c
    $<$<BOOL:${BUILD_STPCPY}>:safeclib/stpcpy_s.c>
    $<$<BOOL:${BUILD_STPNCPY}>:safeclib/stpncpy_s.c>
    $<$<BOOL:${BUILD_STRCASECMP}>:safeclib/strcasecmp_s.c>
    $<$<BOOL:${BUILD_STRCASESTR}>:safeclib/strcasestr_s.c>
    $<$<BOOL:${BUILD_STRCAT}>:safeclib/strcat_s.c>
    $<$<BOOL:${BUILD_STRCMPFLD}>:safeclib/strcmpfld_s.c>
    $<$<BOOL:${BUILD_STRCMP}>:safeclib/strcmp_s.c>
    $<$<BOOL:${BUILD_STRCPYFLDIN}>:safeclib/strcpyfldin_s.c>
    $<$<BOOL:${BUILD_STRCPYFLDOUT}>:safeclib/strcpyfldout_s.c>
    $<$<BOOL:${BUILD_STRCPYFLD}>:safeclib/strcpyfld_s.c>
    $<$<BOOL:${BUILD_STRCPY}>:safeclib/strcpy_s.c>
    $<$<BOOL:${BUILD_STRCSPN}>:safeclib/strcspn_s.c>
    $<$<BOOL:${BUILD_STRFIRSTCHAR}>:safeclib/strfirstchar_s.c>
    $<$<BOOL:${BUILD_STRFIRSTDIFF}>:safeclib/strfirstdiff_s.c>
    $<$<BOOL:${BUILD_STRFIRSTSAME}>:safeclib/strfirstsame_s.c>
    $<$<BOOL:${BUILD_STRISALPHANUMERIC}>:safeclib/strisalphanumeric_s.c>
    $<$<BOOL:${BUILD_STRISASCII}>:safeclib/strisascii_s.c>
    $<$<BOOL:${BUILD_STRISDIGIT}>:safeclib/strisdigit_s.c>
    $<$<BOOL:${BUILD_STRISHEX}>:safeclib/strishex_s.c>
    $<$<BOOL:${BUILD_STRISLOWERCASE}>:safeclib/strislowercase_s.c>
    $<$<BOOL:${BUILD_STRISMIXEDCASE}>:safeclib/strismixedcase_s.c>
    $<$<BOOL:${BUILD_STRISPASSWORD}>:safeclib/strispassword_s.c>
    $<$<BOOL:${BUILD_STRISUPPERCASE}>:safeclib/strisuppercase_s.c>
    $<$<BOOL:${BUILD_STRLASTCHAR}>:safeclib/strlastchar_s.c>
    $<$<BOOL:${BUILD_STRLASTDIFF}>:safeclib/strlastdiff_s.c>
    $<$<BOOL:${BUILD_STRLASTSAME}>:safeclib/strlastsame_s.c>
    $<$<BOOL:${BUILD_STRLJUSTIFY}>:safeclib/strljustify_s.c>
    $<$<BOOL:${BUILD_STRNCAT}>:safeclib/strncat_s.c>
    $<$<BOOL:${BUILD_STRNCPY}>:safeclib/strncpy_s.c>
    $<$<BOOL:${BUILD_STRNLEN}>:safeclib/strnlen_s.c>
    $<$<BOOL:${BUILD_STRNTERMINATE}>:safeclib/strnterminate_s.c>
    $<$<BOOL:${BUILD_STRPBRK}>:safeclib/strpbrk_s.c>
    $<$<BOOL:${BUILD_STRPREFIX}>:safeclib/strprefix_s.c>
    $<$<BOOL:${BUILD_STRREMOVEWS}>:safeclib/strremovews_s.c>
    $<$<BOOL:${BUILD_STRSPN}>:safeclib/strspn_s.c>
    $<$<BOOL:${BUILD_STRSTR}>:safeclib/strstr_s.c>
    $<$<BOOL:${BUILD_STRTOK}>:safeclib/strtok_s.c>
    $<$<BOOL:${BUILD_STRTOLOWERCASE}>:safeclib/strtolowercase_s.c>
    $<$<BOOL:${BUILD_STRTOUPPERCASE}>:safeclib/strtouppercase_s.c>
    $<$<BOOL:${BUILD_STRZERO}>:safeclib/strzero_s.c>
    $<$<BOOL:${BUILD_WCPCPY}>:safeclib/wcpcpy_s.c>
    $<$<BOOL:${BUILD_WCSCAT}>:safeclib/wcscat_s.c>
    $<$<BOOL:${BUILD_WCSCPY}>:safeclib/wcscpy_s.c>
    $<$<BOOL:${BUILD_WCSNCAT}>:safeclib/wcsncat_s.c>
    $<$<BOOL:${BUILD_WCSNCPY}>:safeclib/wcsncpy_s.c>
    $<$<BOOL:${BUILD_WCSNLEN}>:safeclib/wcsnlen_s.c>
    $<$<BOOL:${BUILD_WMEMCMP}>:safeclib/wmemcmp_s.c>
    $<$<BOOL:${BUILD_WMEMCPY}>:safeclib/wmemcpy_s.c>
    $<$<BOOL:${BUILD_WMEMMOVE}>:safeclib/wmemmove_s.c>
    $<$<BOOL:${BUILD_WMEMSET}>:safeclib/wmemset_s.c>
    )

include_directories(include)

add_library(${PROJECT_NAME}_objlib OBJECT ${SOURCES})
set_target_properties(${PROJECT_NAME}_objlib
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_compile_definitions(${PROJECT_NAME}_objlib PRIVATE -DSTDC_HEADERS)

target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE -Wno-unknown-pragmas -Wno-unused-parameter)
if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_C_COMPILER_VERSION VERSION_GREATER 7)
  target_compile_options(${PROJECT_NAME}_objlib
                         PRIVATE -Wno-implicit-fallthrough)
endif()
target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE -Wall -Wextra -Wsign-compare -Wformat-security)
target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE  -Wstack-protector -Winit-self)
target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE -D_FORTIFY_SOURCE=2 -O2)
target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE -fstack-protector-all)
target_compile_options(${PROJECT_NAME}_objlib
                       PRIVATE --param ssp-buffer-size=4 -ftrapv)
target_compile_options(${PROJECT_NAME}_objlib PRIVATE -fPIE -fPIC)

if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_C_COMPILER_VERSION VERSION_GREATER 6)
    target_compile_options(${PROJECT_NAME}_objlib PRIVATE -mmitigate-rop)
endif()
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -z noexecstack -z relro -z now")

option(BUILD_ERROR_ON_WARNING "Fail compilation on warning" OFF)

if(BUILD_ERROR_ON_WARNING)
    target_compile_options(${PROJECT_NAME}_objlib PRIVATE -Werror)
endif()

target_compile_options(${PROJECT_NAME}_objlib PRIVATE $<$<CONFIG:RELEASE>:-s>)

add_library(${PROJECT_NAME}_shared SHARED $<TARGET_OBJECTS:${PROJECT_NAME}_objlib>)
add_library(${PROJECT_NAME}_static STATIC $<TARGET_OBJECTS:${PROJECT_NAME}_objlib>)
target_include_directories(${PROJECT_NAME}_shared PUBLIC include)
target_include_directories(${PROJECT_NAME}_static PUBLIC include)

if(BUILD_UNITTESTS)
  add_subdirectory(unittests)
endif()
