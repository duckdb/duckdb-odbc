add_subdirectory(odbc_api)
add_subdirectory(odbc_driver)

# DuckDB's stable C++ API, through which the driver uses DuckDB, see
# third_party/duckdb_cpp/README.md
add_library(odbc_duckdb_cpp_api OBJECT
            ${PROJECT_SOURCE_DIR}/third_party/duckdb_cpp/duckdb_cpp.cpp)
target_compile_definitions(odbc_duckdb_cpp_api PRIVATE -DDUCKDB_STATIC_BUILD)
if(WIN32)
  target_compile_options(odbc_duckdb_cpp_api PRIVATE /bigobj /utf-8)
endif()

# Collect all object files for parent scope
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_api> $<TARGET_OBJECTS:odbc_driver>
    $<TARGET_OBJECTS:odbc_duckdb_cpp_api>
    PARENT_SCOPE)
