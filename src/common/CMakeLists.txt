add_library(
  odbc_common OBJECT
  duckdb_odbc.cpp
  file_io.cpp
  handle_functions.cpp
  odbc_diagnostic.cpp
  odbc_fetch.cpp
  odbc_interval.cpp
  odbc_utils.cpp)

target_compile_definitions(odbc_common PRIVATE -DDUCKDB_STATIC_BUILD)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_common>
    PARENT_SCOPE)
