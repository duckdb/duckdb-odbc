include_directories(../include)
include_directories(./include)
include_directories(${ODBC_INCLUDE_DIRS})

add_executable(
  test_odbc
  $<TARGET_OBJECTS:duckdb_core_obj>
  $<TARGET_OBJECTS:odbc_widechar>
  common.cpp
  connect_helpers.cpp
  temp_directory.cpp
  tests/alter.cpp
  tests/basic_usage.cpp
  tests/bind_col.cpp
  tests/bools_as_char.cpp
  tests/catalog_functions.cpp
  tests/diagnostics.cpp
  tests/extension.cpp
  tests/row_wise_fetching.cpp
  tests/set_attr.cpp
  tests/cte.cpp
  tests/cursor_commit.cpp
  tests/declare_fetch_block.cpp
  tests/data_execution.cpp
  tests/multicolumn_param_bind.cpp
  tests/numeric.cpp
  tests/quotes.cpp
  tests/test_empty_stubs.cpp
  tests/result_conversion.cpp
  tests/test_allowed_paths.cpp
  tests/test_connect.cpp
  tests/test_long_data.cpp
  tests/test_num_result_cols.cpp
  tests/test_select.cpp
  tests/test_session_init.cpp
  tests/test_truncation.cpp
  tests/test_timestamp.cpp
  tests/test_unbound_params.cpp
  tests/test_widechar_conv.cpp
  tests/test_widechar_data.cpp
  tests/col_attribute/col_attribute.cpp
  tests/col_attribute/char_query.cpp
  tests/col_attribute/decimal_query.cpp
  tests/col_attribute/int_query.cpp
  tests/col_attribute/interval_query.cpp
  tests/col_attribute/temporal_query.cpp
  tests/col_attribute/unsigned_int_query.cpp
  tests/col_attribute/uuid_query.cpp
  tests/col_attribute/utils.cpp)

add_executable(
  test_connection_odbc
  $<TARGET_OBJECTS:duckdb_core_obj> $<TARGET_OBJECTS:odbc_widechar> common.cpp
  tests/connect_with_ini.cpp connect_helpers.cpp)

if(WIN32)
  target_compile_options(test_odbc PRIVATE /bigobj)
  target_compile_options(test_connection_odbc PRIVATE /bigobj)
endif()

if(CMAKE_GENERATOR MATCHES "Visual Studio")
  # System.Data.ODBC tests Visual Studio Generator due to use of C++ CLI and
  # .NET.
  add_subdirectory(tests/dotnet)
endif()

target_link_libraries(test_odbc duckdb_odbc)
target_link_libraries(test_connection_odbc duckdb_odbc)
