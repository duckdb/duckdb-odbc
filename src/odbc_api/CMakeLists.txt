add_library(
  odbc_api OBJECT
  attribute_api.cpp
  binding_api.cpp
  column_info_api.cpp
  connection_api.cpp
  cursor_api.cpp
  descriptor_api.cpp
  empty_api_stubs.cpp
  metadata_api.cpp
  parameter_api.cpp
  result_api.cpp
  statement_api.cpp
  handle_api.cpp
  diagnostic_api.cpp
  info_api.cpp
  transaction_api.cpp)

target_compile_definitions(odbc_api PRIVATE -DDUCKDB_STATIC_BUILD)

if(WIN32)
  target_compile_definitions(odbc_api
                             PRIVATE -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_api>
    PARENT_SCOPE)
