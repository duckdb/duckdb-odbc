add_subdirectory(common)
add_subdirectory(connect)
add_subdirectory(statement)
add_subdirectory(widechar)

add_library(
  odbc_src OBJECT
  driver.cpp
  prepared.cpp
  result.cpp
  api_info.cpp
  empty_stubs.cpp
  descriptor.cpp
  parameter_descriptor.cpp
  row_descriptor.cpp
  cursor.cpp)

target_compile_definitions(odbc_src PRIVATE -DDUCKDB_STATIC_BUILD)

if(WIN32)
  target_compile_definitions(odbc_src
                             PRIVATE -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_src>
    PARENT_SCOPE)
