add_library(odbc_statement OBJECT statement.cpp statement_functions.cpp)

target_compile_definitions(odbc_statement PRIVATE -DDUCKDB_STATIC_BUILD)

if(WIN32)
  target_compile_definitions(odbc_statement
                             PRIVATE -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_statement>
    PARENT_SCOPE)
