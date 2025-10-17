add_library(odbc_statement OBJECT statement_functions.cpp)

target_compile_definitions(odbc_statement PRIVATE -DDUCKDB_STATIC_BUILD)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_statement>
    PARENT_SCOPE)
