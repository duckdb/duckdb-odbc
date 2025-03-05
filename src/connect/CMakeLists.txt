add_library(odbc_connect OBJECT connect.cpp connection.cpp driver_connect.cpp)

target_compile_definitions(odbc_connect PRIVATE -DDUCKDB_STATIC_BUILD)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_connect>
    PARENT_SCOPE)
