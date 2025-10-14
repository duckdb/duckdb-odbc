add_library(odbc_connect OBJECT connect.cpp session_init.cpp)

target_compile_definitions(odbc_connect PRIVATE -DDUCKDB_STATIC_BUILD)

if(WIN32)
  target_compile_definitions(odbc_connect
                             PRIVATE -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_connect>
    PARENT_SCOPE)
