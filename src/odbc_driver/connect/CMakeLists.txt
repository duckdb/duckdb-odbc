add_library(odbc_connect OBJECT connect.cpp session_init.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_connect>
    PARENT_SCOPE)
