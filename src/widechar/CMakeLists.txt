add_library(odbc_widechar OBJECT widechar.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_widechar>
    PARENT_SCOPE)
