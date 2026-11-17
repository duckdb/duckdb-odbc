add_subdirectory(common)
add_subdirectory(connect)
add_subdirectory(statement)
add_subdirectory(widechar)

add_library(odbc_driver OBJECT driver.cpp api_info.cpp descriptor.cpp
                               parameter_descriptor.cpp row_descriptor.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_driver>
    PARENT_SCOPE)
