add_subdirectory(odbc_api)
add_subdirectory(odbc_driver)

# Collect all object files for parent scope
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_api> $<TARGET_OBJECTS:odbc_driver>
    PARENT_SCOPE)
