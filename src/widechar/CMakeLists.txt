add_library(odbc_widechar OBJECT widechar.cpp)

if(WIN32)
  target_compile_definitions(odbc_widechar
                             PRIVATE -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_widechar>
    PARENT_SCOPE)
