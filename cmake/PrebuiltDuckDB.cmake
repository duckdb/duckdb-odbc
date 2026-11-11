option(ODBC_USE_PREBUILT_DUCKDB
       "Try staged DuckDB static libraries before building vendored sources" ON)
set(ODBC_DUCKDB_STAGING_URL
    "https://duckdb-staging.duckdb.org"
    CACHE STRING "Base URL for staged DuckDB static libraries")

function(odbc_find_prebuilt_duckdb)
  # Release archives do not have the assertions or runtime used by debug builds.
  if(NOT ODBC_USE_PREBUILT_DUCKDB
     OR NOT CMAKE_BUILD_TYPE STREQUAL "Release"
     OR CLANG_TIDY)
    return()
  endif()

  set(ARCH "${CMAKE_SYSTEM_PROCESSOR}")
  if(APPLE AND CMAKE_OSX_ARCHITECTURES)
    set(ARCH "${CMAKE_OSX_ARCHITECTURES}")
  elseif(MSVC AND CMAKE_CXX_COMPILER_ARCHITECTURE_ID)
    # Use the compiler's target when cross compiling Windows ARM64 on x64.
    set(ARCH "${CMAKE_CXX_COMPILER_ARCHITECTURE_ID}")
  endif()
  string(TOLOWER "${ARCH}" ARCH)
  if(ARCH MATCHES "^(x86_64|amd64|x64)$")
    set(ARCH amd64)
  elseif(ARCH MATCHES "^(aarch64|arm64)$")
    set(ARCH arm64)
  else()
    message(
      STATUS "No staged DuckDB archives for ${ARCH}; building from source")
    return()
  endif()

  if(APPLE)
    set(PLATFORM "osx-${ARCH}")
  elseif(MSVC)
    set(PLATFORM "windows-${ARCH}")
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(PLATFORM "linux-${ARCH}")
  else()
    message(
      STATUS "No staged DuckDB archives for this platform; building from source"
    )
    return()
  endif()

  file(
    READ
    "${PROJECT_SOURCE_DIR}/src/duckdb/src/function/table/version/pragma_version.cpp"
    VERSION_SOURCE)
  string(REGEX MATCH "#define DUCKDB_SOURCE_ID \"([a-f0-9]+)\"" _
               "${VERSION_SOURCE}")
  set(COMMIT "${CMAKE_MATCH_1}")
  if(NOT COMMIT)
    message(STATUS "DuckDB source ID unavailable; building from source")
    return()
  endif()

  set(DOWNLOAD_DIR "${PROJECT_BINARY_DIR}/prebuilt-duckdb")
  file(MAKE_DIRECTORY "${DOWNLOAD_DIR}")
  set(VERSION_FILE "${DOWNLOAD_DIR}/latest_alpha_version.txt")
  file(
    DOWNLOAD "${ODBC_DUCKDB_STAGING_URL}/${COMMIT}/latest_alpha_version.txt"
    "${VERSION_FILE}"
    STATUS DOWNLOAD_STATUS
    TIMEOUT 30
    TLS_VERIFY ON)
  list(GET DOWNLOAD_STATUS 0 DOWNLOAD_CODE)
  if(NOT DOWNLOAD_CODE EQUAL 0)
    message(
      STATUS
        "No staged DuckDB version for ${COMMIT}; building from source (${DOWNLOAD_STATUS})"
    )
    return()
  endif()
  file(READ "${VERSION_FILE}" VERSION)
  string(STRIP "${VERSION}" VERSION)
  if(NOT VERSION MATCHES "^v[0-9]+\\.[0-9]+\\.[0-9]+(-(alpha|rc|dev)[0-9]+)?$")
    message(STATUS "Invalid staged DuckDB version; building from source")
    return()
  endif()

  # Include the version in the cache path: a commit can be staged more than
  # once.
  set(LIBRARY_DIR "${DOWNLOAD_DIR}/${COMMIT}/${VERSION}/${PLATFORM}")
  set(ARCHIVE_NAME "duckdb-static-libs-${PLATFORM}.tar.gz")
  set(ARCHIVE_URL
      "${ODBC_DUCKDB_STAGING_URL}/${COMMIT}/${VERSION}/duckdb/duckdb/github_release/${ARCHIVE_NAME}"
  )
  if(NOT EXISTS "${LIBRARY_DIR}/complete")
    file(MAKE_DIRECTORY "${LIBRARY_DIR}")
    set(ARCHIVE "${LIBRARY_DIR}/${ARCHIVE_NAME}")
    message(STATUS "Downloading staged DuckDB libraries: ${ARCHIVE_URL}")
    file(
      DOWNLOAD "${ARCHIVE_URL}" "${ARCHIVE}"
      STATUS DOWNLOAD_STATUS
      TIMEOUT 180
      TLS_VERIFY ON)
    list(GET DOWNLOAD_STATUS 0 DOWNLOAD_CODE)
    if(NOT DOWNLOAD_CODE EQUAL 0)
      file(REMOVE "${ARCHIVE}")
      message(
        STATUS
          "DuckDB static archive unavailable; building from source (${DOWNLOAD_STATUS})"
      )
      return()
    endif()
    execute_process(
      COMMAND "${CMAKE_COMMAND}" -E tar xzf "${ARCHIVE}"
      WORKING_DIRECTORY "${LIBRARY_DIR}"
      RESULT_VARIABLE EXTRACT_RESULT
      ERROR_VARIABLE EXTRACT_ERROR)
    file(REMOVE "${ARCHIVE}")
    if(NOT EXTRACT_RESULT EQUAL 0)
      file(REMOVE_RECURSE "${LIBRARY_DIR}")
      message(
        STATUS
          "Cannot extract DuckDB static archive; building from source (${EXTRACT_ERROR})"
      )
      return()
    endif()
  endif()

  # Keep the same bundled extensions as the source build. Their registration
  # code is compiled locally, so unrelated extensions in the archive stay out.
  set(LIBRARIES "")
  foreach(
    LIBRARY
    duckdb_static
    duckdb_httplib
    duckdb_loadable_extensions
    core_functions_extension
    parquet_extension
    icu_extension
    json_extension)
    if(MSVC)
      set(LIBRARY_PATH "${LIBRARY_DIR}/${LIBRARY}.lib")
    else()
      set(LIBRARY_PATH "${LIBRARY_DIR}/lib${LIBRARY}.a")
    endif()
    if(NOT EXISTS "${LIBRARY_PATH}")
      file(REMOVE_RECURSE "${LIBRARY_DIR}")
      message(
        STATUS "DuckDB archive is missing ${LIBRARY}; building from source")
      return()
    endif()
    list(APPEND LIBRARIES "${LIBRARY_PATH}")
  endforeach()
  file(WRITE "${LIBRARY_DIR}/complete" "")

  # DuckDB and its extensions reference each other. ELF linkers need to rescan
  # the archives to resolve these cycles.
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(LIBRARIES "-Wl,--start-group" ${LIBRARIES} "-Wl,--end-group")
  endif()
  set(DUCKDB_PREBUILT_LIBRARIES
      "${LIBRARIES}"
      PARENT_SCOPE)
  message(
    STATUS
      "Using staged DuckDB static libraries for ${COMMIT} (${VERSION}, ${PLATFORM})"
  )
endfunction()

odbc_find_prebuilt_duckdb()
