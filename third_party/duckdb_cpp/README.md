# DuckDB stable C++ API

The driver uses DuckDB only through its stable C++ API (`duckdb::cxx`), a header and a source file that wrap DuckDB's
C API. They are not part of the vendored sources in `src/duckdb`, so they are kept here, and compiled against the C API
headers of the vendored engine (`src/duckdb/src/include/duckdb_v2.h`).

They are `tools/cpp/duckdb_cpp.hpp` and `tools/cpp/duckdb_cpp.cpp` of the vendored DuckDB revision, taken as they are.
The driver needs a revision that contains duckdb/duckdb#29068, for `Instance::OpenShared` with an init callback.

When the vendored sources are updated, update these two files from the same DuckDB revision: the wrapper follows the
engine, for example in the width of a DECIMAL, which is no longer a single byte.
