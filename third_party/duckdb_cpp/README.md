# DuckDB stable C++ API

The driver uses DuckDB only through its stable C++ API (`duckdb::cxx`), a header and a source file that wrap DuckDB's
C API. They are not part of the vendored sources in `src/duckdb`, so they are kept here, and compiled against the C API
headers of the vendored engine (`src/duckdb/src/include/duckdb_v2.h`).

They are `tools/cpp/duckdb_cpp.hpp` and `tools/cpp/duckdb_cpp.cpp` of the vendored DuckDB revision (23221edfd57), with
the C++ API changes of duckdb/duckdb#28881 applied: `StartupOptions::Set`, `Value::GetDecimal` and a public
`Value::CreateDecimal`, and compiling after `<windows.h>`. Those only use C API functions the vendored revision has.

When the vendored sources are updated, update these two files from the same DuckDB revision (from a revision that
contains duckdb/duckdb#28881, they are taken as they are).
