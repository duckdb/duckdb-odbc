## Running the ODBC Client and Tests

#### How the driver uses DuckDB

The driver is written against DuckDB's stable C++ API (`duckdb::cxx`), which wraps the DuckDB C API, and uses the C API
directly for date and time conversions. Only that API and the C API headers are vendored, in `third_party/duckdb_cpp`,
together with the DuckDB revision they were taken from
(`third_party/duckdb_cpp/duckdb_revision.cmake`). The DuckDB engine of that revision is built separately, with the
`core_functions`, `parquet`, `icu` and `json` extensions, and statically linked into the driver, so the driver is still
a single self-contained library.

`make` builds the engine on first use, into `build/duckdb`, through `scripts/build_duckdb.py`. To link an engine you
built yourself, pass its install prefix instead:

```bash
python3 scripts/build_duckdb.py --install-dir /path/to/duckdb-install --source-dir /path/to/duckdb
make DUCKDB_ROOT=/path/to/duckdb-install
```

To move the driver to another DuckDB revision, vendor its C++ API from a DuckDB checkout:

```bash
python3 vendor.py --duckdb /path/to/duckdb
```

`vendor.py --patch <file>` applies a patch to the vendored C++ API, and records it in `duckdb_revision.cmake`. The
driver is currently pinned to the v2.0.0 release with `third_party/duckdb_cpp_patches/duckdb-28881-cpp-api.patch`, the
C++ API additions of duckdb/duckdb#28881, which only use C API functions v2.0.0 has:

```bash
python3 vendor.py --duckdb /path/to/duckdb --patch third_party/duckdb_cpp_patches/duckdb-28881-cpp-api.patch
```

Vendoring a DuckDB revision that contains duckdb/duckdb#28881 needs no patch.

#### Build the ODBC client

###### Debug (for development)

```bash
make debug
```
###### Release (for usage)

```bash
make
```

#### Run the ODBC Unit Tests

The ODBC tests are written with the catch framework. To run the tests, run the following command from the main DuckDB repository:

```bash
build/debug/test/test_odbc
```

You can also individually run the tests by specifying the test name as an argument to the test executable:

```bash
build/debug/test/test_odbc 'Test ALTER TABLE statement'
```