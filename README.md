## Running the ODBC Client and Tests

#### Build the ODBC client

###### Debug (for development)

```bash
make debug
```
###### Release (for usage)

```bash
make
```

Release builds first look up the vendored DuckDB commit's version at
`https://duckdb-staging.duckdb.org/<commit>/latest_alpha_version.txt` and download
the matching static libraries for Linux, macOS, or Windows (amd64 or arm64).
The driver and tests link these libraries using the vendored C++ headers.
Missing versions, unavailable archives, and incomplete archives fall back to
building the vendored DuckDB sources. Debug builds always use the sources.

To force a source build:

```bash
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DODBC_USE_PREBUILT_DUCKDB=OFF
cmake --build build/release --parallel
```

Use `GEN=ninja make release` to select Ninja, which builds in parallel by default.

#### Run the ODBC Unit Tests

The ODBC tests are written with the catch framework. To run the tests, run the following command from the main DuckDB repository:

```bash
build/debug/test/test_odbc
```

You can also individually run the tests by specifying the test name as an argument to the test executable:

```bash
build/debug/test/test_odbc 'Test ALTER TABLE statement'
```
