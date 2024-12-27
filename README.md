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

#### Run the ODBC Unit Tests

The ODBC tests are written with the catch framework. To run the tests, run the following command from the main DuckDB repository:

```bash
build/debug/test/test_odbc
```

You can also individually run the tests by specifying the test name as an argument to the test executable:

```bash
build/debug/test/test_odbc 'Test ALTER TABLE statement'
```