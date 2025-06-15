#include "connect_helpers.h"

#include <iostream>
#include <thread>
#include <vector>

#include <odbcinst.h>

using namespace odbc_test;

// Connect to database using SQLDriverConnect without a DSN
void ConnectWithoutDSN(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string conn_str = "";
	SQLCHAR str[1024];
	SQLSMALLINT strl;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	EXECUTE_AND_CHECK("SQLDriverConnect", nullptr, SQLDriverConnect, dbc, nullptr, ConvertToSQLCHAR(conn_str.c_str()),
	                  SQL_NTS, str, sizeof(str), &strl, SQL_DRIVER_COMPLETE);
}

// Connect to a database with extra keywords provided by Power Query SDK
void ConnectWithPowerQuerySDK(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string conn_str = "DRIVER={DuckDB Driver};database=" + GetTesterDirectory() + ";" +
	                       "custom_user_agent=powerbi/v0.0(DuckDB);" + "Trusted_Connection=yes;" + "UID=user1;" +
	                       "PWD=password1;" + "allow_unsigned_extensions=true;";
	std::vector<SQLCHAR> out_str_vec;
	out_str_vec.resize(1024);
	SQLSMALLINT out_str_len = 0;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	EXECUTE_AND_CHECK("SQLDriverConnect", nullptr, SQLDriverConnect, dbc, nullptr, ConvertToSQLCHAR(conn_str), SQL_NTS,
	                  out_str_vec.data(), out_str_vec.size(), &out_str_len, SQL_DRIVER_COMPLETE);
	REQUIRE(out_str_len > 0);
	auto out_str = std::string(reinterpret_cast<char *>(out_str_vec.data()), static_cast<std::size_t>(out_str_len));
	REQUIRE(out_str == conn_str);

	CheckConfig(dbc, "allow_unsigned_extensions", "true");
}

void ConnectWithPowerQuerySDKWide(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string conn_str = "DRIVER={DuckDB Driver};database=" + GetTesterDirectory() + ";" +
	                       "custom_user_agent=powerbi/v0.0(DuckDB);" + "Trusted_Connection=yes;" + "UID=user1;" +
	                       "PWD=password1;" + "allow_unsigned_extensions=true;";
	SQLWCHAR wstr[1024];
	SQLSMALLINT wstr_len;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	EXECUTE_AND_CHECK("SQLDriverConnectW", nullptr, SQLDriverConnectW, dbc, nullptr,
	                  ConvertToSQLWCHARNTS(conn_str).data(), SQL_NTS, wstr, sizeof(wstr), &wstr_len,
	                  SQL_DRIVER_COMPLETE);

	REQUIRE(wstr_len > 0);
	REQUIRE(0 == wstr[static_cast<std::size_t>(wstr_len)]);
	auto str = ConvertToString(wstr);
	REQUIRE(str == std::string(conn_str));

	CheckConfig(dbc, "allow_unsigned_extensions", "true");
}

// Connect with incorrect params
void ConnectWithIncorrectParam(std::string param) {
	SQLHANDLE env;
	SQLHANDLE dbc;
	std::string dsn = "DSN=duckdbmemory;" + param;
	SQLCHAR str[1024];
	SQLSMALLINT strl;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR(dsn.c_str()), SQL_NTS, str, sizeof(str), &strl,
	                       SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, dbc, SQL_HANDLE_DBC);
	REQUIRE(duckdb::StringUtil::Contains(message, "Invalid keyword"));
	REQUIRE(duckdb::StringUtil::Contains(message, "Did you mean: "));

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test sending incorrect parameters to SQLDriverConnect
static void TestIncorrectParams() {
	ConnectWithIncorrectParam("UnsignedAttribute=true");
	ConnectWithIncorrectParam("dtabase=test.duckdb");
	ConnectWithIncorrectParam("this_doesnt_exist=?");
}

// Test setting a database from the connection string
static void TestSettingDatabase() {
	SQLHANDLE env;
	SQLHANDLE dbc;

	auto db_path = "Database=" + GetTesterDirectory();

	// Connect to database using a connection string with a database path
	DRIVER_CONNECT_TO_DATABASE(env, dbc, db_path);

	// Check that the connection was successful
	CheckDatabase(dbc);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Connect with connection string that sets a specific config then checks if correctly set
static void SetConfig(const std::string &param, const std::string &setting, const std::string &expected_content) {
	SQLHANDLE env;
	SQLHANDLE dbc;

	DRIVER_CONNECT_TO_DATABASE(env, dbc, param);

	CheckConfig(dbc, setting, expected_content);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test setting different configs through the connection string
static void TestSettingConfigs() {
	// Test setting allow_unsigned_extensions
	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "allow_unsigned_extensions=true",
	          "allow_unsigned_extensions", "true");

	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "allow_unsigned_extensions=false",
	          "allow_unsigned_extensions", "false");

	SetConfig("allow_unsigned_extensions=true", "allow_unsigned_extensions", "true");

	SetConfig("allow_unsigned_extensions=false", "allow_unsigned_extensions", "false");

	// Test setting access_mode
	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "access_mode=READ_ONLY", "access_mode",
	          "READ_ONLY");

	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "access_mode=READ_WRITE", "access_mode",
	          "READ_WRITE");

	// Test handling unsupported connection string options
	SetConfig("unsupported_option_1=value_1;allow_unsigned_extensions=true;", "allow_unsigned_extensions", "true");

	// Test mixed case
	SetConfig("Allow_Unsigned_Extensions=True", "allow_unsigned_extensions", "true");
	SetConfig("Allow_Unsigned_Extensions=False", "allow_unsigned_extensions", "false");

	// Test options trimming
	SetConfig("foo1=bar1;  allow_unsigned_extensions = true ;foo2=bar2;", "allow_unsigned_extensions", "true");
	SetConfig("foo1=bar1;  allow_unsigned_extensions = false ;foo2=bar2;", "allow_unsigned_extensions", "false");
}

static void CheckWorkerThreads(SQLHANDLE dbc, std::size_t expected_threads_count) {
	HSTMT hstmt = nullptr;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect (worker_threads)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT current_setting('worker_threads')"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch (worker_threads)", hstmt, SQLFetch, hstmt);
	uint64_t fetched = -1;
	EXECUTE_AND_CHECK("SQLGetData (worker_threads)", hstmt, SQLGetData, hstmt, 1, SQL_C_UBIGINT, &fetched,
	                  sizeof(fetched), nullptr);
	REQUIRE(fetched == expected_threads_count);
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
}

TEST_CASE("Test SQLConnect and SQLDriverConnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
	DISCONNECT_FROM_DATABASE(env, dbc);

	TestIncorrectParams();

	TestSettingDatabase();

	TestSettingConfigs();

	ConnectWithoutDSN(env, dbc);
	ConnectWithPowerQuerySDK(env, dbc);
	ConnectWithPowerQuerySDKWide(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test user_agent - in-memory database", "[odbc][useragent]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", hstmt, SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc') FROM pragma_user_agent()"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test user_agent - named database", "[odbc][useragent]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", hstmt, SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc') FROM pragma_user_agent()"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// In-memory databases are a singleton from duckdb_odbc.hpp, so cannot have custom options
TEST_CASE("Test user_agent - named database, custom useragent", "[odbc][useragent]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect with a custom user_agent
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named_ua.db;custom_user_agent=CUSTOM_STRING");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", hstmt, SQLExecDirect, hstmt,
	    ConvertToSQLCHAR(
	        "SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc CUSTOM_STRING') FROM pragma_user_agent()"),
	    SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Creates a table, inserts a row, selects the row, fetches the result and checks it, then disconnects and reconnects to
// make sure the data is still there
TEST_CASE("Connect with named file, disconnect and reconnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// create a table
	EXECUTE_AND_CHECK("SQLExecDirect (create table)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE OR REPLACE TABLE test_table (a INTEGER)"), SQL_NTS);

	// insert a row
	EXECUTE_AND_CHECK("SQLExecDirect (insert row)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO test_table VALUES (1)"), SQL_NTS);

	// select the row
	EXECUTE_AND_CHECK("SQLExecDirect (select row)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM test_table"), SQL_NTS);

	// Fetch the result
	EXECUTE_AND_CHECK("SQLFetch (select row)", hstmt, SQLFetch, hstmt);

	// Check the result
	DATA_CHECK(hstmt, 1, "1");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Reconnect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// select the row
	EXECUTE_AND_CHECK("SQLExecDirect (select row)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM test_table"), SQL_NTS);

	// Fetch the result
	EXECUTE_AND_CHECK("SQLFetch (select row)", hstmt, SQLFetch, hstmt);

	// Check the result
	DATA_CHECK(hstmt, 1, "1");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test worker threads", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Single thread
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "threads=1");
	CheckWorkerThreads(dbc, 1);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Default, must be the number of CPU cores
	const auto processor_count = std::thread::hardware_concurrency();
	if (0 == processor_count) {
		std::cout << "Cannot detect the number of CPU cores, skipping 'Test worker threads' test" << std::endl;
		return;
	}
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
	CheckWorkerThreads(dbc, processor_count);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test connection string without null terminator", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	std::string conn_string_prefix = "Driver={DuckDB Driver};";
	// This connection option is deliverately invalid
	std::string conn_string_suffix = "threads=0;";
	SQLSMALLINT conn_string_len = static_cast<SQLSMALLINT>(conn_string_prefix.length());

	SQLRETURN ret =
	    SQLDriverConnectW(dbc, nullptr, ConvertToSQLWCHARNTS(conn_string_prefix + conn_string_suffix).data(), SQL_NTS,
	                      nullptr, SQL_NTS, nullptr, SQL_DRIVER_COMPLETE);
	REQUIRE(SQL_ERROR == ret);

	EXECUTE_AND_CHECK("SQLDriverConnectW", nullptr, SQLDriverConnectW, dbc, nullptr,
	                  ConvertToSQLWCHAR(conn_string_prefix + conn_string_suffix).data(), conn_string_len, nullptr,
	                  SQL_NTS, nullptr, SQL_DRIVER_COMPLETE);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
