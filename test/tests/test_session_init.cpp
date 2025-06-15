#include "odbc_test_common.h"

#include "temp_directory.hpp"

using namespace odbc_test;

TEST_CASE("Test session init for DB only", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, "CREATE TABLE tab1(col1 int);CREATE TABLE tab2(col2 int);");
	UserOdbcIni odbc_ini({{"session_init_sql_file", path}});

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("DROP TABLE tab1"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("DROP TABLE tab2"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test session init for DB and connection", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, R"(
CREATE TABLE tab1(col1 int);
 /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ 
INSERT INTO tab1 VALUES(42);
)");
	UserOdbcIni odbc_ini({{"session_init_sql_file", path}});

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM tab1"), SQL_NTS);

	int32_t fetched = -1;
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_SLONG, &fetched, sizeof(fetched), nullptr);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test session init for connection only", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, R"(
 /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */
CREATE TABLE tab1(col1 int);
)");
	UserOdbcIni odbc_ini({{"session_init_sql_file", path}});

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("DROP TABLE tab1"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test session init fail no file", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	// Not writing the file
	UserOdbcIni odbc_ini({{"session_init_sql_file", path}});

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "IM003");
	REQUIRE(message.find("Specified session init SQL file not found") != std::string::npos);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

TEST_CASE("Test session init SHA256", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, "CREATE TABLE tab1(col1 int)");
	UserOdbcIni odbc_ini(
	    {{"session_init_sql_file", path},
	     {"session_init_sql_file_sha256", "e916fc2bcab2e0fad8d8e94273b8c79ff576aafbca9941e06ab268d9176269ec"}});

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("DROP TABLE tab1"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test session init SHA256 fail", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, "CREATE TABLE tab1(col1 int)");
	UserOdbcIni odbc_ini(
	    {{"session_init_sql_file", path},
	     {"session_init_sql_file_sha256", "FAIL_e916fc2bcab2e0fad8d8e94273b8c79ff576aafbca9941e06ab268d9176269ec"}});

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "IM003");
	REQUIRE(message.find("Session init SQL file SHA-256 mismatch") != std::string::npos);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

TEST_CASE("Test session init conn string prohibited", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, "CREATE TABLE tab1(col1 int)");

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	std::string conn_str =
	    "driver={DuckDB Driver};session_init_sql_file=" + path +
	    ";session_init_sql_file_sha256=e916fc2bcab2e0fad8d8e94273b8c79ff576aafbca9941e06ab268d9176269ec;";
	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR(conn_str.c_str()), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "01S09");
	REQUIRE(message.find("Options 'session_init_sql_file' and 'session_init_sql_file_sha256' can only be specified in "
	                     "DSN configuration in a file or registry") != std::string::npos);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

TEST_CASE("Test session init tracing", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	std::string init_text = "CREATE TABLE tab1(col1 int)";
	WriteStringToFile(path, init_text);
	UserOdbcIni odbc_ini({
	    {"session_init_sql_file", path},
	});

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "01000");
	REQUIRE(message == "ODBC_DuckDB->SQLDriverConnect\nSession init SQL:\nCREATE TABLE tab1(col1 int)");

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test session init invalid file", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, R"(
CREATE TABLE tab1(col1 int);
 /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ 
INSERT INTO tab1 VALUES(42);
 /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ 
INSERT INTO tab1 VALUES(43);
)");
	UserOdbcIni odbc_ini({
	    {"session_init_sql_file", path},
	});

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "IM003");
	REQUIRE(message.length() > 0);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

TEST_CASE("Test session init file with bad SQL", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	TempDirectory tmpdir;
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, R"(
CREATE TABLE tab1(col1 INT NOT NULL);
INSERT INTO tab1 VALUES(NULL);
)");
	UserOdbcIni odbc_ini({
	    {"session_init_sql_file", path},
	});

	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	auto ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                            SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC_WIDE(state, message, dbc, SQL_HANDLE_DBC);

	REQUIRE(state == "42000");
	REQUIRE(message.length() > 0);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}
