#include "odbc_test_common.h"

#include "temp_directory.hpp"

#include <atomic>
#include <thread>
#include <vector>

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

TEST_CASE("Test session init failure releases the database", "[odbc]") {
	TempDirectory tmpdir;
	std::string db_path = tmpdir.path + "/session_init_failure.db";
	std::string path = tmpdir.path + "/session_init.sql";
	WriteStringToFile(path, "SELECT * FROM table_that_does_not_exist;");
	UserOdbcIni odbc_ini({
	    {"database", db_path},
	    {"session_init_sql_file", path},
	});

	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	EXECUTE_AND_CHECK("SQLAllocHandle", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);
	REQUIRE(SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS, nullptr,
	                         SQL_DRIVER_COMPLETE) == SQL_ERROR);

	// while the handle of the failed connection is still allocated, the database can be opened with another
	// configuration: the failed connection does not keep it open
	SQLHANDLE env2, dbc2;
	DRIVER_CONNECT_TO_DATABASE(env2, dbc2, "Database=" + db_path + ";threads=1");
	DISCONNECT_FROM_DATABASE(env2, dbc2);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
}

TEST_CASE("Test session init for DB runs before the database is shared", "[odbc]") {
	TempDirectory tmpdir;
	std::string db_path = tmpdir.path + "/session_init_shared.db";
	std::string path = tmpdir.path + "/session_init.sql";
	// fails when run a second time on the same database
	WriteStringToFile(path, "CREATE TABLE tab1 AS SELECT 42 AS col1;");
	UserOdbcIni odbc_ini({
	    {"database", db_path},
	    {"session_init_sql_file", path},
	});

	// every thread connects at once, and keeps its connection until all have connected: the db init SQL runs once,
	// and every connection sees what it created. Catch's assertions are not thread-safe: the threads only count their
	// failures.
	const size_t thread_count = 8;
	std::vector<size_t> failures(thread_count, 0);
	std::atomic<size_t> connected {0};
	std::vector<std::thread> threads;
	for (size_t t = 0; t < thread_count; t++) {
		threads.emplace_back([&, t]() {
			SQLHANDLE env = nullptr;
			SQLHANDLE dbc = nullptr;
			SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
			SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
			SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
			if (!SQL_SUCCEEDED(SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=DuckDB"), SQL_NTS, nullptr, SQL_NTS,
			                                    nullptr, SQL_DRIVER_COMPLETE))) {
				failures[t]++;
				connected++;
			} else {
				HSTMT hstmt = nullptr;
				SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
				int32_t fetched = -1;
				if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, ConvertToSQLCHAR("SELECT col1 FROM tab1"), SQL_NTS)) ||
				    !SQL_SUCCEEDED(SQLFetch(hstmt)) ||
				    !SQL_SUCCEEDED(SQLGetData(hstmt, 1, SQL_C_SLONG, &fetched, sizeof(fetched), nullptr)) ||
				    fetched != 42) {
					failures[t]++;
				}
				SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
				connected++;
				while (connected < thread_count) {
					std::this_thread::yield();
				}
				SQLDisconnect(dbc);
			}
			SQLFreeHandle(SQL_HANDLE_DBC, dbc);
			SQLFreeHandle(SQL_HANDLE_ENV, env);
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto failure_count : failures) {
		REQUIRE(failure_count == 0);
	}
}
