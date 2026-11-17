#include "odbc_test_common.h"
#include "temp_directory.hpp"

using namespace odbc_test;

static SQLBIGINT FetchCount(HSTMT hstmt, const std::string &query) {
	EXEC_SQL(hstmt, query);
	SQLBIGINT count = -1;
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_SBIGINT, &count, sizeof(count), nullptr);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	return count;
}

TEST_CASE("Test connections to the same database file share it", "[odbc]") {
	TempDirectory tmpdir;
	std::string conn_str = "Database=" + tmpdir.path + "/shared.db";

	SQLHANDLE env1, dbc1, env2, dbc2;
	DRIVER_CONNECT_TO_DATABASE(env1, dbc1, conn_str);
	DRIVER_CONNECT_TO_DATABASE(env2, dbc2, conn_str);

	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt1, SQLAllocHandle, SQL_HANDLE_STMT, dbc1, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt2, SQLAllocHandle, SQL_HANDLE_STMT, dbc2, &hstmt2);

	// a table created through one connection is seen through the other
	EXEC_SQL(hstmt1, "CREATE TABLE shared AS SELECT range AS i FROM range(10)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM shared") == 10);

	// a connection with a different configuration cannot share the database
	SQLHANDLE env3, dbc3;
	REQUIRE(SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env3) == SQL_SUCCESS);
	EXECUTE_AND_CHECK("SQLSetEnvAttr", nullptr, SQLSetEnvAttr, env3, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env3, &dbc3);
	std::string other_config = conn_str + ";threads=1";
	SQLCHAR out[1024];
	SQLSMALLINT out_len;
	REQUIRE(SQLDriverConnect(dbc3, nullptr, ConvertToSQLCHAR(other_config.c_str()), SQL_NTS, out, sizeof(out), &out_len,
	                         SQL_DRIVER_COMPLETE) == SQL_ERROR);
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, dbc3, SQL_HANDLE_DBC);
	REQUIRE(state == "IM003");
	REQUIRE(message.find("different configuration") != std::string::npos);
	EXECUTE_AND_CHECK("SQLFreeHandle (DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc3);
	EXECUTE_AND_CHECK("SQLFreeHandle (ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env3);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt1, SQLFreeHandle, SQL_HANDLE_STMT, hstmt1);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt2, SQLFreeHandle, SQL_HANDLE_STMT, hstmt2);
	DISCONNECT_FROM_DATABASE(env1, dbc1);
	DISCONNECT_FROM_DATABASE(env2, dbc2);

	// once every connection is closed, the database can be opened with another configuration
	DRIVER_CONNECT_TO_DATABASE(env3, dbc3, conn_str + ";access_mode=read_only");
	HSTMT hstmt3 = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt3, SQLAllocHandle, SQL_HANDLE_STMT, dbc3, &hstmt3);
	REQUIRE(FetchCount(hstmt3, "SELECT count(*) FROM shared") == 10);
	REQUIRE(SQLExecDirect(hstmt3, ConvertToSQLCHAR("INSERT INTO shared VALUES (42)"), SQL_NTS) == SQL_ERROR);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt3, SQLFreeHandle, SQL_HANDLE_STMT, hstmt3);
	DISCONNECT_FROM_DATABASE(env3, dbc3);
}

TEST_CASE("Test interleaving statements on one connection", "[odbc]") {
	SQLHANDLE env, dbc;
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");

	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt1, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt2, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt2);

	// a result of several chunks is read from while other statements run on the same connection
	EXEC_SQL(hstmt1, "SELECT range AS i FROM range(10000)");
	SQLBIGINT expected = 0;
	SQLBIGINT value;
	for (; expected < 10; expected++) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt1, SQLFetch, hstmt1);
		EXECUTE_AND_CHECK("SQLGetData", hstmt1, SQLGetData, hstmt1, 1, SQL_C_SBIGINT, &value, sizeof(value), nullptr);
		REQUIRE(value == expected);
	}
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM range(123)") == 123);
	EXEC_SQL(hstmt2, "CREATE TABLE interleaved (i INTEGER)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt2, SQLFreeStmt, hstmt2, SQL_CLOSE);
	for (; SQLFetch(hstmt1) != SQL_NO_DATA; expected++) {
		EXECUTE_AND_CHECK("SQLGetData", hstmt1, SQLGetData, hstmt1, 1, SQL_C_SBIGINT, &value, sizeof(value), nullptr);
		REQUIRE(value == expected);
	}
	REQUIRE(expected == 10000);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt1, SQLFreeHandle, SQL_HANDLE_STMT, hstmt1);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt2, SQLFreeHandle, SQL_HANDLE_STMT, hstmt2);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test manual-commit mode", "[odbc]") {
	TempDirectory tmpdir;
	std::string conn_str = "Database=" + tmpdir.path + "/manual_commit.db";

	SQLHANDLE env1, dbc1, env2, dbc2;
	DRIVER_CONNECT_TO_DATABASE(env1, dbc1, conn_str);
	DRIVER_CONNECT_TO_DATABASE(env2, dbc2, conn_str);
	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt1, SQLAllocHandle, SQL_HANDLE_STMT, dbc1, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt2, SQLAllocHandle, SQL_HANDLE_STMT, dbc2, &hstmt2);
	EXEC_SQL(hstmt1, "CREATE TABLE tx (i INTEGER)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);

	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_AUTOCOMMIT)", nullptr, SQLSetConnectAttr, dbc1, SQL_ATTR_AUTOCOMMIT,
	                  (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);

	// rolled back
	EXEC_SQL(hstmt1, "INSERT INTO tx VALUES (1)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 0);
	EXECUTE_AND_CHECK("SQLEndTran (ROLLBACK)", nullptr, SQLEndTran, SQL_HANDLE_DBC, dbc1, SQL_ROLLBACK);
	REQUIRE(FetchCount(hstmt1, "SELECT count(*) FROM tx") == 0);

	// committed, and the connection stays in manual-commit mode afterwards
	EXEC_SQL(hstmt1, "INSERT INTO tx VALUES (2)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLEndTran (COMMIT)", nullptr, SQLEndTran, SQL_HANDLE_DBC, dbc1, SQL_COMMIT);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 1);
	EXEC_SQL(hstmt1, "INSERT INTO tx VALUES (3)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 1);

	// a COMMIT issued as SQL also keeps the connection in manual-commit mode
	EXEC_SQL(hstmt1, "COMMIT");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 2);
	EXEC_SQL(hstmt1, "INSERT INTO tx VALUES (4)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 2);

	// switching back to auto-commit commits the open transaction
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_AUTOCOMMIT)", nullptr, SQLSetConnectAttr, dbc1, SQL_ATTR_AUTOCOMMIT,
	                  (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER);
	REQUIRE(FetchCount(hstmt2, "SELECT count(*) FROM tx") == 3);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt1, SQLFreeHandle, SQL_HANDLE_STMT, hstmt1);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt2, SQLFreeHandle, SQL_HANDLE_STMT, hstmt2);
	DISCONNECT_FROM_DATABASE(env1, dbc1);
	DISCONNECT_FROM_DATABASE(env2, dbc2);
}
