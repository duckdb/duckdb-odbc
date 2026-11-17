#include "odbc_test_common.h"
#include "temp_directory.hpp"

#include <thread>
#include <vector>

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

TEST_CASE("Test concurrent connections to several database files", "[odbc]") {
	TempDirectory tmpdir;
	const size_t file_count = 3;
	const size_t connections_per_file = 4;
	std::vector<std::string> conn_strs;
	for (size_t i = 0; i < file_count; i++) {
		conn_strs.push_back("Database=" + tmpdir.path + "/concurrent" + std::to_string(i) + ".db");
		SQLHANDLE env, dbc;
		DRIVER_CONNECT_TO_DATABASE(env, dbc, conn_strs.back());
		HSTMT hstmt = SQL_NULL_HSTMT;
		EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
		EXEC_SQL(hstmt, "CREATE TABLE t (i INTEGER)");
		EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
		DISCONNECT_FROM_DATABASE(env, dbc);
	}

	// every database is opened by connections racing to connect to it, while others connect to the other files.
	// Catch's assertions are not thread-safe: the threads only record their outcome.
	struct ConcurrentConnection {
		SQLHANDLE env = nullptr;
		SQLHANDLE dbc = nullptr;
		SQLRETURN connect_ret = SQL_ERROR;
		SQLRETURN insert_ret = SQL_ERROR;
	};
	std::vector<ConcurrentConnection> connections(file_count * connections_per_file);
	std::vector<std::thread> threads;
	for (size_t i = 0; i < connections.size(); i++) {
		threads.emplace_back([&connections, &conn_strs, i, file_count]() {
			auto &connection = connections[i];
			SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &connection.env);
			SQLSetEnvAttr(connection.env, SQL_ATTR_ODBC_VERSION, ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
			SQLAllocHandle(SQL_HANDLE_DBC, connection.env, &connection.dbc);
			SQLCHAR out[1024];
			SQLSMALLINT out_len;
			connection.connect_ret =
			    SQLDriverConnect(connection.dbc, nullptr, ConvertToSQLCHAR(conn_strs[i % file_count]), SQL_NTS, out,
			                     sizeof(out), &out_len, SQL_DRIVER_COMPLETE);
			if (!SQL_SUCCEEDED(connection.connect_ret)) {
				return;
			}
			HSTMT hstmt = SQL_NULL_HSTMT;
			SQLAllocHandle(SQL_HANDLE_STMT, connection.dbc, &hstmt);
			connection.insert_ret = SQLExecDirect(hstmt, ConvertToSQLCHAR("INSERT INTO t VALUES (1)"), SQL_NTS);
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto &connection : connections) {
		REQUIRE(connection.connect_ret == SQL_SUCCESS);
		REQUIRE(connection.insert_ret == SQL_SUCCESS);
	}

	// the connections to a file share one database: each sees the rows inserted through the others
	for (size_t i = 0; i < file_count; i++) {
		HSTMT hstmt = SQL_NULL_HSTMT;
		EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, connections[i].dbc, &hstmt);
		REQUIRE(FetchCount(hstmt, "SELECT count(*) FROM t") == static_cast<SQLBIGINT>(connections_per_file));
		EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	}
	for (auto &connection : connections) {
		DISCONNECT_FROM_DATABASE(connection.env, connection.dbc);
	}
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

//! Inserts a row through a manual-commit connection and rolls it back: it must not have been committed on its own.
static void RequireInTransaction(HSTMT hstmt, SQLHANDLE dbc, HSTMT check_hstmt) {
	auto before = FetchCount(check_hstmt, "SELECT count(*) FROM tm");
	EXEC_SQL(hstmt, "INSERT INTO tm VALUES (100)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLEndTran (ROLLBACK)", nullptr, SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
	REQUIRE(FetchCount(check_hstmt, "SELECT count(*) FROM tm") == before);
}

TEST_CASE("Test manual-commit mode after commented or failed transaction statements", "[odbc]") {
	TempDirectory tmpdir;
	std::string conn_str = "Database=" + tmpdir.path + "/manual_commit_errors.db";

	SQLHANDLE env1, dbc1, env2, dbc2, env3, dbc3;
	DRIVER_CONNECT_TO_DATABASE(env1, dbc1, conn_str);
	DRIVER_CONNECT_TO_DATABASE(env2, dbc2, conn_str);
	DRIVER_CONNECT_TO_DATABASE(env3, dbc3, conn_str);
	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;
	HSTMT hstmt3 = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt1, SQLAllocHandle, SQL_HANDLE_STMT, dbc1, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt2, SQLAllocHandle, SQL_HANDLE_STMT, dbc2, &hstmt2);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt3, SQLAllocHandle, SQL_HANDLE_STMT, dbc3, &hstmt3);
	EXEC_SQL(hstmt3, "CREATE TABLE tm (i INTEGER)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt3, SQLFreeStmt, hstmt3, SQL_CLOSE);
	EXEC_SQL(hstmt3, "CREATE TABLE conflict (i INTEGER PRIMARY KEY)");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt3, SQLFreeStmt, hstmt3, SQL_CLOSE);

	for (auto dbc : {dbc1, dbc2}) {
		EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_AUTOCOMMIT)", nullptr, SQLSetConnectAttr, dbc,
		                  SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
	}

	// COMMIT and ROLLBACK issued as SQL after a comment
	EXEC_SQL(hstmt1, "/* done */ COMMIT");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	RequireInTransaction(hstmt1, dbc1, hstmt3);
	EXEC_SQL(hstmt1, "-- done\nROLLBACK");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
	RequireInTransaction(hstmt1, dbc1, hstmt3);

	// a commit that fails on a conflict rolls the transaction back: a new one is started
	int key = 0;
	auto conflict = [&]() {
		// the same key, inserted by two transactions: the second one to commit fails
		auto insert = "INSERT INTO conflict VALUES (" + std::to_string(++key) + ")";
		EXEC_SQL(hstmt1, insert);
		EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt1, SQLFreeStmt, hstmt1, SQL_CLOSE);
		EXEC_SQL(hstmt2, insert);
		EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt2, SQLFreeStmt, hstmt2, SQL_CLOSE);
		EXECUTE_AND_CHECK("SQLEndTran (COMMIT)", nullptr, SQLEndTran, SQL_HANDLE_DBC, dbc1, SQL_COMMIT);
	};
	conflict();
	REQUIRE(SQLEndTran(SQL_HANDLE_DBC, dbc2, SQL_COMMIT) == SQL_ERROR);
	RequireInTransaction(hstmt2, dbc2, hstmt3);

	conflict();
	REQUIRE(SQLExecDirect(hstmt2, ConvertToSQLCHAR("COMMIT"), SQL_NTS) == SQL_ERROR);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt2, SQLFreeStmt, hstmt2, SQL_CLOSE);
	RequireInTransaction(hstmt2, dbc2, hstmt3);

	// switching to auto-commit fails with the commit, and the connection stays in manual-commit mode
	conflict();
	REQUIRE(SQLSetConnectAttr(dbc2, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER) == SQL_ERROR);
	RequireInTransaction(hstmt2, dbc2, hstmt3);

	for (auto hstmt : {hstmt1, hstmt2, hstmt3}) {
		EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	}
	DISCONNECT_FROM_DATABASE(env1, dbc1);
	DISCONNECT_FROM_DATABASE(env2, dbc2);
	DISCONNECT_FROM_DATABASE(env3, dbc3);
}

TEST_CASE("Test connecting a connected handle", "[odbc]") {
	SQLHANDLE env, dbc;
	CONNECT_TO_DATABASE(env, dbc);
	SQLCHAR out[1024];
	SQLSMALLINT out_len;
	REQUIRE(SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("Database=:memory:"), SQL_NTS, out, sizeof(out), &out_len,
	                         SQL_DRIVER_COMPLETE) == SQL_ERROR);
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, dbc, SQL_HANDLE_DBC);
	REQUIRE(state == "08002");

	// the connection is still usable
	HSTMT hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	REQUIRE(FetchCount(hstmt, "SELECT count(*) FROM range(3)") == 3);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test connections with equivalent configurations share a database", "[odbc]") {
	TempDirectory tmpdir;
	auto connect = [](const std::string &conn_str, SQLHANDLE &env, SQLHANDLE &dbc) {
		REQUIRE(SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env) == SQL_SUCCESS);
		EXECUTE_AND_CHECK("SQLSetEnvAttr", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
		                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
		EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);
		SQLCHAR out[1024];
		SQLSMALLINT out_len;
		return SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR(conn_str), SQL_NTS, out, sizeof(out), &out_len,
		                        SQL_DRIVER_COMPLETE);
	};

	std::string database = "Database=" + tmpdir.path + "/equivalent.db";
	// the same settings, written differently: an alias, a different case, a different number format
	std::vector<std::string> equivalent = {database + ";threads=3;access_mode=READ_WRITE",
	                                       database + ";worker_threads=3;access_mode=read_write",
	                                       database + ";threads=03;ACCESS_MODE=Read_Write"};
	std::vector<std::pair<SQLHANDLE, SQLHANDLE>> handles;
	for (auto &conn_str : equivalent) {
		INFO(conn_str);
		SQLHANDLE env, dbc;
		REQUIRE(SQL_SUCCEEDED(connect(conn_str, env, dbc)));
		handles.emplace_back(env, dbc);
	}

	// a different setting
	SQLHANDLE env, dbc;
	REQUIRE(connect(database + ";threads=2;access_mode=read_write", env, dbc) == SQL_ERROR);
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, dbc, SQL_HANDLE_DBC);
	REQUIRE(message.find("different configuration") != std::string::npos);
	EXECUTE_AND_CHECK("SQLFreeHandle (DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
	EXECUTE_AND_CHECK("SQLFreeHandle (ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);

	for (auto &handle : handles) {
		DISCONNECT_FROM_DATABASE(handle.first, handle.second);
	}
}
