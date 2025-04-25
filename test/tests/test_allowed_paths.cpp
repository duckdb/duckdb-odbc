#include "odbc_test_common.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

using namespace odbc_test;

static void check_read_file(HSTMT hstmt, const std::string &path, SQLRETURN expected_ret = SQL_SUCCESS,
                            const std::string &expected_state = std::string(),
                            const std::string &expected_msg = std::string()) {
	std::string text("foo");

	{ // Write the file
		std::ofstream file(path);
		file << text;
	}

	// Try to read the file
	auto ret = SQLExecDirect(hstmt, ConvertToSQLCHAR("SELECT * FROM read_text('" + path + "')"), SQL_NTS);

	// Clean up promptly
	std::remove(path.c_str());

	// Check expected success or failure
	REQUIRE(ret == expected_ret);

	// Check details
	if (expected_ret == SQL_SUCCESS) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		std::vector<char> fetched_buf;
		fetched_buf.resize(256);
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_CHAR, fetched_buf.data(), fetched_buf.size(),
		                  nullptr);
		REQUIRE(std::string(fetched_buf.data()) == text);
	} else {
		std::string state, message;
		ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
		REQUIRE(state == expected_state);
		REQUIRE(duckdb::StringUtil::Contains(message, expected_msg));
	}
}

TEST_CASE("Test allowed_paths option", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// TODO: need to add generic scratch test dir support
	auto fs = duckdb::FileSystem::CreateLocal();
	std::string dir = fs->GetWorkingDirectory();
	std::string file_allowed_1 = fs->JoinPath(dir, "test_allowed_paths_1.txt");
	std::string file_allowed_2 = fs->JoinPath(dir, "test_allowed_paths_2.txt");
	std::string file_not_allowed_1 = fs->JoinPath(dir, "test_allowed_paths_3.txt");

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc,
	                           "Driver={DuckDB Driver};enable_external_access=false;"
	                           "allowed_paths=['" +
	                               file_allowed_1 + "', '" + file_allowed_2 + "']");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check read from allowed path
	check_read_file(hstmt, file_allowed_1);

	// Check read from unallowed path
	check_read_file(hstmt, file_not_allowed_1, SQL_ERROR, "42000", "Permission Error: Cannot access file");

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test allowed_directories option", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// TODO: need to add generic scratch test dir support
	auto fs = duckdb::FileSystem::CreateLocal();
	std::string dir = fs->GetWorkingDirectory();
	std::string dir_allowed_1 = fs->JoinPath(dir, "test_allowed_paths_dir_1");
	fs->CreateDirectory(dir_allowed_1);
	std::string dir_allowed_2 = fs->JoinPath(dir, "test_allowed_paths_dir_2");
	std::string dir_not_allowed_1 = fs->JoinPath(dir, "test_allowed_paths_dir_3");
	fs->CreateDirectory(dir_not_allowed_1);

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc,
	                           "Driver={DuckDB Driver};enable_external_access=false;"
	                           "allowed_directories=['" +
	                               dir_allowed_1 + "', '" + dir_allowed_2 + "']");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check read from allowed path
	check_read_file(hstmt, fs->JoinPath(dir_allowed_1, "foo.txt"));

	// Check read from unallowed path
	check_read_file(hstmt, fs->JoinPath(dir_not_allowed_1, "foo.txt"), SQL_ERROR, "42000",
	                "Permission Error: Cannot access file");

	// Cleanup
	fs->RemoveDirectory(dir_allowed_1);
	fs->RemoveDirectory(dir_not_allowed_1);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
