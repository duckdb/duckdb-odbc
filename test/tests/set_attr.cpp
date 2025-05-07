#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test SQL_ATTR_ROW_BIND_TYPE and SQL_ATTR_MAX_LENGTH attributes in SQLSetStmtAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the statement attribute SQL_ATTR_ROW_BIND_TYPE
	uint64_t row_len = 256;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  ConvertToSQLPOINTER(row_len), SQL_IS_INTEGER);

	// Check the statement attribute SQL_ATTR_ROW_BIND_TYPE
	{
		SQLULEN buf;
		EXECUTE_AND_CHECK("SQLGetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", hstmt, SQLGetStmtAttr, hstmt,
		                  SQL_ATTR_ROW_BIND_TYPE, &buf, sizeof(buf), nullptr);
		REQUIRE(row_len == buf);
	}
	{
		SQLULEN buf;
		EXECUTE_AND_CHECK("SQLGetStmtAttrW (SQL_ATTR_ROW_BIND_TYPE)", hstmt, SQLGetStmtAttrW, hstmt,
		                  SQL_ATTR_ROW_BIND_TYPE, &buf, sizeof(buf), nullptr);
		REQUIRE(row_len == buf);
	}

	// Check that SQL_ATTR_MAX_LENGTH client attr cant be set and is preserved
	SQLULEN max_len_buf = 1;
	EXECUTE_AND_CHECK("SQLGetStmtAttr (SQL_ATTR_MAX_LENGTH)", hstmt, SQLGetStmtAttr, hstmt, SQL_ATTR_MAX_LENGTH,
	                  &max_len_buf, sizeof(max_len_buf), nullptr);
	REQUIRE(0 == max_len_buf);
	SQLULEN max_len = 42;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_MAX_LENGTH)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_MAX_LENGTH,
	                  ConvertToSQLPOINTER(max_len), SQL_IS_INTEGER);
	EXECUTE_AND_CHECK("SQLGetStmtAttr (SQL_ATTR_MAX_LENGTH)", hstmt, SQLGetStmtAttr, hstmt, SQL_ATTR_MAX_LENGTH,
	                  &max_len_buf, sizeof(max_len_buf), nullptr);
	REQUIRE(42 == max_len_buf);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test MS Access attribute in SQLSetConnectAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	// Check this vendor-specific attribute can be set before the connection is open
	EXECUTE_AND_CHECK("SQLSetConnectAttr (30002)", nullptr, SQLSetConnectAttr, dbc, 30002,
	                  ConvertToSQLPOINTER(SQL_TRUE), SQL_IS_INTEGER);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", nullptr, SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

TEST_CASE("Test SQL_ATTR_ACCESS_MODE and SQL_ATTR_METADATA_ID attribute in SQLSetConnectAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the Connect attribute SQL_ATTR_ACCESS_MODE to SQL_MODE_READ_ONLY
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_ACCESS_MODE)", hstmt, SQLSetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  ConvertToSQLPOINTER(SQL_MODE_READ_ONLY), SQL_IS_INTEGER);

	// Check the Connect attribute SQL_ATTR_ACCESS_MODE
	SQLUINTEGER buf;
	EXECUTE_AND_CHECK("SQLGetConnectAttr (SQL_ATTR_ACCESS_MODE)", hstmt, SQLGetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  &buf, sizeof(buf), nullptr);
	REQUIRE(SQL_MODE_READ_ONLY == buf);

	// Set the Connect attribute SQL_ATTR_ACCESS_MODE to SQL_MODE_READ_WRITE
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_ACCESS_MODE)", hstmt, SQLSetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  (SQLPOINTER)SQL_MODE_READ_WRITE, SQL_IS_INTEGER);

	// Check the Connect attribute SQL_ATTR_ACCESS_MODE
	EXECUTE_AND_CHECK("SQLGetConnectAttr (SQL_ATTR_ACCESS_MODE)", hstmt, SQLGetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  &buf, sizeof(buf), nullptr);
	REQUIRE(SQL_MODE_READ_WRITE == buf);

	// Set the Connect attribute SQL_ATTR_METADATA_ID to SQL_TRUE
	EXECUTE_AND_CHECK("SQLSetConnectAttrW (SQL_ATTR_METADATA_ID)", hstmt, SQLSetConnectAttrW, dbc, SQL_ATTR_METADATA_ID,
	                  ConvertToSQLPOINTER(SQL_TRUE), SQL_IS_INTEGER);

	std::vector<SQLWCHAR> current_catalog_utf16;
	current_catalog_utf16.resize(64);
	EXECUTE_AND_CHECK("SQLGetConnectAttrW (SQL_ATTR_CURRENT_CATALOG)", hstmt, SQLGetConnectAttrW, dbc,
	                  SQL_ATTR_CURRENT_CATALOG, current_catalog_utf16.data(), current_catalog_utf16.size(), nullptr);
	auto current_catalog_utf8 = ConvertToString(current_catalog_utf16.data());
	REQUIRE(current_catalog_utf8 == std::string(":memory:"));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLSetEnvAttr and SQLGetEnvAttr") {
	SQLHANDLE env;

	EXECUTE_AND_CHECK("SQLAllocHandle (ENV)", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);

	// Set the env attribute SQL_ATTR_ODBC_VERSION to SQL_OV_ODBC3
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	SQLINTEGER odbc_version;
	EXECUTE_AND_CHECK("SQLGetEnvAttr (SQL_ATTR_ODBC_VERSION)", nullptr, SQLGetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  &odbc_version, sizeof(odbc_version), nullptr);
	REQUIRE(odbc_version == SQL_OV_ODBC3);

	// Set the env attribute SQL_ATTR_OUTPUT_NTS to SQL_TRUE
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_OUTPUT_NTS)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_OUTPUT_NTS,
	                  ConvertToSQLPOINTER(SQL_TRUE), 0);
	SQLINTEGER output_nts;
	EXECUTE_AND_CHECK("SQLGetEnvAttr (SQL_ATTR_OUTPUT_NTS)", nullptr, SQLGetEnvAttr, env, SQL_ATTR_OUTPUT_NTS,
	                  &output_nts, sizeof(output_nts), nullptr);
	REQUIRE(output_nts == SQL_TRUE);

	// Set the env attribute SQL_ATTR_CONNECTION_POOLING to SQL_CP_ONE_PER_DRIVER
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_CONNECTION_POOLING)", nullptr, SQLSetEnvAttr, env,
	                  SQL_ATTR_CONNECTION_POOLING, ConvertToSQLPOINTER(SQL_CP_ONE_PER_DRIVER), 0);
	SQLUINTEGER connection_pooling;
	EXECUTE_AND_CHECK("SQLGetEnvAttr (SQL_ATTR_CONNECTION_POOLING)", nullptr, SQLGetEnvAttr, env,
	                  SQL_ATTR_CONNECTION_POOLING, &connection_pooling, sizeof(connection_pooling), nullptr);
	REQUIRE(connection_pooling == SQL_CP_ONE_PER_DRIVER);

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (ENV)", nullptr, SQLFreeHandle, SQL_HANDLE_ENV, env);
}
