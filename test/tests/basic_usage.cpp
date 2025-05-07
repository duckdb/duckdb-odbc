#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Basic ODBC usage", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "DuckDB";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr);

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle (DBC)", nullptr);

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS);
	ODBC_CHECK(ret, "SQLConnect", nullptr);

	ret = SQLConnectW(dbc, ConvertToSQLWCHARNTS(dsn).data(), SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS);
	ODBC_CHECK(ret, "SQLConnectW", nullptr);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle (STMT)", stmt);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle (STMT)", stmt);

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect", stmt);

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle (DBC)", stmt);

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle (ENV)", stmt);
}
