#include "../common.h"

#include <iostream>

using namespace odbc_test;

/* Tests the following catalog functions:
 * SQLGetTypeInfo
 * SQLTables
 * SQLColumns
 * SQLGetInfo
 *
 * TODO: Test the following catalog functions:
 * - SQLSpecialColumns
 * - SQLStatistics
 * - SQLPrimaryKeys
 * - SQLForeignKeys
 * - SQLProcedureColumns
 * - SQLTablePrivileges
 * - SQLColumnPrivileges
 * - SQLProcedures
 */

void TestGetTypeInfo(HSTMT &hstmt) {
	SQLRETURN ret;
	SQLSMALLINT col_count;

	// Check for SQLGetTypeInfo
	ret = SQLGetTypeInfo(hstmt, SQL_VARCHAR);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLGetTypeInfo");

	ret = SQLNumResultCols(hstmt, &col_count);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(col_count == 19);

	ret = SQLFetch(hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch");

	map<SQLSMALLINT, SQLULEN> types_map = InitializeTypesMap();
	vector<metadata_data> expected_metadata;
	vector<string> expected_data;
	expected_metadata.push_back({"TYPE_NAME", SQL_VARCHAR});
	expected_data.emplace_back("VARCHAR");
	expected_metadata.push_back({"DATA_TYPE", SQL_SMALLINT});
	expected_data.emplace_back("12");
	expected_metadata.push_back({"COLUMN_SIZE", SQL_INTEGER});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"LITERAL_PREFIX", SQL_VARCHAR});
	expected_data.emplace_back("'");
	expected_metadata.push_back({"LITERAL_SUFFIX", SQL_VARCHAR});
	expected_data.emplace_back("'");
	expected_metadata.push_back({"CREATE_PARAMS", SQL_VARCHAR});
	expected_data.emplace_back("length");
	expected_metadata.push_back({"NULLABLE", SQL_SMALLINT});
	expected_data.emplace_back("1");
	expected_metadata.push_back({"CASE_SENSITIVE", SQL_SMALLINT});
	expected_data.emplace_back("1");
	expected_metadata.push_back({"SEARCHABLE", SQL_SMALLINT});
	expected_data.emplace_back("3");
	expected_metadata.push_back({"UNSIGNED_ATTRIBUTE", SQL_SMALLINT});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"FIXED_PREC_SCALE", SQL_SMALLINT});
	expected_data.emplace_back("0");
	expected_metadata.push_back({"AUTO_UNIQUE_VALUE", SQL_SMALLINT});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"LOCAL_TYPE_NAME", SQL_VARCHAR});
	expected_data.emplace_back("");
	expected_metadata.push_back({"MINIMUM_SCALE", SQL_SMALLINT});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"MAXIMUM_SCALE", SQL_SMALLINT});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"SQL_DATA_TYPE", SQL_SMALLINT});
	expected_data.emplace_back("12");
	expected_metadata.push_back({"SQL_DATETIME_SUB", SQL_SMALLINT});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"NUM_PREC_RADIX", SQL_INTEGER});
	expected_data.emplace_back("-1");
	expected_metadata.push_back({"INTERVAL_PRECISION", SQL_SMALLINT});
	expected_data.emplace_back("-1");

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_metadata[i];
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
		if (expected_data[i].empty()) {
			DATA_CHECK(hstmt, i + 1, nullptr);
			continue;
		}
		DATA_CHECK(hstmt, i + 1, expected_data[i].c_str());
	}
}

static void TestSQLTables(HSTMT &hstmt) {
	SQLRETURN ret;

	ret = SQLTables(hstmt, NULL, 0, (SQLCHAR *)"main", SQL_NTS, (SQLCHAR *)"%", SQL_NTS, (SQLCHAR *)"TABLE", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLTables");

	SQLSMALLINT col_count;

	ret = SQLNumResultCols(hstmt, &col_count);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(col_count == 5);

	map<SQLSMALLINT, SQLULEN> types_map = InitializeTypesMap();
	vector<metadata_data> expected_metadata;
	expected_metadata.push_back({"TABLE_CAT", SQL_VARCHAR});
	expected_metadata.push_back({"TABLE_SCHEM", SQL_VARCHAR});
	expected_metadata.push_back({"TABLE_NAME", SQL_VARCHAR});
	expected_metadata.push_back({"TABLE_TYPE", SQL_VARCHAR});
	expected_metadata.push_back({"REMARKS", SQL_VARCHAR});

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_metadata[i];
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
	}

	int fetch_count = 0;
	do {
		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch");
		fetch_count++;

		DATA_CHECK(hstmt, 1, "memory");
		DATA_CHECK(hstmt, 2, "main");

		switch (fetch_count) {
		case 1:
			DATA_CHECK(hstmt, 3, "bool_table");
			break;
		case 2:
			DATA_CHECK(hstmt, 3, "byte_table");
			break;
		case 3:
			DATA_CHECK(hstmt, 3, "interval_table");
			break;
		case 4:
			DATA_CHECK(hstmt, 3, "lo_test_table");
			break;
		case 5:
			DATA_CHECK(hstmt, 3, "test_table_1");
		}

		DATA_CHECK(hstmt, 4, "TABLE");
	} while (ret == SQL_SUCCESS);
}

// static void TestSQLTablesLong(HSTMT &hstmt) {
//	SQLRETURN ret;
//
//	ret = SQLTables(hstmt, (SQLCHAR *) "", SQL_NTS,
//	                (SQLCHAR *) "main", SQL_NTS,
//	                (SQLCHAR *) "test_table_%", SQL_NTS,
//	                (SQLCHAR *) "1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5, 'TABLE'",
//SQL_NTS); 	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLTables");
//
//	DATA_CHECK(hstmt, 1, "memory");
//	DATA_CHECK(hstmt, 2, "main");
//	DATA_CHECK(hstmt, 3, "test_table_1");
//	DATA_CHECK(hstmt, 4, "TABLE");
// }

static void TestSQLColumns(HSTMT &hstmt) {
	SQLRETURN ret;

	ret = SQLColumns(hstmt, NULL, 0, (SQLCHAR *)"main", SQL_NTS, (SQLCHAR *)"%", SQL_NTS, NULL, 0);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLColumns");

	SQLSMALLINT col_count;

	ret = SQLNumResultCols(hstmt, &col_count);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(col_count == 18);

	// Create a map of column types and a vector of expected metadata
	map<SQLSMALLINT, SQLULEN> types_map = InitializeTypesMap();
	vector<metadata_data> expected_metadata;
	expected_metadata.push_back({"TABLE_CAT", SQL_INTEGER});
	expected_metadata.push_back({"TABLE_SCHEM", SQL_VARCHAR});
	expected_metadata.push_back({"TABLE_NAME", SQL_VARCHAR});
	expected_metadata.push_back({"COLUMN_NAME", SQL_VARCHAR});
	expected_metadata.push_back({"DATA_TYPE", SQL_BIGINT});
	expected_metadata.push_back({"TYPE_NAME", SQL_VARCHAR});
	expected_metadata.push_back({"COLUMN_SIZE", SQL_INTEGER});
	expected_metadata.push_back({"BUFFER_LENGTH", SQL_INTEGER});
	expected_metadata.push_back({"DECIMAL_DIGITS", SQL_INTEGER});
	expected_metadata.push_back({"NUM_PREC_RADIX", SQL_INTEGER});
	expected_metadata.push_back({"NULLABLE", SQL_INTEGER});
	expected_metadata.push_back({"REMARKS", SQL_INTEGER});
	expected_metadata.push_back({"COLUMN_DEF", SQL_VARCHAR});
	expected_metadata.push_back({"SQL_DATA_TYPE", SQL_BIGINT});
	expected_metadata.push_back({"SQL_DATETIME_SUB", SQL_BIGINT});
	expected_metadata.push_back({"CHAR_OCTET_LENGTH", SQL_INTEGER});
	expected_metadata.push_back({"ORDINAL_POSITION", SQL_INTEGER});
	expected_metadata.push_back({"IS_NULLABLE", SQL_VARCHAR});

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_metadata[i];
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
	}

	vector<array<string, 4>> expected_data;
	expected_data.emplace_back(array<string, 4> {"bool_table", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"bool_table", "t", "25", "VARCHAR"});
	expected_data.emplace_back(array<string, 4> {"bool_table", "b", "10", "BOOLEAN"});
	expected_data.emplace_back(array<string, 4> {"byte_table", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"byte_table", "t", "26", "BLOB"});
	expected_data.emplace_back(array<string, 4> {"interval_table", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"interval_table", "iv", "27", "INTERVAL"});
	expected_data.emplace_back(array<string, 4> {"interval_table", "d", "25", "VARCHAR"});
	expected_data.emplace_back(array<string, 4> {"lo_test_table", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"lo_test_table", "large_data", "26", "BLOB"});
	expected_data.emplace_back(array<string, 4> {"test_table_1", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"test_table_1", "t", "25", "VARCHAR"});
	expected_data.emplace_back(array<string, 4> {"test_view", "id", "13", "INTEGER"});
	expected_data.emplace_back(array<string, 4> {"test_view", "t", "25", "VARCHAR"});

	for (int i = 0; i < expected_data.size(); i++) {
		ret = SQLFetch(hstmt);
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch");

		auto &entry = expected_data[i];
		DATA_CHECK(hstmt, 1, nullptr);
		DATA_CHECK(hstmt, 2, "main");
		DATA_CHECK(hstmt, 3, entry[0].c_str());
		DATA_CHECK(hstmt, 4, entry[1].c_str());
		DATA_CHECK(hstmt, 5, entry[2].c_str());
		DATA_CHECK(hstmt, 6, entry[3].c_str());
	}
}

TEST_CASE("catalog_functions", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	// Initializes the database with dummy data
	INITIALIZE_DATABASE(hstmt);

	SQLExecDirect(hstmt, (SQLCHAR *)"DROP TABLE IF EXISTS test_table", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (DROP TABLE)");

	// Check for SQLGetTypeInfo
	TestGetTypeInfo(hstmt);

	// Check for SQLTables
	TestSQLTables(hstmt);
	//	TestSQLTablesLong(hstmt);

	// Check for SQLColumns
	TestSQLColumns(hstmt);

	// Test SQLGetInfo
	char database_name[128];
	SQLSMALLINT len;
	ret = SQLGetInfo(hstmt, SQL_TABLE_TERM, database_name, sizeof(database_name), &len);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLGetInfo (SQL_DATABASE_NAME)");
	REQUIRE(strcmp(database_name, "table") == 0);

	// Free the statement handle
	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect (HDBC)");
}