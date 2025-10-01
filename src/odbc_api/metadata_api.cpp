#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "widechar.hpp"

#include "duckdb/common/constants.hpp"

#include <regex>

using duckdb::LogicalTypeId;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;

//===--------------------------------------------------------------------===//
// SQLTables
//===--------------------------------------------------------------------===//

static SQLRETURN TablesInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                SQLSMALLINT name_length3, SQLCHAR *table_type, SQLSMALLINT name_length4) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	// String search pattern for catalog name
	auto catalog_n = OdbcUtils::ConvertSQLCHARToString(catalog_name, name_length1);
	std::string catalog_filter =
	    OdbcUtils::ParseStringFilter("\"TABLE_CAT\"", catalog_n, hstmt->dbc->sql_attr_metadata_id);

	// String search pattern for schema name
	auto schema_n = OdbcUtils::ConvertSQLCHARToString(schema_name, name_length2);
	std::string schema_filter =
	    OdbcUtils::ParseStringFilter("\"TABLE_SCHEM\"", schema_n, hstmt->dbc->sql_attr_metadata_id);

	// String search pattern for table name
	auto table_n = OdbcUtils::ConvertSQLCHARToString(table_name, name_length3);
	std::string table_filter =
	    OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, hstmt->dbc->sql_attr_metadata_id);

	// Table types
	auto table_tp = OdbcUtils::ConvertSQLCHARToString(table_type, name_length4);

	std::string tables_query;

	// Note, Excel violates the special cases spec by passing CatalogName=SQL_ALL_CATALOGS and TableType="TABLE,VIEW" to
	// list tables, so we also check for empty TableType below.
	if (catalog_n == std::string(SQL_ALL_CATALOGS) && schema_n.empty() && table_n.empty() && table_tp.empty()) {
		// If CatalogName is SQL_ALL_CATALOGS and SchemaName and TableName are empty strings, the result set contains a
		// list of valid catalogs for the data source. (All columns except the TABLE_CAT column contain NULLs.)
		tables_query = R"#(
SELECT DISTINCT
	CAST(catalog_name AS VARCHAR) AS "TABLE_CAT",
	CAST(NULL         AS VARCHAR) AS "TABLE_SCHEM",
	CAST(NULL         AS VARCHAR) AS "TABLE_NAME",
	CAST(NULL         AS VARCHAR) AS "TABLE_TYPE",
	CAST(NULL         AS VARCHAR) AS "REMARKS"
FROM information_schema.schemata
WHERE catalog_name NOT IN (
	'system',
	'temp')
AND catalog_name NOT LIKE '__ducklake_%'
ORDER BY "TABLE_CAT"
)#";
	} else if (schema_n == std::string(SQL_ALL_SCHEMAS) && catalog_n.empty() && table_n.empty() && table_tp.empty()) {
		// If SchemaName is SQL_ALL_SCHEMAS and CatalogName and TableName are empty strings, the result set contains a
		// list of valid schemas for the data source. (All columns except the TABLE_SCHEM column contain NULLs.)
		tables_query = R"#(
SELECT
	CAST(NULL        AS VARCHAR) AS "TABLE_CAT",
	CAST(schema_name AS VARCHAR) AS "TABLE_SCHEM",
	CAST(NULL        AS VARCHAR) AS "TABLE_NAME",
	CAST(NULL        AS VARCHAR) AS "TABLE_TYPE",
	CAST(NULL        AS VARCHAR) AS "REMARKS"
FROM information_schema.schemata
WHERE catalog_name NOT IN (
	'system',
	'temp')
AND catalog_name NOT LIKE '__ducklake_%'
ORDER BY "TABLE_SCHEM"
)#";
	} else if (table_tp == std::string(SQL_ALL_TABLE_TYPES) && catalog_n.empty() && schema_n.empty() &&
	           table_n.empty()) {
		// If TableType is SQL_ALL_TABLE_TYPES and CatalogName, SchemaName, and TableName are empty strings, the result
		// set contains a list of valid table types for the data source. (All columns except the TABLE_TYPE column
		// contain NULLs.)
		tables_query = R"#(
SELECT
	CAST(NULL AS VARCHAR) AS "TABLE_CAT",
	CAST(NULL AS VARCHAR) AS "TABLE_SCHEM",
	CAST(NULL AS VARCHAR) AS "TABLE_NAME",
	UNNEST([
		CAST('TABLE' AS VARCHAR),
		CAST('VIEW'  AS VARCHAR)
	]) AS "TABLE_TYPE",
	CAST(NULL AS VARCHAR) AS "REMARKS"
)#";
	} else {
		// .Net ODBC driver GetSchema() call includes "SYSTEM TABLE" (doesn't exist in DuckDB),
		// and the regex tweaks below break if "SYSTEM TABLE" is in the query, so remove it
		table_tp = std::regex_replace(table_tp, std::regex("('SYSTEM TABLE'|SYSTEM TABLE)"), "");
		table_tp = std::regex_replace(table_tp, std::regex(",\\s*,"), ",");

		table_tp = std::regex_replace(table_tp, std::regex("('TABLE'|TABLE)"), "'BASE TABLE'");
		table_tp = std::regex_replace(table_tp, std::regex("('VIEW'|VIEW)"), "'VIEW'");
		table_tp = std::regex_replace(table_tp, std::regex("('%'|%)"), "'%'");

		tables_query = OdbcUtils::GetQueryDuckdbTables(catalog_filter, schema_filter, table_filter, table_tp);
	}

	return duckdb::ExecDirectStmt(hstmt, tables_query);
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLTables(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                            SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                            SQLSMALLINT name_length3, SQLCHAR *table_type, SQLSMALLINT name_length4) {
	return TablesInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                      name_length3, table_type, name_length4);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLTablesW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                             SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                             SQLSMALLINT name_length3, SQLWCHAR *table_type, SQLSMALLINT name_length4) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	auto table_type_conv = duckdb::widechar::utf16_conv(table_type, name_length4);
	return TablesInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                      schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(), table_name_conv.utf8_str,
	                      table_name_conv.utf8_len_smallint(), table_type_conv.utf8_str,
	                      table_type_conv.utf8_len_smallint());
}

//===--------------------------------------------------------------------===//
// SQLColumns
//===--------------------------------------------------------------------===//

static SQLRETURN ColumnsInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                 SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	auto catalog_n = OdbcUtils::ConvertSQLCHARToString(catalog_name, name_length1);
	std::string catalog_filter;
	if (catalog_n.empty()) {
		catalog_filter = "\"TABLE_CAT\" IS NULL";
	} else if (hstmt->dbc->sql_attr_metadata_id == SQL_TRUE) {
		catalog_filter = "\"TABLE_CAT\"=" + OdbcUtils::GetStringAsIdentifier(catalog_n);
	}

	// String search pattern for schema name
	auto schema_n = OdbcUtils::ConvertSQLCHARToString(schema_name, name_length2);
	std::string schema_filter = OdbcUtils::ParseStringFilter(
	    "\"TABLE_SCHEM\"", schema_n, hstmt->dbc->sql_attr_metadata_id, std::string(DEFAULT_SCHEMA));

	// String search pattern for table name
	auto table_n = OdbcUtils::ConvertSQLCHARToString(table_name, name_length3);
	std::string table_filter =
	    OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, hstmt->dbc->sql_attr_metadata_id);

	// String search pattern for column name
	auto column_n = OdbcUtils::ConvertSQLCHARToString(column_name, name_length4);
	std::string column_filter =
	    OdbcUtils::ParseStringFilter("\"COLUMN_NAME\"", column_n, hstmt->dbc->sql_attr_metadata_id);

	std::string sql_columns =
	    OdbcUtils::GetQueryDuckdbColumns(catalog_filter, schema_filter, table_filter, column_filter);

	ret = duckdb::ExecDirectStmt(hstmt, sql_columns);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	return SQL_SUCCESS;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                             SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                             SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	return ColumnsInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                       name_length3, column_name, name_length4);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLColumnsW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                              SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                              SQLSMALLINT name_length3, SQLWCHAR *column_name, SQLSMALLINT name_length4) {

	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	auto column_name_conv = duckdb::widechar::utf16_conv(column_name, name_length4);

	return ColumnsInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                       schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(), table_name_conv.utf8_str,
	                       table_name_conv.utf8_len_smallint(), column_name_conv.utf8_str,
	                       column_name_conv.utf8_len_smallint());
}
