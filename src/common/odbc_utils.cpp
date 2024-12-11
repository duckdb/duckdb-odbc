#include "odbc_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlext.h"

#include <sql.h>
#include <regex>
#include "duckdb/common/vector.hpp"

using duckdb::OdbcUtils;
using duckdb::vector;
using std::string;

string OdbcUtils::ReadString(const SQLPOINTER ptr, const SQLSMALLINT len) {
	return len == SQL_NTS ? string((const char *)ptr) : string((const char *)ptr, (size_t)len);
}

SQLRETURN OdbcUtils::SetStringValueLength(const string &val_str, SQLLEN *str_len_or_ind_ptr) {
	if (str_len_or_ind_ptr) {
		// it fills the required lenght from string value
		*str_len_or_ind_ptr = val_str.size();
		return SQL_SUCCESS;
	}
	// there is no length pointer
	return SQL_ERROR;
}

bool OdbcUtils::IsCharType(SQLSMALLINT type) {
	switch (type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_WVARCHAR:
	case SQL_BINARY:
		return true;
	default:
		return false;
	}
}

string OdbcUtils::GetStringAsIdentifier(const string &str) {
	string str_ret;

	std::regex regex_str_quoted("^(\".*\"|\'.*\')$");

	if (std::regex_match(str, regex_str_quoted)) {
		// removing quotes
		str_ret = str_ret.substr(1, str_ret.size() - 2);
		// removing leading and trailing blanks
		str_ret = std::regex_replace(str, std::regex("^ +| +$|( ) +"), "$1");
	} else {
		// removing trailing blanks
		str_ret = std::regex_replace(str, std::regex(" +$|( ) +"), "$1");
		str_ret = duckdb::StringUtil::Upper(str_ret);
	}

	return str_ret;
}

string OdbcUtils::ParseStringFilter(const string &filter_name, const string &filter_value,
                                    SQLUINTEGER sql_attr_metadata_id, const string &coalesce_str) {
	string filter;
	if (filter_value.empty()) {
		if (coalesce_str.empty()) {
			filter = "COALESCE(" + filter_name + ",'') LIKE  '%'";
		} else {
			filter = "COALESCE(" + filter_name + ",'" + coalesce_str + "') LIKE '" + coalesce_str + "'";
		}
	} else if (sql_attr_metadata_id == SQL_TRUE) {
		filter = filter_name + "=" + OdbcUtils::GetStringAsIdentifier(filter_value);
	} else {
		filter = filter_name + " LIKE '" + filter_value + "'";
	}
	// Handle escape character passed by Power Query SDK
	filter += " ESCAPE '\\'";
	return filter;
}

string OdbcUtils::GetQueryDuckdbColumns(const string &catalog_filter, const string &schema_filter,
                                        const string &table_filter, const string &column_filter) {
	string sql_duckdb_columns = R"(
        SELECT * EXCLUDE mapping
        FROM (
            SELECT NULL "TABLE_CAT",
            SCHEMA_NAME "TABLE_SCHEM",
            TABLE_NAME "TABLE_NAME",
            COLUMN_NAME "COLUMN_NAME",
            MAP {
                'BOOLEAN': 1, -- SQL_CHAR
                'TINYINT': -6, -- SQL_TINYINT
                'UTINYINT': -6, -- SQL_TINYINT
                'SMALLINT': 5, -- SQL_SMALLINT
                'USMALLINT': 5, -- SQL_SMALLINT
                'INTEGER': 4, -- SQL_INTEGER
                'UINTEGER': 4, -- SQL_INTEGER
                'BIGINT': -5, -- SQL_BIGINT
                'UBIGINT': -5, -- SQL_BIGINT
                'FLOAT': 6, -- SQL_FLOAT
                'HUGEINT': 8, -- SQL_DOUBLE
                'DOUBLE': 8, -- SQL_DOUBLE
                'DATE': 91, -- SQL_TYPE_DATE
                'TIMESTAMP': 93, -- SQL_TYPE_TIMESTAMP
                'TIME': 92, -- SQL_TYPE_TIME
                'VARCHAR': 12, -- SQL_VARCHAR
                'BLOB': -3, -- SQL_VARBINARY
                'INTERVAL': 10, -- SQL_INTERVAL
                'DECIMAL': 8, -- SQL_DOUBLE
                'BIT': -7, -- SQL_BIT
                'LIST': 12 -- SQL_VARCHAR
            } AS mapping,
            CASE
                WHEN mapping[data_type] IS NOT NULL THEN mapping[data_type]::BIGINT
                ELSE data_type_id
            END AS "DATA_TYPE",
            data_type "TYPE_NAME",
            CASE
                WHEN data_type='DATE' THEN 12
                WHEN data_type='TIME' THEN 15
                WHEN data_type LIKE 'TIMESTAMP%' THEN 26
                WHEN data_type='CHAR'
                    OR data_type='BOOLEAN' THEN 1
                WHEN data_type='VARCHAR'
                    OR data_type='BLOB' THEN character_maximum_length
                WHEN data_type LIKE '%INT%' THEN numeric_precision
                WHEN data_type like 'DECIMAL%' THEN numeric_precision
                WHEN data_type='FLOAT'
                    OR data_type='DOUBLE' THEN numeric_precision
                ELSE NULL
            END AS "COLUMN_SIZE",
            CASE
                WHEN data_type='DATE' THEN 4
                WHEN data_type='TIME' THEN 8
                WHEN data_type LIKE 'TIMESTAMP%' THEN 8
                WHEN data_type='CHAR'
                    OR data_type='BOOLEAN' THEN 1
                WHEN data_type='VARCHAR'
                    OR data_type='BLOB' THEN 16
                WHEN data_type LIKE '%TINYINT' THEN 1
                WHEN data_type LIKE '%SMALLINT' THEN 2
                WHEN data_type LIKE '%INTEGER' THEN 4
                WHEN data_type LIKE '%BIGINT' THEN 8
                WHEN data_type='HUGEINT' THEN 16
                WHEN data_type='FLOAT' THEN 4
                WHEN data_type='DOUBLE' THEN 8
                ELSE NULL
            END AS "BUFFER_LENGTH",
            numeric_scale "DECIMAL_DIGITS",
            numeric_precision_radix "NUM_PREC_RADIX",
            CASE is_nullable
                WHEN FALSE THEN 0
                WHEN TRUE THEN 1
                ELSE 2
            END AS "NULLABLE",
            NULL "REMARKS",
            column_default "COLUMN_DEF",
            CASE
                WHEN mapping[data_type] IS NOT NULL THEN mapping[data_type]::BIGINT
                ELSE data_type_id
            END AS "SQL_DATA_TYPE",
            CASE
                WHEN data_type='DATE'
                     OR data_type='TIME'
                     OR data_type LIKE 'TIMESTAMP%' THEN data_type_id
                ELSE NULL
            END AS "SQL_DATETIME_SUB",
            CASE
                WHEN data_type='%CHAR'
                     OR data_type='BLOB' THEN character_maximum_length
                ELSE NULL
            END AS "CHAR_OCTET_LENGTH",
            column_index AS "ORDINAL_POSITION",
            CASE is_nullable
                WHEN FALSE THEN 'NO'
                WHEN TRUE THEN 'YES'
                ELSE ''
            END AS "IS_NULLABLE"
            FROM duckdb_columns
        )
	)";

	sql_duckdb_columns += " WHERE ";
	if (!catalog_filter.empty()) {
		sql_duckdb_columns += catalog_filter + " AND ";
	}
	if (!schema_filter.empty()) {
		sql_duckdb_columns += schema_filter + " AND ";
	}
	if (!table_filter.empty()) {
		sql_duckdb_columns += table_filter + " AND ";
	}
	sql_duckdb_columns += column_filter;

	// clang-format off
	sql_duckdb_columns += R"(
		ORDER BY
			"TABLE_CAT",
			"TABLE_SCHEM",
			"TABLE_NAME",
			"ORDINAL_POSITION"
	)";
	// clang-format on

	return sql_duckdb_columns;
}

string OdbcUtils::GetQueryDuckdbTables(const string &schema_filter, const string &table_filter,
                                       const string &table_type_filter) {
	string sql_duckdb_tables = R"(
		SELECT
			table_catalog::VARCHAR "TABLE_CAT",
			table_schema "TABLE_SCHEM",
			table_name "TABLE_NAME",
			CASE
				WHEN table_type='BASE TABLE'
				THEN 'TABLE'
				ELSE table_type
			END "TABLE_TYPE",
			'' "REMARKS"
			FROM information_schema.tables
	)";

	sql_duckdb_tables += " WHERE " + schema_filter + " AND " + table_filter;

	if (!table_type_filter.empty()) {
		if (table_type_filter != "'%'") {
			sql_duckdb_tables += " AND table_type IN (" + table_type_filter + ") ";
		}
	}

	sql_duckdb_tables += "ORDER BY TABLE_TYPE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME";

	return sql_duckdb_tables;
}

SQLUINTEGER OdbcUtils::SQLPointerToSQLUInteger(SQLPOINTER value) {
	return static_cast<SQLUINTEGER>(reinterpret_cast<SQLULEN>(value));
}

std::string OdbcUtils::ConvertSQLCHARToString(SQLCHAR *str) {
	return std::string(reinterpret_cast<char *>(str));
}

LPCSTR duckdb::OdbcUtils::ConvertStringToLPCSTR(const std::string &str) {
	return reinterpret_cast<LPCSTR>(const_cast<char *>(str.c_str()));
}
