#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "statement_functions.hpp"
#include "widechar.hpp"

//! The following ODBC stub functions aren't implemented yet,
//! when implementing the function must be moved to the proper source file

// ============================================================================
// Functions returning valid empty result sets (i.e., SELECT ... WHERE 1 < 0)
// ============================================================================

// -----------------------------------------------------------------------------
// SQLPrimaryKeys and SQLPrimaryKeysW
// -----------------------------------------------------------------------------

static SQLRETURN PrimaryKeysInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                     SQLSMALLINT name_length3) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "TABLE_CAT",
	CAST('' AS VARCHAR) AS "TABLE_SCHEM",
	CAST('' AS VARCHAR) AS "TABLE_NAME",
	CAST('' AS VARCHAR) AS "COLUMN_NAME",
	CAST(0 AS SMALLINT) AS "KEY_SEQ",
	CAST('' AS VARCHAR) AS "PK_NAME"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                 SQLSMALLINT name_length3) {
	return PrimaryKeysInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                           name_length3);
}

SQLRETURN SQL_API SQLPrimaryKeysW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                  SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                  SQLSMALLINT name_length3) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	return PrimaryKeysInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                           schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(),
	                           table_name_conv.utf8_str, table_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLForeignKeys and SQLForeignKeysW
// -----------------------------------------------------------------------------

static SQLRETURN ForeignKeysInternal(SQLHSTMT statement_handle, SQLCHAR *pk_catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *pk_schema_name, SQLSMALLINT name_length2, SQLCHAR *pk_table_name,
                                     SQLSMALLINT name_length3, SQLCHAR *fk_catalog_name, SQLSMALLINT name_length4,
                                     SQLCHAR *fk_schema_name, SQLSMALLINT name_length5, SQLCHAR *fk_table_name,
                                     SQLSMALLINT name_length6) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "PKTABLE_CAT",
	CAST('' AS VARCHAR) AS "PKTABLE_SCHEM",
	CAST('' AS VARCHAR) AS "PKTABLE_NAME",
	CAST('' AS VARCHAR) AS "PKCOLUMN_NAME",
	CAST('' AS VARCHAR) AS "FKTABLE_CAT",
	CAST('' AS VARCHAR) AS "FKTABLE_SCHEM",
	CAST('' AS VARCHAR) AS "FKTABLE_NAME",
	CAST('' AS VARCHAR) AS "FKCOLUMN_NAME",
	CAST(0 AS SMALLINT) AS "KEY_SEQ",
	CAST(0 AS SMALLINT) AS "UPDATE_RULE",
	CAST(0 AS SMALLINT) AS "DELETE_RULE",
	CAST('' AS VARCHAR) AS "FK_NAME",
	CAST('' AS VARCHAR) AS "PK_NAME",
	CAST(0 AS SMALLINT) AS "DEFERRABILITY"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT statement_handle, SQLCHAR *pk_catalog_name, SQLSMALLINT name_length1,
                                 SQLCHAR *pk_schema_name, SQLSMALLINT name_length2, SQLCHAR *pk_table_name,
                                 SQLSMALLINT name_length3, SQLCHAR *fk_catalog_name, SQLSMALLINT name_length4,
                                 SQLCHAR *fk_schema_name, SQLSMALLINT name_length5, SQLCHAR *fk_table_name,
                                 SQLSMALLINT name_length6) {
	return ForeignKeysInternal(statement_handle, pk_catalog_name, name_length1, pk_schema_name, name_length2,
	                           pk_table_name, name_length3, fk_catalog_name, name_length4, fk_schema_name, name_length5,
	                           fk_table_name, name_length6);
}

SQLRETURN SQL_API SQLForeignKeysW(SQLHSTMT statement_handle, SQLWCHAR *pk_catalog_name, SQLSMALLINT name_length1,
                                  SQLWCHAR *pk_schema_name, SQLSMALLINT name_length2, SQLWCHAR *pk_table_name,
                                  SQLSMALLINT name_length3, SQLWCHAR *fk_catalog_name, SQLSMALLINT name_length4,
                                  SQLWCHAR *fk_schema_name, SQLSMALLINT name_length5, SQLWCHAR *fk_table_name,
                                  SQLSMALLINT name_length6) {
	auto pk_catalog_name_conv = duckdb::widechar::utf16_conv(pk_catalog_name, name_length1);
	auto pk_schema_name_conv = duckdb::widechar::utf16_conv(pk_schema_name, name_length2);
	auto pk_table_name_conv = duckdb::widechar::utf16_conv(pk_table_name, name_length3);
	auto fk_catalog_name_conv = duckdb::widechar::utf16_conv(fk_catalog_name, name_length4);
	auto fk_schema_name_conv = duckdb::widechar::utf16_conv(fk_schema_name, name_length5);
	auto fk_table_name_conv = duckdb::widechar::utf16_conv(fk_table_name, name_length6);
	return ForeignKeysInternal(
	    statement_handle, pk_catalog_name_conv.utf8_str, pk_catalog_name_conv.utf8_len_smallint(),
	    pk_schema_name_conv.utf8_str, pk_schema_name_conv.utf8_len_smallint(), pk_table_name_conv.utf8_str,
	    pk_table_name_conv.utf8_len_smallint(), fk_catalog_name_conv.utf8_str, fk_catalog_name_conv.utf8_len_smallint(),
	    fk_schema_name_conv.utf8_str, fk_schema_name_conv.utf8_len_smallint(), fk_table_name_conv.utf8_str,
	    fk_table_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLProcedureColumns and SQLProcedureColumnsW
// -----------------------------------------------------------------------------

SQLRETURN SQL_API ProcedureColumnsInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                           SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                           SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "PROCEDURE_CAT",
	CAST('' AS VARCHAR) AS "PROCEDURE_SCHEM",
	CAST('' AS VARCHAR) AS "PROCEDURE_NAME",
	CAST('' AS VARCHAR) AS "COLUMN_NAME",
	CAST(0 AS SMALLINT) AS "COLUMN_TYPE",
	CAST(0 AS SMALLINT) AS "DATA_TYPE",
	CAST('' AS VARCHAR) AS "TYPE_NAME",
	CAST(0 AS INT) AS "COLUMN_SIZE",
	CAST(0 AS INT) AS "BUFFER_LENGTH",
	CAST(0 AS SMALLINT) AS "DECIMAL_DIGITS",
	CAST(0 AS SMALLINT) AS "NUM_PREC_RADIX",
	CAST(0 AS SMALLINT) AS "NULLABLE",
	CAST('' AS VARCHAR) AS "REMARKS",
	CAST('' AS VARCHAR) AS "COLUMN_DEF",
	CAST(0 AS SMALLINT) AS "SQL_DATA_TYPE",
	CAST(0 AS SMALLINT) AS "SQL_DATETIME_SUB",
	CAST(0 AS INT) AS "CHAR_OCTET_LENGTH",
	CAST(0 AS INT) AS "ORDINAL_POSITION",
	CAST('' AS VARCHAR) AS "IS_NULLABLE"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                      SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	return ProcedureColumnsInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, proc_name,
	                                name_length3, column_name, name_length4);
}

SQLRETURN SQL_API SQLProcedureColumnsW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                       SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *proc_name,
                                       SQLSMALLINT name_length3, SQLWCHAR *column_name, SQLSMALLINT name_length4) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto proc_name_conv = duckdb::widechar::utf16_conv(proc_name, name_length3);
	auto column_name_conv = duckdb::widechar::utf16_conv(column_name, name_length4);
	return ProcedureColumnsInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                                schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(),
	                                proc_name_conv.utf8_str, proc_name_conv.utf8_len_smallint(),
	                                column_name_conv.utf8_str, column_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLProcedures and SQLProceduresW
// -----------------------------------------------------------------------------

SQLRETURN SQL_API ProceduresInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                     SQLSMALLINT name_length3) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "PROCEDURE_CAT",
	CAST('' AS VARCHAR) AS "PROCEDURE_SCHEM",
	CAST('' AS VARCHAR) AS "PROCEDURE_NAME",
	CAST(0 AS INT) AS "NUM_INPUT_PARAMS",
	CAST(0 AS INT) AS "NUM_OUTPUT_PARAMS",
	CAST(0 AS INT) AS "NUM_RESULT_SETS",
	CAST('' AS VARCHAR) AS "REMARKS",
	CAST(0 AS SMALLINT) AS "PROCEDURE_TYPE"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                SQLSMALLINT name_length3) {
	return ProceduresInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, proc_name,
	                          name_length3);
}

SQLRETURN SQL_API SQLProceduresW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *proc_name,
                                 SQLSMALLINT name_length3) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto proc_name_conv = duckdb::widechar::utf16_conv(proc_name, name_length3);
	return ProceduresInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                          schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(), proc_name_conv.utf8_str,
	                          proc_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLColumnPrivileges and SQLColumnPrivilegesW
// -----------------------------------------------------------------------------

static SQLRETURN ColumnPrivilegesInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                          SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                          SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "TABLE_CAT",
	CAST('' AS VARCHAR) AS "TABLE_SCHEM",
	CAST('' AS VARCHAR) AS "TABLE_NAME",
	CAST('' AS VARCHAR) AS "COLUMN_NAME",
	CAST('' AS VARCHAR) AS "GRANTOR",
	CAST('' AS VARCHAR) AS "GRANTEE",
	CAST('' AS VARCHAR) AS "PRIVILEGE",
	CAST('' AS VARCHAR) AS "IS_GRANTABLE"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                      SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	return ColumnPrivilegesInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                                name_length3, column_name, name_length4);
}

SQLRETURN SQL_API SQLColumnPrivilegesW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                       SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                       SQLSMALLINT name_length3, SQLWCHAR *column_name, SQLSMALLINT name_length4) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	auto column_name_conv = duckdb::widechar::utf16_conv(column_name, name_length4);
	return ColumnPrivilegesInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                                schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(),
	                                table_name_conv.utf8_str, table_name_conv.utf8_len_smallint(),
	                                column_name_conv.utf8_str, column_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLTablePrivileges and SQLTablePrivilegesW
// -----------------------------------------------------------------------------

static SQLRETURN TablePrivilegesInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                         SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                         SQLSMALLINT name_length3) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "TABLE_CAT",
	CAST('' AS VARCHAR) AS "TABLE_SCHEM",
	CAST('' AS VARCHAR) AS "TABLE_NAME",
	CAST('' AS VARCHAR) AS "GRANTOR",
	CAST('' AS VARCHAR) AS "GRANTEE",
	CAST('' AS VARCHAR) AS "PRIVILEGE",
	CAST('' AS VARCHAR) AS "IS_GRANTABLE"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                     SQLSMALLINT name_length3) {
	return TablePrivilegesInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                               name_length3);
}

SQLRETURN SQL_API SQLTablePrivilegesW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                      SQLSMALLINT name_length3) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	return TablePrivilegesInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                               schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(),
	                               table_name_conv.utf8_str, table_name_conv.utf8_len_smallint());
}

// -----------------------------------------------------------------------------
// SQLSpecialColumns and SQLSpecialColumnsW
// -----------------------------------------------------------------------------

static SQLRETURN SpecialColumnsInternal(SQLHSTMT statement_handle, SQLUSMALLINT identifier_type, SQLCHAR *catalog_name,
                                        SQLSMALLINT name_length1, SQLCHAR *schema_name, SQLSMALLINT name_length2,
                                        SQLCHAR *table_name, SQLSMALLINT name_length3, SQLUSMALLINT scope,
                                        SQLUSMALLINT nullable) {
	const std::string query = R"#(
SELECT
	CAST(0  AS SMALLINT) AS "SCOPE",
	CAST('' AS VARCHAR ) AS "COLUMN_NAME",
	CAST(0  AS SMALLINT) AS "DATA_TYPE",
	CAST('' AS VARCHAR ) AS "TYPE_NAME",
	CAST(0  AS INT     ) AS "COLUMN_SIZE",
	CAST(0  AS INT     ) AS "BUFFER_LENGTH",
	CAST(0  AS SMALLINT) AS "DECIMAL_DIGITS",
	CAST(0  AS SMALLINT) AS "PSEUDO_COLUMN"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT statement_handle, SQLUSMALLINT identifier_type, SQLCHAR *catalog_name,
                                    SQLSMALLINT name_length1, SQLCHAR *schema_name, SQLSMALLINT name_length2,
                                    SQLCHAR *table_name, SQLSMALLINT name_length3, SQLUSMALLINT scope,
                                    SQLUSMALLINT nullable) {
	return SpecialColumnsInternal(statement_handle, identifier_type, catalog_name, name_length1, schema_name,
	                              name_length2, table_name, name_length3, scope, nullable);
}

SQLRETURN SQL_API SQLSpecialColumnsW(SQLHSTMT statement_handle, SQLUSMALLINT identifier_type, SQLWCHAR *catalog_name,
                                     SQLSMALLINT name_length1, SQLWCHAR *schema_name, SQLSMALLINT name_length2,
                                     SQLWCHAR *table_name, SQLSMALLINT name_length3, SQLUSMALLINT scope,
                                     SQLUSMALLINT nullable) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	return SpecialColumnsInternal(statement_handle, identifier_type, catalog_name_conv.utf8_str,
	                              catalog_name_conv.utf8_len_smallint(), schema_name_conv.utf8_str,
	                              schema_name_conv.utf8_len_smallint(), table_name_conv.utf8_str,
	                              table_name_conv.utf8_len_smallint(), scope, nullable);
}

// -----------------------------------------------------------------------------
// SQLStatistics and SQLStatisticsW
// -----------------------------------------------------------------------------

static SQLRETURN StatisticsInternal(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                    SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                    SQLSMALLINT name_length3, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
	std::string query = R"#(
SELECT
	CAST('' AS VARCHAR ) AS "TABLE_CAT",
	CAST('' AS VARCHAR ) AS "TABLE_SCHEM",
	CAST('' AS VARCHAR ) AS "TABLE_NAME",
	CAST(0  AS SMALLINT) AS "NON_UNIQUE",
	CAST('' AS VARCHAR ) AS "INDEX_QUALIFIER",
	CAST('' AS VARCHAR ) AS "INDEX_NAME",
	CAST(0  AS SMALLINT) AS "TYPE",
	CAST(0  AS SMALLINT) AS "ORDINAL_POSITION",
	CAST('' AS VARCHAR ) AS "COLUMN_NAME",
	CAST('' AS CHAR(1) ) AS "ASC_OR_DESC",
	CAST(0  AS INT     ) AS "CARDINALITY",
	CAST(0  AS INT     ) AS "PAGES",
	CAST('' AS VARCHAR ) AS "FILTER_CONDITION"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query),
	                              static_cast<SQLINTEGER>(query.size()));
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                SQLSMALLINT name_length3, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
	return StatisticsInternal(statement_handle, catalog_name, name_length1, schema_name, name_length2, table_name,
	                          name_length3, unique, reserved);
}

SQLRETURN SQL_API SQLStatisticsW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                 SQLSMALLINT name_length3, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
	auto catalog_name_conv = duckdb::widechar::utf16_conv(catalog_name, name_length1);
	auto schema_name_conv = duckdb::widechar::utf16_conv(schema_name, name_length2);
	auto table_name_conv = duckdb::widechar::utf16_conv(table_name, name_length3);
	return StatisticsInternal(statement_handle, catalog_name_conv.utf8_str, catalog_name_conv.utf8_len_smallint(),
	                          schema_name_conv.utf8_str, schema_name_conv.utf8_len_smallint(), table_name_conv.utf8_str,
	                          table_name_conv.utf8_len_smallint(), unique, reserved);
}

// ============================================================================
// Functions explicitly marked as not implemented (SetNotImplemented)
// ============================================================================

static SQLRETURN SetNotImplemented(duckdb::OdbcHandle *handle, const std::string &func_name) {
	return SetDiagnosticRecord(handle, SQL_ERROR, func_name, func_name + " is not implemented",
	                           duckdb::SQLStateType::ST_HYC00, "");
}

// --------------------------------------------------------------
// SQLNativeSql and SQLNativeSqlW
// --------------------------------------------------------------

static SQLRETURN NativeSQLInternal(SQLHDBC connection_handle, SQLCHAR *in_statement_text, SQLINTEGER text_length1,
                                   SQLCHAR *out_statement_text, SQLINTEGER buffer_length,
                                   SQLINTEGER *text_length2_ptr) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(connection_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLNativeSql");
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connection_handle, SQLCHAR *in_statement_text, SQLINTEGER text_length1,
                               SQLCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	return NativeSQLInternal(connection_handle, in_statement_text, text_length1, out_statement_text, buffer_length,
	                         text_length2_ptr);
}

SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC connection_handle, SQLWCHAR *in_statement_text, SQLINTEGER text_length1,
                                SQLWCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	auto in_statement_text_conv = duckdb::widechar::utf16_conv(in_statement_text, text_length1);
	auto out_statement_text_conv = duckdb::widechar::utf16_conv(out_statement_text, buffer_length);
	return NativeSQLInternal(connection_handle, in_statement_text_conv.utf8_str,
	                         in_statement_text_conv.utf8_len_smallint(), out_statement_text_conv.utf8_str,
	                         out_statement_text_conv.utf8_len_smallint(), text_length2_ptr);
}

// --------------------------------------------------------------
// SQLBrowseConnect
// --------------------------------------------------------------

static SQLRETURN BrowseConnectInternal(SQLHDBC connection_handle, SQLCHAR *in_connection_string,
                                       SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                       SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(connection_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLBrowseConnect");
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC connection_handle, SQLCHAR *in_connection_string, SQLSMALLINT string_length1,
                                   SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length2_ptr) {
	return BrowseConnectInternal(connection_handle, in_connection_string, string_length1, out_connection_string,
	                             buffer_length, string_length2_ptr);
}

SQLRETURN SQL_API SQLBrowseConnectW(SQLHDBC connection_handle, SQLWCHAR *in_connection_string,
                                    SQLSMALLINT string_length1, SQLWCHAR *out_connection_string,
                                    SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr) {
	auto in_connection_string_conv = duckdb::widechar::utf16_conv(in_connection_string, string_length1);
	auto out_connection_string_conv = duckdb::widechar::utf16_conv(out_connection_string, buffer_length);
	return BrowseConnectInternal(connection_handle, in_connection_string_conv.utf8_str,
	                             in_connection_string_conv.utf8_len_smallint(), out_connection_string_conv.utf8_str,
	                             out_connection_string_conv.utf8_len_smallint(), string_length2_ptr);
}

// --------------------------------------------------------------
// SQLBulkOperations
// --------------------------------------------------------------

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT statement_handle, SQLSMALLINT operation) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(statement_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLBulkOperations");
}

// --------------------------------------------------------------
// SQLSetPos
// --------------------------------------------------------------

SQLRETURN SQL_API SQLSetPos(SQLHSTMT statement_handle, SQLSETPOSIROW row_number, SQLUSMALLINT operation,
                            SQLUSMALLINT lock_type) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(statement_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLSetPos");
}
