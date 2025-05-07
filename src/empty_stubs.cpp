#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "statement_functions.hpp"

//! The following ODBC stub functions aren't implemented yet,
//! when implementing the function must be moved to the proper source file

// ============================================================================
// Functions returning valid empty result sets (i.e., SELECT ... WHERE 1 < 0)
// ============================================================================
SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC connection_handle, SQLWCHAR *in_statement_text, SQLINTEGER text_length1,
                                SQLWCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	std::cout << "***** SQLNativeSqlW" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC connection_handle, SQLCHAR *in_connection_string, SQLSMALLINT string_length1,
                                   SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length2_ptr) {
	std::cout << "***** SQLBrowseConnect" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
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

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLColumnPrivilegesW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                       SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                       SQLSMALLINT name_length3, SQLWCHAR *column_name, SQLSMALLINT name_length4) {
	std::cout << "***** SQLColumnPrivilegesW" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT statement_handle, SQLCHAR *pk_catalog_name, SQLSMALLINT name_length1,
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

SQLRETURN SQL_API SQLForeignKeysW(SQLHSTMT statement_handle, SQLWCHAR *pk_catalog_name, SQLSMALLINT name_length1,
                                  SQLWCHAR *pk_schema_name, SQLSMALLINT name_length2, SQLWCHAR *pk_table_name,
                                  SQLSMALLINT name_length3, SQLWCHAR *fk_catalog_name, SQLSMALLINT name_length4,
                                  SQLWCHAR *fk_schema_name, SQLSMALLINT name_length5, SQLWCHAR *fk_table_name,
                                  SQLSMALLINT name_length6) {
	std::cout << "***** SQLForeignKeysW" << std::endl;
	return SQL_ERROR;
}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLPrimaryKeysW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                  SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                  SQLSMALLINT name_length3) {
	std::cout << "***** SQLPrimaryKeysW" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
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
	CAST(0 AS INT) AS "PRECISION",
	CAST(0 AS INT) AS "LENGTH",
	CAST(0 AS SMALLINT) AS "SCALE",
	CAST(0 AS SMALLINT) AS "RADIX",
	CAST(0 AS SMALLINT) AS "NULLABLE",
	CAST('' AS VARCHAR) AS "REMARKS"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLProcedureColumnsW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                       SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *proc_name,
                                       SQLSMALLINT name_length3, SQLWCHAR *column_name, SQLSMALLINT name_length4) {
	std::cout << "***** SQLProcedureColumnsW" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                SQLSMALLINT name_length3) {
	const std::string query = R"#(
SELECT
	CAST('' AS VARCHAR) AS "PROCEDURE_CAT",
	CAST('' AS VARCHAR) AS "PROCEDURE_SCHEM",
	CAST('' AS VARCHAR) AS "PROCEDURE_NAME",
	CAST('' AS VARCHAR) AS "REMARKS",
	CAST(0 AS SMALLINT) AS "PROCEDURE_TYPE"
WHERE 1 < 0
)#";
	duckdb::OdbcHandleStmt *stmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
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

SQLRETURN SQL_API SQLProceduresW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *proc_name,
                                 SQLSMALLINT name_length3) {
	std::cout << "***** SQLProceduresW" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT statement_handle, SQLSETPOSIROW row_number, SQLUSMALLINT operation,
                            SQLUSMALLINT lock_type) {
	std::cout << "***** SQLSetPos" << std::endl;
	return SQL_ERROR;
	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                     SQLSMALLINT name_length3) {
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

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT statement_handle, SQLUSMALLINT identifier_type, SQLCHAR *catalog_name,
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

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                SQLSMALLINT name_length3, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
	const std::string query = R"#(
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

	return duckdb::ExecDirectStmt(stmt, duckdb::OdbcUtils::ConvertStringToSQLCHAR(query), query.size());
}

// ============================================================================
// Functions explicitly marked as not implemented (SetNotImplemented)
// ============================================================================

SQLRETURN SetNotImplemented(duckdb::OdbcHandle *handle, const std::string &func_name) {
	return SetDiagnosticRecord(handle, SQL_ERROR, func_name, func_name + " is not implemented",
	                           duckdb::SQLStateType::ST_HYC00, "");
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connection_handle, SQLCHAR *in_statement_text, SQLINTEGER text_length1,
                               SQLCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(connection_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLNativeSql");
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC connection_handle, SQLCHAR *in_connection_string, SQLSMALLINT string_length1,
                                   SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length2_ptr) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(connection_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLBrowseConnect");
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT statement_handle, SQLSMALLINT operation) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(statement_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLBulkOperations");
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT statement_handle, SQLSETPOSIROW row_number, SQLUSMALLINT operation,
                            SQLUSMALLINT lock_type) {
	duckdb::OdbcHandle *hdl;
	const auto ret = ConvertHandle(statement_handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return SetNotImplemented(hdl, "SQLSetPos");
}

SQLRETURN SQL_API SQLTablePrivilegesW(SQLHSTMT statement_handle, SQLWCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLWCHAR *schema_name, SQLSMALLINT name_length2, SQLWCHAR *table_name,
                                      SQLSMALLINT name_length3) {
	std::cout << "***** SQLTablePrivilegesW" << std::endl;
	return SQL_ERROR;
}
