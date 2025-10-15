#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "api_info.hpp"
#include "widechar.hpp"
#include "odbc_diagnostic.hpp"
#include "row_descriptor.hpp"

#include "duckdb/main/prepared_statement_data.hpp"

using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::Load;
using duckdb::LogicalType;
using duckdb::SQLStateType;
using duckdb::Value;

//===--------------------------------------------------------------------===//
// SQLDescribeCol
//===--------------------------------------------------------------------===//

static SQLRETURN DescribeColInternal(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLCHAR *column_name,
                                     SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr,
                                     SQLSMALLINT *data_type_ptr, SQLULEN *column_size_ptr,
                                     SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (column_number > hstmt->stmt->ColumnCount()) {
		return SQL_ERROR;
	}

	duckdb::idx_t col_idx = column_number - 1;

	duckdb::OdbcUtils::WriteString(hstmt->stmt->GetNames()[col_idx], column_name, buffer_length, name_length_ptr);

	LogicalType col_type = hstmt->stmt->GetTypes()[col_idx];

	if (data_type_ptr) {
		*data_type_ptr = duckdb::ApiInfo::FindRelatedSQLType(col_type.id());
	}
	if (column_size_ptr) {
		*column_size_ptr = duckdb::ApiInfo::GetColumnSize(col_type);
	}
	if (decimal_digits_ptr) {
		*decimal_digits_ptr = 0;
		if (col_type.id() == duckdb::LogicalTypeId::DECIMAL) {
			*decimal_digits_ptr = duckdb::DecimalType::GetScale(col_type);
		} else {
			auto sql_type = duckdb::ApiInfo::FindRelatedSQLType(col_type.id());
			if (sql_type == SQL_TYPE_TIME || sql_type == SQL_TYPE_TIMESTAMP) {
				*decimal_digits_ptr = duckdb::ApiInfo::GetTemporalPrecision(col_type.id());
			}
		}
	}
	if (nullable_ptr) {
		*nullable_ptr = SQL_NULLABLE_UNKNOWN;
	}
	return SQL_SUCCESS;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribecol-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLCHAR *column_name,
                                 SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr, SQLSMALLINT *data_type_ptr,
                                 SQLULEN *column_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr) {
	return DescribeColInternal(statement_handle, column_number, column_name, buffer_length, name_length_ptr,
	                           data_type_ptr, column_size_ptr, decimal_digits_ptr, nullable_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribecol-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDescribeColW(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLWCHAR *column_name,
                                  SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr, SQLSMALLINT *data_type_ptr,
                                  SQLULEN *column_size_ptr, SQLSMALLINT *decimal_digits_ptr,
                                  SQLSMALLINT *nullable_ptr) {
	auto column_name_vec = duckdb::widechar::utf8_alloc_out_vec(buffer_length);
	auto ret = DescribeColInternal(statement_handle, column_number, column_name_vec.data(),
	                               static_cast<SQLSMALLINT>(column_name_vec.size()), name_length_ptr, data_type_ptr,
	                               column_size_ptr, decimal_digits_ptr, nullable_ptr);
	duckdb::widechar::utf16_write_str(ret, column_name_vec, column_name, buffer_length, name_length_ptr);
	return ret;
}

//===--------------------------------------------------------------------===//
// SQLColAttribute
//===--------------------------------------------------------------------===//
template <typename T>
static SQLRETURN SetNumericAttributePtr(duckdb::OdbcHandleStmt *hstmt, const T &val, SQLLEN *numeric_attribute_ptr) {
	if (numeric_attribute_ptr) {
		duckdb::Store<SQLLEN>(val, reinterpret_cast<duckdb::data_ptr_t>(numeric_attribute_ptr));
		return SQL_SUCCESS;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
	                                   "Memory management error; numeric "
	                                   "attribute pointer is null",
	                                   SQLStateType::ST_HY013, hstmt->dbc->GetDataSourceName());
}

template <typename CHAR_TYPE>
static SQLRETURN SetCharacterAttributePtr(duckdb::OdbcHandleStmt *hstmt, const std::string &str,
                                          CHAR_TYPE *character_attribute_ptr, SQLSMALLINT buffer_length,
                                          SQLSMALLINT *string_length_ptr) {
	// user only wants the size of the character attribute
	if (character_attribute_ptr == nullptr && string_length_ptr) {
		*string_length_ptr = static_cast<SQLSMALLINT>(str.size());
		return SQL_SUCCESS;
	}
	if (buffer_length <= 0) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Invalid string or buffer length",
		                                   SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}
	if (character_attribute_ptr) {
		duckdb::OdbcUtils::WriteString(str, character_attribute_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
	                                   "Memory management error; character "
	                                   "attribute pointer is null",
	                                   SQLStateType::ST_HY013, hstmt->dbc->GetDataSourceName());
}

template <typename CHAR_TYPE>
static SQLRETURN ColAttributeInternal(SQLHSTMT statement_handle, SQLUSMALLINT column_number,
                                      SQLUSMALLINT field_identifier, CHAR_TYPE *character_attribute_ptr,
                                      SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr,
                                      SQLLEN *numeric_attribute_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (field_identifier != SQL_DESC_COUNT && hstmt->res &&
	    hstmt->res->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
		                                   "Prepared statement not a cursor-specification", SQLStateType::ST_07005,
		                                   hstmt->dbc->GetDataSourceName());
	}

	// TODO: SQL_DESC_TYPE and SQL_DESC_OCTET_LENGTH should return values if column_number is 0, otherwise they should
	// return undefined values
	if (column_number < 1 || column_number > hstmt->stmt->GetTypes().size()) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Invalid descriptor index",
		                                   SQLStateType::ST_07009, hstmt->dbc->GetDataSourceName());
	}

	duckdb::idx_t col_idx = column_number - 1;
	auto desc_record = hstmt->row_desc->GetIRD()->GetDescRecord(col_idx);

	switch (field_identifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE: {
		ret = SetNumericAttributePtr(hstmt, desc_record->sql_desc_auto_unique_value, numeric_attribute_ptr);
		if (ret == SQL_ERROR) {
			return ret;
		}
		return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
		                                   "DuckDB does not support autoincrementing columns", SQLStateType::ST_HYC00,
		                                   hstmt->dbc->GetDataSourceName());
	}
	case SQL_DESC_BASE_COLUMN_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_base_column_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_BASE_TABLE_NAME: {
		// not possible to access this information (also not possible in HandleStmt::FillIRD)
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Driver not capable",
		                                   SQLStateType::ST_HYC00, hstmt->dbc->GetDataSourceName());
	}
	case SQL_DESC_CASE_SENSITIVE:
		// DuckDB is case insensitive so will always return false
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_case_sensitive, numeric_attribute_ptr);
	case SQL_DESC_CATALOG_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_catalog_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_CONCISE_TYPE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_concise_type, numeric_attribute_ptr);
	case SQL_COLUMN_COUNT:
	case SQL_DESC_COUNT:
		return SetNumericAttributePtr(hstmt, hstmt->row_desc->GetIRD()->GetRecordCount(), numeric_attribute_ptr);
	case SQL_DESC_DISPLAY_SIZE: {
		ret = SetNumericAttributePtr(hstmt, desc_record->sql_desc_display_size, numeric_attribute_ptr);
		if (desc_record->sql_desc_display_size == 0) {
			return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
			                                   "Cannot determine the column or parameter length of variable types",
			                                   SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
		}
		return ret;
	}
	case SQL_DESC_FIXED_PREC_SCALE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_fixed_prec_scale, numeric_attribute_ptr);
	case SQL_DESC_LABEL:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_label,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_LENGTH:
	case SQL_DESC_LENGTH:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_length, numeric_attribute_ptr);
	case SQL_DESC_LITERAL_PREFIX:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_literal_prefix,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_LITERAL_SUFFIX:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_literal_suffix,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_LOCAL_TYPE_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_local_type_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_NAME:
	case SQL_DESC_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_NULLABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_nullable, numeric_attribute_ptr);
	case SQL_DESC_NULLABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_nullable, numeric_attribute_ptr);
	case SQL_DESC_NUM_PREC_RADIX:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_num_prec_radix, numeric_attribute_ptr);
	case SQL_DESC_OCTET_LENGTH:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_octet_length, numeric_attribute_ptr);
	case SQL_COLUMN_PRECISION:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_precision, numeric_attribute_ptr);
	case SQL_DESC_PRECISION:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_precision, numeric_attribute_ptr);
	case SQL_COLUMN_SCALE:
	case SQL_DESC_SCALE: {
		if (desc_record->sql_desc_scale == -1) {
			ret = SetNumericAttributePtr(hstmt, SQL_NO_TOTAL, numeric_attribute_ptr);
			if (SQL_SUCCEEDED(ret)) {
				return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
				                                   "SQL_NO_TOTAL: Scale is undefined for this column data type",
				                                   SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
			}
			return ret;
		}
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_scale, numeric_attribute_ptr);
	}
	case SQL_DESC_SCHEMA_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_schema_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_SEARCHABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_searchable, numeric_attribute_ptr);
	case SQL_DESC_TYPE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_type, numeric_attribute_ptr);
	case SQL_DESC_TYPE_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_type_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_UNNAMED:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_unnamed, numeric_attribute_ptr);
	case SQL_DESC_UNSIGNED:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_unsigned, numeric_attribute_ptr);
	case SQL_DESC_UPDATABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_updatable, numeric_attribute_ptr);
	default: {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
		                                   "Invalid descriptor field identifier", SQLStateType::ST_HY091,
		                                   hstmt->dbc->GetDataSourceName());
	}
	}
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                  SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return ColAttributeInternal(statement_handle, column_number, field_identifier,
	                            reinterpret_cast<SQLCHAR *>(character_attribute_ptr), buffer_length, string_length_ptr,
	                            numeric_attribute_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                   SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	auto character_attribute_vec = duckdb::widechar::utf8_alloc_out_vec(buffer_length);
	auto ret = ColAttributeInternal(statement_handle, column_number, field_identifier, character_attribute_vec.data(),
	                                static_cast<SQLSMALLINT>(character_attribute_vec.size()), string_length_ptr,
	                                numeric_attribute_ptr);

	SQLSMALLINT buf_len_chars = buffer_length / sizeof(SQLWCHAR);
	SQLSMALLINT written_chars = 0;
	duckdb::widechar::utf16_write_str(ret, character_attribute_vec,
	                                  reinterpret_cast<SQLWCHAR *>(character_attribute_ptr), buf_len_chars,
	                                  &written_chars);
	if (string_length_ptr != nullptr) {
		*string_length_ptr = written_chars * sizeof(SQLWCHAR);
	}

	return ret;
}

/**
 * Deprecated. Use SQLColAttribute or SQLColAttributeW instead.
 */
SQLRETURN SQL_API SQLColAttributes(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                   SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return ColAttributeInternal(statement_handle, column_number, field_identifier,
	                            reinterpret_cast<SQLCHAR *>(character_attribute_ptr), buffer_length, string_length_ptr,
	                            numeric_attribute_ptr);
}

//===--------------------------------------------------------------------===//
// SQLNumResultCols
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlnumresultcols-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT statement_handle, SQLSMALLINT *column_count_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (!column_count_ptr) {
		return SQL_ERROR;
	}
	*column_count_ptr = (SQLSMALLINT)hstmt->stmt->ColumnCount();

	switch (hstmt->stmt->data->statement_type) {
	case duckdb::StatementType::CALL_STATEMENT:
	case duckdb::StatementType::EXECUTE_STATEMENT:
	case duckdb::StatementType::EXPLAIN_STATEMENT:
	case duckdb::StatementType::SELECT_STATEMENT:
		break;
	default:
		*column_count_ptr = 0;
	}
	return SQL_SUCCESS;
}
