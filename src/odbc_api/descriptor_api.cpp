#include "descriptor.hpp"
#include "api_info.hpp"
#include "odbc_interval.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "handle_functions.hpp"

#include "duckdb/common/vector.hpp"

using duckdb::ApiInfo;
using duckdb::DescHeader;
using duckdb::DescRecord;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcInterval;
using duckdb::vector;

//===--------------------------------------------------------------------===//
// SQLGetDescField
//===--------------------------------------------------------------------===//

template <typename CHAR_TYPE>
static SQLRETURN GetDescFieldInternal(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                      SQLPOINTER value_ptr, SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	duckdb::OdbcHandleDesc *desc = nullptr;
	SQLRETURN ret = ConvertDescriptor(descriptor_handle, desc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (duckdb::ApiInfo::IsNumericDescriptorField(field_identifier) && value_ptr == nullptr) {
		return duckdb::SetDiagnosticRecord(desc, SQL_ERROR, "SQLGetDescField",
		                                   "Invalid null value pointer for descriptor numeric field",
		                                   duckdb::SQLStateType::ST_HY009, desc->dbc->GetDataSourceName());
	}

	// descriptor header fields
	switch (field_identifier) {
	case SQL_DESC_ALLOC_TYPE: {
		*(SQLSMALLINT *)value_ptr = desc->header.sql_desc_alloc_type;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ARRAY_SIZE: {
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLULEN *)value_ptr = desc->header.sql_desc_array_size;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ARRAY_STATUS_PTR: {
		*(SQLUSMALLINT **)value_ptr = desc->header.sql_desc_array_status_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_BIND_OFFSET_PTR: {
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLLEN **)value_ptr = desc->header.sql_desc_bind_offset_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_BIND_TYPE: {
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLINTEGER *)value_ptr = desc->header.sql_desc_bind_type;
		return SQL_SUCCESS;
	}
	case SQL_DESC_COUNT: {
		*(SQLSMALLINT *)value_ptr = desc->header.sql_desc_count;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ROWS_PROCESSED_PTR: {
		if (desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLULEN **)value_ptr = desc->header.sql_desc_rows_processed_ptr;
		return SQL_SUCCESS;
	}
	default:
		break;
	}

	if (rec_number <= 0 || rec_number > (SQLSMALLINT)desc->records.size()) {
		return duckdb::SetDiagnosticRecord(desc, SQL_ERROR, "SQLGetDescField", "Invalid descriptor index",
		                                   duckdb::SQLStateType::ST_07009, desc->dbc->GetDataSourceName());
	}
	duckdb::idx_t rec_idx = rec_number - 1;

	// checking descriptor record fields
	switch (field_identifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		// always false, DuckDB doesn't support auto-incrementing
		*(SQLINTEGER *)value_ptr = SQL_FALSE;
		return SQL_SUCCESS;
	}
	case SQL_DESC_BASE_COLUMN_NAME: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_base_column_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_BASE_TABLE_NAME: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_base_table_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_CASE_SENSITIVE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLINTEGER *)value_ptr = desc->records[rec_idx].sql_desc_case_sensitive;
		return SQL_SUCCESS;
	}
	case SQL_DESC_CATALOG_NAME: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_catalog_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_CONCISE_TYPE: {
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_concise_type;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATA_PTR: {
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		value_ptr = desc->records[rec_idx].sql_desc_data_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATETIME_INTERVAL_CODE: {
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_datetime_interval_code;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATETIME_INTERVAL_PRECISION: {
		*(SQLINTEGER *)value_ptr = desc->records[rec_idx].sql_desc_datetime_interval_precision;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DISPLAY_SIZE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLLEN *)value_ptr = desc->records[rec_idx].sql_desc_display_size;
		return SQL_SUCCESS;
	}
	case SQL_DESC_FIXED_PREC_SCALE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_fixed_prec_scale;
		return SQL_SUCCESS;
	}
	case SQL_DESC_INDICATOR_PTR: {
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLLEN **)value_ptr = desc->records[rec_idx].sql_desc_indicator_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_LABEL: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_label, reinterpret_cast<CHAR_TYPE *>(value_ptr),
		                               buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_LENGTH: {
		*(SQLULEN *)value_ptr = desc->records[rec_idx].sql_desc_length;
		return SQL_SUCCESS;
	}
	case SQL_DESC_LITERAL_PREFIX: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_literal_prefix,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_LITERAL_SUFFIX: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_literal_suffix,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_LOCAL_TYPE_NAME: {
		// is AD
		if (desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_local_type_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_NAME: {
		// is AD
		if (desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_name, reinterpret_cast<CHAR_TYPE *>(value_ptr),
		                               buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_NULLABLE: {
		// is AD
		if (!desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_nullable;
		return SQL_SUCCESS;
	}
	case SQL_DESC_NUM_PREC_RADIX: {
		*(SQLINTEGER *)value_ptr = desc->records[rec_idx].sql_desc_num_prec_radix;
		return SQL_SUCCESS;
	}
	case SQL_DESC_OCTET_LENGTH: {
		*(SQLLEN *)value_ptr = desc->records[rec_idx].sql_desc_octet_length;
		return SQL_SUCCESS;
	}
	case SQL_DESC_OCTET_LENGTH_PTR: {
		// is ID
		if (desc->IsID()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLLEN **)value_ptr = desc->records[rec_idx].sql_desc_octet_length_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_PARAMETER_TYPE: {
		// not IPD
		if (!desc->IsIPD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_parameter_type;
		return SQL_SUCCESS;
	}
	case SQL_DESC_PRECISION: {
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_precision;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ROWVER: {
		// is AD
		if (!desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_rowver;
		return SQL_SUCCESS;
	}
	case SQL_DESC_SCALE: {
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_scale;
		return SQL_SUCCESS;
	}
	case SQL_DESC_SCHEMA_NAME: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_schema_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_SEARCHABLE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_searchable;
		return SQL_SUCCESS;
	}
	case SQL_DESC_TABLE_NAME: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_table_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_TYPE: {
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_type;
		return SQL_SUCCESS;
	}
	case SQL_DESC_TYPE_NAME: {
		// is AD
		if (desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		duckdb::OdbcUtils::WriteString(desc->records[rec_idx].sql_desc_type_name,
		                               reinterpret_cast<CHAR_TYPE *>(value_ptr), buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESC_UNNAMED: {
		// is AD
		if (!desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_unnamed;
		return SQL_SUCCESS;
	}
	case SQL_DESC_UNSIGNED: {
		// is AD
		if (!desc->IsAD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_unsigned;
		return SQL_SUCCESS;
	}
	case SQL_DESC_UPDATABLE: {
		// not IRD
		if (!desc->IsIRD()) {
			return desc->ReturnInvalidFieldIdentifier(false);
		}
		*(SQLSMALLINT *)value_ptr = desc->records[rec_idx].sql_desc_updatable;
		return SQL_SUCCESS;
	}
	default:
		return desc->ReturnInvalidFieldIdentifier(false);
	}
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdescfield-function?view=sql-server-ver16">Docs</a>:
 */
SQLRETURN SQL_API SQLGetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return GetDescFieldInternal<SQLCHAR>(descriptor_handle, rec_number, field_identifier, value_ptr, buffer_length,
	                                     string_length_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdescfield-function?view=sql-server-ver16">Docs</a>:
 */
SQLRETURN SQL_API SQLGetDescFieldW(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                   SQLPOINTER value_ptr, SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return GetDescFieldInternal<SQLWCHAR>(descriptor_handle, rec_number, field_identifier, value_ptr, buffer_length,
	                                      string_length_ptr);
}

//===--------------------------------------------------------------------===//
// SQLSetDescField
//===--------------------------------------------------------------------===//

SQLRETURN SQL_API SQLSetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length) {
	duckdb::OdbcHandleDesc *desc = nullptr;
	SQLRETURN ret = ConvertDescriptor(descriptor_handle, desc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return desc->SetDescField(rec_number, field_identifier, value_ptr, buffer_length);
}

//===--------------------------------------------------------------------===//
// SQLGetDescRec
//===--------------------------------------------------------------------===//

template <typename CHAR_TYPE>
static SQLRETURN GetDescRecInternal(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, CHAR_TYPE *name,
                                    SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr, SQLSMALLINT *type_ptr,
                                    SQLSMALLINT *sub_type_ptr, SQLLEN *length_ptr, SQLSMALLINT *precision_ptr,
                                    SQLSMALLINT *scale_ptr, SQLSMALLINT *nullable_ptr) {
	duckdb::OdbcHandleDesc *desc = nullptr;
	SQLRETURN ret = ConvertDescriptor(descriptor_handle, desc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (rec_number < 1) {
		return SQL_ERROR; // TODO: per the docs record number 0 is the bookmark record, so we should support that
	}
	if (rec_number > desc->header.sql_desc_count) {
		return SQL_NO_DATA;
	}
	if (desc->IsIRD() && desc->stmt && desc->stmt->IsPrepared()) {
		return SQL_NO_DATA;
	}

	auto rec_idx = rec_number - 1;
	auto desc_record = desc->GetDescRecord(rec_idx);
	auto sql_type = desc_record->sql_desc_type;

	duckdb::OdbcUtils::WriteString(desc_record->sql_desc_name, name, buffer_length, string_length_ptr);
	if (type_ptr) {
		duckdb::Store<SQLSMALLINT>(sql_type, (duckdb::data_ptr_t)type_ptr);
	}
	if (sql_type == SQL_DATETIME || sql_type == SQL_INTERVAL) {
		if (sub_type_ptr) {
			duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_datetime_interval_code, (duckdb::data_ptr_t)sub_type_ptr);
		}
	}
	if (length_ptr) {
		duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_octet_length, (duckdb::data_ptr_t)length_ptr);
	}
	if (precision_ptr) {
		duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_precision, (duckdb::data_ptr_t)precision_ptr);
	}
	if (scale_ptr) {
		duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_scale, (duckdb::data_ptr_t)scale_ptr);
	}
	if (nullable_ptr) {
		duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_nullable, (duckdb::data_ptr_t)nullable_ptr);
	}
	return SQL_SUCCESS;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdescrec-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetDescRec(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLCHAR *name,
                                SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr, SQLSMALLINT *type_ptr,
                                SQLSMALLINT *sub_type_ptr, SQLLEN *length_ptr, SQLSMALLINT *precision_ptr,
                                SQLSMALLINT *scale_ptr, SQLSMALLINT *nullable_ptr) {
	return GetDescRecInternal(descriptor_handle, rec_number, name, buffer_length, string_length_ptr, type_ptr,
	                          sub_type_ptr, length_ptr, precision_ptr, scale_ptr, nullable_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdescrec-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetDescRecW(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLWCHAR *name,
                                 SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr, SQLSMALLINT *type_ptr,
                                 SQLSMALLINT *sub_type_ptr, SQLLEN *length_ptr, SQLSMALLINT *precision_ptr,
                                 SQLSMALLINT *scale_ptr, SQLSMALLINT *nullable_ptr) {
	return GetDescRecInternal(descriptor_handle, rec_number, name, buffer_length, string_length_ptr, type_ptr,
	                          sub_type_ptr, length_ptr, precision_ptr, scale_ptr, nullable_ptr);
}

//===--------------------------------------------------------------------===//
// SQLSetDescRec
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetdescrec-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLSetDescRec(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT type,
                                SQLSMALLINT sub_type, SQLLEN length, SQLSMALLINT precision, SQLSMALLINT scale,
                                SQLPOINTER data_ptr, SQLLEN *string_length_ptr, SQLLEN *indicator_ptr) {
	duckdb::OdbcHandleDesc *desc = nullptr;
	SQLRETURN ret = ConvertDescriptor(descriptor_handle, desc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (desc->IsIRD()) {
		return duckdb::SetDiagnosticRecord(desc, SQL_ERROR, "SQLSetDescRec",
		                                   "Cannot modify an implementation row descriptor",
		                                   duckdb::SQLStateType::ST_HY016, desc->dbc->GetDataSourceName());
	}
	if (rec_number <= 0) {
		return duckdb::SetDiagnosticRecord(desc, SQL_ERROR, "SQLSetDescRec", "Invalid descriptor index",
		                                   duckdb::SQLStateType::ST_07009, desc->dbc->GetDataSourceName());
	}
	if (rec_number > desc->header.sql_desc_count) {
		desc->AddMoreRecords(rec_number);
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_TYPE, &type, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	if (type == SQL_DATETIME || type == SQL_INTERVAL) {
		ret = desc->SetDescField(rec_number, SQL_DESC_DATETIME_INTERVAL_CODE, &sub_type, 0);
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		}
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_OCTET_LENGTH, &length, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_PRECISION, &precision, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_SCALE, &scale, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_DATA_PTR, &data_ptr, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_OCTET_LENGTH_PTR, &string_length_ptr, 0);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	ret = desc->SetDescField(rec_number, SQL_DESC_INDICATOR_PTR, &indicator_ptr, 0);
	return ret;
}

//===--------------------------------------------------------------------===//
// SQLCopyDesc
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcopydesc-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLCopyDesc(SQLHDESC source_desc_handle, SQLHDESC target_desc_handle) {
	if (source_desc_handle == nullptr || target_desc_handle == nullptr) {
		return SQL_INVALID_HANDLE;
	}
	auto source_desc = (OdbcHandleDesc *)source_desc_handle;
	auto target_desc = (OdbcHandleDesc *)target_desc_handle;
	// Check if source descriptor is an APD, ARD
	for (auto stmt : source_desc->dbc->vec_stmt_ref) {
		if (target_desc == stmt->row_desc->ird.get()) {
			return duckdb::SetDiagnosticRecord(target_desc, SQL_ERROR, "SQLCopyDesc",
			                                   "Cannot modify an implementation row descriptor.",
			                                   duckdb::SQLStateType::ST_HY016, target_desc->dbc->GetDataSourceName());
		}
		if (source_desc == stmt->row_desc->ird.get()) {
			if (!stmt->IsPrepared()) {
				return duckdb::SetDiagnosticRecord(
				    source_desc, SQL_ERROR, "SQLCopyDesc", "Associated statement is not prepared.",
				    duckdb::SQLStateType::ST_HY007, source_desc->dbc->GetDataSourceName());
			}
		}
	}

	// descriptors are associated with the same connection
	if (source_desc->dbc == target_desc->dbc) {
		// copy assignment operator
		target_desc = source_desc;
	} else {
		// descriptors are associated with the same enviroment
		if (source_desc->dbc->env == target_desc->dbc->env) {
			target_desc->CopyOnlyOdbcFields(*source_desc);
		} else {
			// descriptor connections belong to separate drivers
			target_desc->CopyFieldByField(*source_desc);
		}
	}
	return SQL_SUCCESS;
}
