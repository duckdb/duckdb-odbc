#include "descriptor.hpp"
#include "api_info.hpp"
#include "odbc_interval.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "duckdb/common/vector.hpp"
#include "handle_functions.hpp"

using duckdb::ApiInfo;
using duckdb::DescHeader;
using duckdb::DescRecord;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcInterval;
using duckdb::vector;

//! OdbcHandleDesc functions ********************************

OdbcHandleDesc::OdbcHandleDesc(const OdbcHandleDesc &other) : OdbcHandle(other) {
	// calling copy assigment operator
	*this = other;
}

OdbcHandleDesc &OdbcHandleDesc::operator=(const OdbcHandleDesc &other) {
	if (&other != this) {
		OdbcHandle::operator=(other);
		dbc = other.dbc;
		CopyOnlyOdbcFields(other);
	}
	return *this;
}

void OdbcHandleDesc::CopyOnlyOdbcFields(const OdbcHandleDesc &other) {
	header = other.header;
	std::copy(other.records.begin(), other.records.end(), std::back_inserter(records));
}

void OdbcHandleDesc::CopyFieldByField(const OdbcHandleDesc &other) {
	// TODO
}

duckdb::DescRecord *OdbcHandleDesc::GetDescRecord(duckdb::idx_t param_idx) {
	if (param_idx >= records.size()) {
		records.resize(param_idx + 1);
		header.sql_desc_count = static_cast<SQLSMALLINT>(records.size());
	}
	return &records[param_idx];
}

SQLRETURN OdbcHandleDesc::ReturnInvalidFieldIdentifier(bool is_readonly) {
	std::string msg = "Invalid descriptor field identifier";
	if (is_readonly) {
		msg += " (read-only field)";
	}
	return duckdb::SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", msg, duckdb::SQLStateType::ST_HY092,
	                                   dbc->GetDataSourceName());
}

SQLRETURN OdbcHandleDesc::SetDescField(SQLSMALLINT rec_number, SQLSMALLINT field_identifier, SQLPOINTER value_ptr,
                                       SQLINTEGER buffer_length) {
	// descriptor header fields
	switch (field_identifier) {
	case SQL_DESC_ALLOC_TYPE: {
		// TODO: Check if this is the correct way to handle this
		return ReturnInvalidFieldIdentifier(true);
	}
	case SQL_DESC_ARRAY_SIZE: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(false);
		}
		auto size = *(SQLULEN *)value_ptr;
		if (size <= 0) {
			return SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", "Invalid attribute/option identifier.",
			                           duckdb::SQLStateType::ST_HY092, this->dbc->GetDataSourceName());
		}
		header.sql_desc_array_size = size;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ARRAY_STATUS_PTR: {
		header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_BIND_OFFSET_PTR: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(false);
		}
		header.sql_desc_bind_offset_ptr = (SQLLEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_BIND_TYPE: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(false);
		}
		header.sql_desc_bind_type = *(SQLINTEGER *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_COUNT: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(false);
		}
		header.sql_desc_count = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_ROWS_PROCESSED_PTR: {
		if (IsAD()) {
			return ReturnInvalidFieldIdentifier(false);
		}
		if (*(SQLULEN *)value_ptr < 0) {
			return SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", "Invalid descriptor index.",
			                           duckdb::SQLStateType::ST_07009, this->dbc->GetDataSourceName());
		}
		header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
		return SQL_SUCCESS;
	}
	default:
		break;
	}

	if (rec_number <= 0) {
		return SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", "Invalid descriptor index.",
		                           duckdb::SQLStateType::ST_07009, this->dbc->GetDataSourceName());
	}
	auto rec_idx = rec_number - 1;
	auto desc_record = GetDescRecord(rec_idx);

	// checking descriptor record fields
	switch (field_identifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LABEL:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_ROWVER:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE_NAME:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE: {
		return ReturnInvalidFieldIdentifier(true);
	}
	case SQL_DESC_CONCISE_TYPE: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_concise_type = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATA_PTR: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_data_ptr = value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATETIME_INTERVAL_CODE: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		if (desc_record->SetSqlDescType(desc_record->sql_desc_type)) {
			return SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", "Inconsistent descriptor information.",
			                           duckdb::SQLStateType::ST_HY021, this->dbc->GetDataSourceName());
		}
		return SQL_SUCCESS;
	}
	case SQL_DESC_DATETIME_INTERVAL_PRECISION: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_datetime_interval_precision = *(SQLINTEGER *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_INDICATOR_PTR: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_indicator_ptr = (SQLLEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_LENGTH: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_length = *(SQLULEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_NAME: {
		if (!IsIPD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_name =
		    duckdb::OdbcUtils::ConvertSQLCHARToString(static_cast<SQLCHAR *>(value_ptr), buffer_length);
		return SQL_SUCCESS;
	}
	case SQL_DESC_NUM_PREC_RADIX: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_num_prec_radix = *(SQLINTEGER *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_OCTET_LENGTH: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_octet_length = *(SQLLEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_OCTET_LENGTH_PTR: {
		if (IsID()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_octet_length_ptr = (SQLLEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_PARAMETER_TYPE: {
		if (!IsIPD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_parameter_type = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_PRECISION: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_precision = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_SCALE: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_scale = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_DESC_TYPE: {
		if (IsIRD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		auto sql_type = *(SQLSMALLINT *)value_ptr;
		if (desc_record->SetSqlDescType(sql_type)) {
			return SetDiagnosticRecord(this, SQL_ERROR, "SQLSetDescField", "Inconsistent descriptor information.",
			                           duckdb::SQLStateType::ST_HY021, this->dbc->GetDataSourceName());
		}
		return SQL_SUCCESS;
	}
	case SQL_DESC_UNNAMED: {
		if (!IsIPD()) {
			return ReturnInvalidFieldIdentifier(true);
		}
		desc_record->sql_desc_unnamed = *(SQLSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	default:
		return ReturnInvalidFieldIdentifier(false);
	}
}

void OdbcHandleDesc::Clear() {
	Reset();
}

void OdbcHandleDesc::Reset() {
	records.clear();
}

bool OdbcHandleDesc::IsID() {
	return stmt != nullptr;
}

bool OdbcHandleDesc::IsAD() {
	return stmt == nullptr;
}

bool OdbcHandleDesc::IsIRD() {
	return (IsID() && stmt->row_desc->GetIRD() == this);
}

bool OdbcHandleDesc::IsIPD() {
	return (IsID() && stmt->param_desc->GetIPD() == this);
}

void OdbcHandleDesc::AddMoreRecords(SQLSMALLINT new_size) {
	records.resize(new_size);
	header.sql_desc_count = new_size;
}

//! DescRecord functions ******************************************************

// Copy constructor
DescRecord::DescRecord(const DescRecord &other) {
	sql_desc_base_column_name = other.sql_desc_base_column_name;
	sql_desc_base_table_name = other.sql_desc_base_table_name;
	sql_desc_case_sensitive = other.sql_desc_case_sensitive;
	sql_desc_catalog_name = other.sql_desc_catalog_name;
	sql_desc_concise_type = other.sql_desc_concise_type;
	sql_desc_data_ptr = other.sql_desc_data_ptr;
	sql_desc_datetime_interval_code = other.sql_desc_datetime_interval_code;
	sql_desc_datetime_interval_precision = other.sql_desc_datetime_interval_precision;
	sql_desc_display_size = other.sql_desc_display_size;
	sql_desc_fixed_prec_scale = other.sql_desc_fixed_prec_scale;
	sql_desc_indicator_ptr = other.sql_desc_indicator_ptr;
	sql_desc_label = other.sql_desc_label;
	sql_desc_length = other.sql_desc_length;
	sql_desc_literal_prefix = other.sql_desc_literal_prefix;
	sql_desc_literal_suffix = other.sql_desc_literal_suffix;
	sql_desc_local_type_name = other.sql_desc_local_type_name;
	sql_desc_name = other.sql_desc_name;
	sql_desc_nullable = other.sql_desc_nullable;
	sql_desc_num_prec_radix = other.sql_desc_num_prec_radix;
	sql_desc_octet_length = other.sql_desc_octet_length;
	sql_desc_octet_length_ptr = other.sql_desc_octet_length_ptr;
	sql_desc_parameter_type = other.sql_desc_parameter_type;
	sql_desc_precision = other.sql_desc_precision;
	sql_desc_rowver = other.sql_desc_rowver;
	sql_desc_scale = other.sql_desc_scale;
	sql_desc_schema_name = other.sql_desc_schema_name;
	sql_desc_searchable = other.sql_desc_searchable;
	sql_desc_table_name = other.sql_desc_table_name;
	sql_desc_type = other.sql_desc_type;
	sql_desc_type_name = other.sql_desc_type_name;
	sql_desc_unnamed = other.sql_desc_unnamed;
	sql_desc_unsigned = other.sql_desc_unsigned;
	sql_desc_updatable = other.sql_desc_updatable;
}

SQLRETURN DescRecord::SetSqlDataType(SQLSMALLINT type) {
	sql_desc_type = sql_desc_concise_type = type;
	if (OdbcInterval::IsIntervalType(type)) {
		sql_desc_type = SQL_INTERVAL;
		sql_desc_concise_type = type;
		auto interval_code = OdbcInterval::GetIntervalCode(type);
		if (interval_code == SQL_ERROR) {
			return SQL_ERROR; // handled
		}
		sql_desc_datetime_interval_code = interval_code;
	}

	return SQL_SUCCESS;
}

SQLRETURN DescRecord::SetSqlDescType(SQLSMALLINT type) {
	vector<duckdb::OdbcTypeInfo> vec_typeinfo;
	ApiInfo::FindDataType(type, vec_typeinfo);
	if (vec_typeinfo.empty()) {
		return SQL_ERROR; // handled
	}
	auto type_info = vec_typeinfo.front();
	// for consistency check set all other fields according to the first returned OdbcTypeInfo
	SetSqlDataType(type_info.sql_data_type);

	sql_desc_datetime_interval_code = type_info.sql_datetime_sub;
	sql_desc_precision = type_info.column_size;
	sql_desc_datetime_interval_precision = type_info.interval_precision;
	sql_desc_length = type_info.column_size;
	sql_desc_literal_prefix = type_info.literal_prefix;
	sql_desc_literal_suffix = type_info.literal_suffix;
	sql_desc_local_type_name = !strcmp(type_info.local_type_name, "NULL") ? "" : type_info.local_type_name;
	sql_desc_nullable = type_info.nullable;
	sql_desc_case_sensitive = type_info.case_sensitive;
	sql_desc_scale = type_info.minimum_scale;
	sql_desc_searchable = type_info.searchable;
	sql_desc_fixed_prec_scale = type_info.fixed_prec_scale;
	sql_desc_num_prec_radix = type_info.num_prec_radix == -1 ? 0 : type_info.num_prec_radix;
	sql_desc_unsigned = type_info.unsigned_attribute;
	return SQL_SUCCESS;
}

void DescRecord::SetDescUnsignedField(const duckdb::LogicalType &type) {
	// TRUE if the column is unsigned, or non-numeric or
	// UTINYINT, USMALLINT, UINTEGER, UBIGINT
	if (sql_desc_unsigned == -1 || (type.id() >= LogicalTypeId::UTINYINT && type.id() <= LogicalTypeId::UBIGINT)) {
		sql_desc_unsigned = SQL_TRUE;
	}
}
//! DescHeader functions ******************************************************
DescHeader::DescHeader() {
	Reset();
}

DescHeader::DescHeader(const DescHeader &other) {
	//	sql_desc_alloc_type = other.sql_desc_alloc_type; this can't be copied
	sql_desc_array_size = other.sql_desc_array_size;
	sql_desc_array_status_ptr = other.sql_desc_array_status_ptr;
	sql_desc_bind_offset_ptr = other.sql_desc_bind_offset_ptr;
	sql_desc_bind_type = other.sql_desc_bind_type;
	sql_desc_count = other.sql_desc_count;
	sql_desc_rows_processed_ptr = other.sql_desc_rows_processed_ptr;
}

void DescHeader::Reset() {
	sql_desc_array_size = 1;
	sql_desc_array_status_ptr = nullptr;
	sql_desc_count = 0;
	sql_desc_rows_processed_ptr = nullptr;
}
