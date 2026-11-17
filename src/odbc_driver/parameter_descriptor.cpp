#include "parameter_descriptor.hpp"

#include "handle_functions.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

using duckdb_odbc::idx_t;
using duckdb_odbc::OdbcHandleDesc;
using duckdb_odbc::ParameterDescriptor;
using duckdb_odbc::Value;
using duckdb_odbc::vector;

ParameterDescriptor::ParameterDescriptor(OdbcHandleStmt *stmt_ptr)
    : stmt(stmt_ptr), paramset_idx(0), cur_paramset_idx(0), cur_param_idx(0) {

	apd = make_uniq<OdbcHandleDesc>(stmt->dbc);
	ipd = make_uniq<OdbcHandleDesc>(stmt->dbc);

	cur_apd = apd.get();
}

OdbcHandleDesc *ParameterDescriptor::GetIPD() {
	return ipd.get();
}

OdbcHandleDesc *ParameterDescriptor::GetAPD() {
	return cur_apd;
}

void ParameterDescriptor::Clear() {
	ipd->records.clear();
	cur_apd->records.clear();
	Reset();
}

void ParameterDescriptor::SetCurrentAPD(OdbcHandleDesc *new_apd) {
	cur_apd = new_apd;
	cur_apd->header.sql_desc_alloc_type = SQL_DESC_ALLOC_USER;
}

void ParameterDescriptor::Reset() {
	pool_allocated_ptr.clear();
	ipd->header.sql_desc_count = 0;
	cur_apd->header.sql_desc_count = 0;
	paramset_idx = 0;
	cur_paramset_idx = 0;
	cur_param_idx = 0;
}

void ParameterDescriptor::ResetParams(SQLSMALLINT count) {
	pool_allocated_ptr.clear();

	ipd->records.resize(count);
	ipd->header.sql_desc_count = count;

	cur_apd->records.resize(count);
	cur_apd->header.sql_desc_count = count;
}

void ParameterDescriptor::ResetCurrentAPD() {
	cur_apd = apd.get();
}

SQLRETURN ParameterDescriptor::GetParamValues() {
	if (ipd->records.empty()) {
		values.clear();
		return SQL_SUCCESS;
	}
	D_ASSERT((SQLULEN)paramset_idx < cur_apd->header.sql_desc_array_size);
	// Fill values
	for (idx_t rec_idx = 0; rec_idx < ipd->records.size(); ++rec_idx) {
		auto ret = SetValue(rec_idx);
		if (ipd->header.sql_desc_array_status_ptr) {
			ipd->header.sql_desc_array_status_ptr[paramset_idx] = ret;
		}
		if (ret == SQL_NEED_DATA) {
			return SQL_NEED_DATA;
		}
		if (ret != SQL_PARAM_SUCCESS) {
			duckdb_odbc::SetDiagnosticRecord(this->stmt, SQL_SUCCESS_WITH_INFO, "GetParamValues",
			                                 "Failed to set parameter value", SQLStateType::ST_01000,
			                                 this->stmt->dbc->GetDataSourceName());
			if (!ipd->header.sql_desc_array_status_ptr) {
				return SQL_ERROR;
			}
		}
		if (rec_idx >= values.size()) {
			// the parameter failed to convert: bind it as NULL
			auto &factory = stmt->dbc->conn->GetFactory();
			SetValue(Value::CreateNull(factory, duckdb_odbc::CreateType(factory, LogicalTypeId::SQLNULL)), rec_idx);
		}
	}
	if (values.size() > ipd->records.size()) {
		values.erase(values.begin() + static_cast<std::ptrdiff_t>(ipd->records.size()), values.end());
	}
	return SetParamIndex();
}

void ParameterDescriptor::SetParamProcessedPtr(SQLULEN *value_ptr) {
	ipd->header.sql_desc_rows_processed_ptr = value_ptr;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr = 0;
	}
}

SQLULEN *ParameterDescriptor::GetParamProcessedPtr() {
	return ipd->header.sql_desc_rows_processed_ptr;
}

void ParameterDescriptor::SetArrayStatusPtr(SQLUSMALLINT *value_ptr) {
	ipd->header.sql_desc_array_status_ptr = value_ptr;
}

SQLUSMALLINT *ParameterDescriptor::SetArrayStatusPtr() {
	return ipd->header.sql_desc_array_status_ptr;
}

void ParameterDescriptor::SetBindOffesetPtr(SQLLEN *value_ptr) {
	stmt->param_desc->apd->header.sql_desc_bind_offset_ptr = (SQLLEN *)value_ptr;
}

SQLLEN *ParameterDescriptor::GetBindOffesetPtr() {
	return stmt->param_desc->apd->header.sql_desc_bind_offset_ptr;
}

SQLRETURN ParameterDescriptor::GetNextParam(SQLPOINTER *param) {
	if ((SQLSMALLINT)cur_param_idx >= cur_apd->header.sql_desc_count) {
		return SQL_NO_DATA;
	}

	auto apd_record = &cur_apd->records[cur_param_idx];
	*param = GetSQLDescDataPtr(*apd_record);
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr += 1;
	}
	auto sql_desc_ind_ptr = GetSQLDescIndicatorPtr(*apd_record, cur_paramset_idx);
	if (*sql_desc_ind_ptr == SQL_DATA_AT_EXEC) {
		return SQL_NEED_DATA;
	}
	return SQL_SUCCESS;
}

SQLRETURN ParameterDescriptor::PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	if ((SQLSMALLINT)cur_param_idx >= cur_apd->header.sql_desc_count) {
		return SQL_ERROR;
	}

	auto apd_record = &cur_apd->records[cur_param_idx];
	if (apd_record->sql_desc_type == SQL_C_CHAR || apd_record->sql_desc_type == SQL_C_BINARY) {
		auto ipd_record = &ipd->records[cur_param_idx];
		return PutCharData(*apd_record, *ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	// filling the current param, go to the next one
	++cur_param_idx;
	if (str_len_or_ind_ptr == SQL_NULL_DATA) {
		SetSQLDescIndicatorPtr(*apd_record, SQL_NULL_DATA);
		return SQL_SUCCESS;
	}

	SetSQLDescDataPtr(*apd_record, data_ptr);
	return SQL_SUCCESS;
}

bool ParameterDescriptor::HasParamSetToProcess() {
	return (paramset_idx < cur_apd->header.sql_desc_array_size && !ipd->records.empty());
}

SQLRETURN ParameterDescriptor::PutCharData(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                           SQLLEN str_len_or_ind_ptr) {
	if (cur_apd->header.sql_desc_array_size == 1) {
		return FillParamCharDataBuffer(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	return FillCurParamCharSet(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
}

SQLRETURN ParameterDescriptor::FillParamCharDataBuffer(DescRecord &apd_record, DescRecord &ipd_record,
                                                       SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	size_t offset = 0;
	auto sql_ind_ptr = GetSQLDescIndicatorPtr(apd_record);
	if (*sql_ind_ptr == SQL_DATA_AT_EXEC) {
		pool_allocated_ptr.emplace_back(duckdb_odbc::make_unsafe_uniq_array<char>(ipd_record.sql_desc_length));
		SetSQLDescDataPtr(apd_record, pool_allocated_ptr.back().get());
		*sql_ind_ptr = 0;
	} else {
		// appending more data into it
		offset = *sql_ind_ptr;
	}

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)ipd_record.sql_desc_length - offset)) {
		size = (size_t)ipd_record.sql_desc_length - offset;
		// filled the current param buffer data, go to the next one
		++cur_param_idx;
	}

	memcpy((char *)GetSQLDescDataPtr(apd_record) + offset, (char *)data_ptr, size);
	*sql_ind_ptr += size;

	if (ipd->header.sql_desc_array_status_ptr) {
		ipd->header.sql_desc_array_status_ptr[cur_param_idx] = SQL_PARAM_SUCCESS;
	}

	return SQL_PARAM_SUCCESS;
}

SQLRETURN ParameterDescriptor::FillCurParamCharSet(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                                   SQLLEN str_len_or_ind_ptr) {
	auto len_ptr = GetSQLDescIndicatorPtr(apd_record, cur_paramset_idx);
	auto col_size = (size_t)ipd_record.sql_desc_length;

	if (*len_ptr == SQL_DATA_AT_EXEC && pool_allocated_ptr.empty()) {
		auto alloc_size = col_size * cur_apd->header.sql_desc_array_size;
		pool_allocated_ptr.emplace_back(duckdb_odbc::make_unsafe_uniq_array<char>(alloc_size));
		SetSQLDescDataPtr(apd_record, pool_allocated_ptr.back().get());
	}

	auto value_ptr = (char *)GetSQLDescDataPtr(apd_record) + (cur_paramset_idx * col_size);

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)ipd_record.sql_desc_length)) {
		size = col_size;
	}

	memcpy(value_ptr, (const char *)data_ptr, size);
	*len_ptr = size;

	if (ipd->header.sql_desc_array_status_ptr) {
		ipd->header.sql_desc_array_status_ptr[cur_paramset_idx] = SQL_PARAM_SUCCESS;
	}

	++cur_paramset_idx;
	if (cur_paramset_idx >= cur_apd->header.sql_desc_array_size) {
		// got to the next param set
		cur_paramset_idx = 0;
		++cur_param_idx;
	}

	return SQL_PARAM_SUCCESS;
}

// The widest DECIMAL that is stored as a 64-bit integer, and the widest DECIMAL.
static constexpr int MAX_WIDTH_INT64 = 18;
static constexpr int MAX_WIDTH_DECIMAL = 38;

// A column-wise parameter array holds fixed-length C values back to back, so the value of
// parameter set `set_idx` is `set_idx` elements into the bound buffer.
template <class T>
static T LoadFromSet(duckdb_odbc::const_data_ptr_t dataptr, idx_t set_idx) {
	return duckdb_odbc::Load<T>(dataptr + set_idx * sizeof(T));
}

SQLRETURN ParameterDescriptor::SetValue(idx_t rec_idx) {
	try {
		return SetValueInternal(rec_idx);
	} catch (std::exception &ex) {
		duckdb_odbc::SetDiagnosticRecord(stmt, SQL_SUCCESS_WITH_INFO, "GetParamValues",
		                                 duckdb_odbc::RawErrorMessage(ex), SQLStateType::ST_22018,
		                                 stmt->dbc->GetDataSourceName());
		return SQL_PARAM_ERROR;
	}
}

static duckdb_odbc::cxx::int128_t ToInt128(int64_t value) {
	return duckdb_odbc::cxx::int128_t {static_cast<uint64_t>(value), value < 0 ? -1 : 0};
}

SQLRETURN ParameterDescriptor::SetValueInternal(idx_t rec_idx) {
	auto &factory = stmt->dbc->conn->GetFactory();
	auto val_idx = paramset_idx;
	auto apd_record = &cur_apd->records[rec_idx];
	auto sql_data_ptr = GetSQLDescDataPtr(*apd_record);
	auto sql_ind_ptr = GetSQLDescIndicatorPtr(*apd_record);

	if (sql_data_ptr == nullptr && sql_ind_ptr == nullptr) {
		return SQL_ERROR;
	}

	auto sql_ind_ptr_val_set = GetSQLDescIndicatorPtr(*apd_record, val_idx);
	if (sql_data_ptr == nullptr || sql_ind_ptr == nullptr || *sql_ind_ptr_val_set == SQL_NULL_DATA) {
		SetValue(Value::CreateNull(factory, duckdb_odbc::CreateType(factory, LogicalTypeId::SQLNULL)), rec_idx);
		return SQL_SUCCESS;
	}

	if (*sql_ind_ptr_val_set == SQL_DATA_AT_EXEC ||
	    (SQLULEN)*sql_ind_ptr_val_set == SQL_LEN_DATA_AT_EXEC(ipd->records[rec_idx].sql_desc_length)) {
		return SQL_NEED_DATA;
	}

	duckdb_odbc::unique_ptr<Value> value;
	duckdb_odbc::const_data_ptr_t dataptr = (duckdb_odbc::const_data_ptr_t)sql_data_ptr;

	// Power BI / .NET System.Data.Odbc bind a string buffer (SQL_C_CHAR/SQL_C_WCHAR)
	// against non-string SQL types - a DECIMAL slicer arrives as "42.00", a DATE filter
	// as "2024-01-01". Decode by the C (buffer) type and hand DuckDB a string to cast,
	// rather than reading the buffer as the SQL type's binary layout (which yields
	// garbage). Binary targets are excluded - their buffers are not text.
	const auto sql_type = ipd->records[rec_idx].sql_desc_type;
	const auto c_type = apd_record->sql_desc_type;
	const bool binary_target = sql_type == SQL_BINARY || sql_type == SQL_VARBINARY || sql_type == SQL_LONGVARBINARY;
	if (!binary_target && (c_type == SQL_C_CHAR || c_type == SQL_C_WCHAR)) {
		auto buff_size = duckdb_odbc::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                       apd->records[rec_idx].sql_desc_octet_length);
		if (c_type == SQL_C_WCHAR) {
			auto utf16_data = (SQLWCHAR *)sql_data_ptr + (val_idx * buff_size);
			if (*sql_ind_ptr_val_set == SQL_NTS) {
				*sql_ind_ptr_val_set =
				    static_cast<SQLLEN>(duckdb_odbc::widechar::utf16_length(utf16_data) * sizeof(SQLWCHAR));
			}
			auto utf16_len = static_cast<size_t>(*sql_ind_ptr_val_set / sizeof(SQLWCHAR));
			auto utf8_vec = duckdb_odbc::widechar::utf16_to_utf8_lenient(utf16_data, utf16_len);
			value = duckdb_odbc::make_uniq<Value>(duckdb_odbc::CreateVarchar(
			    factory, std::string(reinterpret_cast<char *>(utf8_vec.data()), utf8_vec.size())));
		} else {
			auto str_data = (char *)sql_data_ptr + (val_idx * buff_size);
			if (*sql_ind_ptr_val_set == SQL_NTS) {
				*sql_ind_ptr_val_set = strlen(str_data);
			}
			value = duckdb_odbc::make_uniq<Value>(
			    duckdb_odbc::CreateVarchar(factory, duckdb_odbc::OdbcUtils::ConvertSQLCHARToString(
			                                            reinterpret_cast<SQLCHAR *>(str_data), *sql_ind_ptr_val_set)));
		}
		SetValue(std::move(*value), rec_idx);
		return SQL_PARAM_SUCCESS;
	}

	switch (sql_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR: {
		auto buff_size = duckdb_odbc::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                       apd->records[rec_idx].sql_desc_octet_length);
		auto str_data = (char *)sql_data_ptr + (val_idx * buff_size);
		if (*sql_ind_ptr_val_set == SQL_NTS) {
			*sql_ind_ptr_val_set = strlen(str_data);
		}
		auto str_len = *sql_ind_ptr_val_set;
		value = duckdb_odbc::make_uniq<Value>(duckdb_odbc::CreateVarchar(
		    factory, duckdb_odbc::OdbcUtils::ConvertSQLCHARToString(reinterpret_cast<SQLCHAR *>(str_data), str_len)));
		break;
	}
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR: {
		auto buff_size = duckdb_odbc::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                       apd->records[rec_idx].sql_desc_octet_length);
		auto utf16_data = (SQLWCHAR *)sql_data_ptr + (val_idx * buff_size);
		if (*sql_ind_ptr_val_set == SQL_NTS) {
			*sql_ind_ptr_val_set =
			    static_cast<SQLLEN>(duckdb_odbc::widechar::utf16_length(utf16_data) * sizeof(SQLWCHAR));
		}
		auto utf16_len = static_cast<size_t>(*sql_ind_ptr_val_set / sizeof(SQLWCHAR));
		auto utf8_vec = duckdb_odbc::widechar::utf16_to_utf8_lenient(utf16_data, utf16_len);
		auto utf8_str = std::string(reinterpret_cast<char *>(utf8_vec.data()), utf8_vec.size());
		value = duckdb_odbc::make_uniq<Value>(duckdb_odbc::CreateVarchar(factory, utf8_str));
		break;
	}
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY: {
		auto buff_size = duckdb_odbc::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                       apd->records[rec_idx].sql_desc_octet_length);
		auto blob_data = (duckdb_odbc::const_data_ptr_t)sql_data_ptr + (val_idx * buff_size);
		auto blob_len = *sql_ind_ptr_val_set;
		value = duckdb_odbc::make_uniq<Value>(
		    Value::Create(factory, duckdb_odbc::cxx::blob_t(reinterpret_cast<const char *>(blob_data),
		                                                    static_cast<uint32_t>(blob_len))));
		break;
	}
	case SQL_TINYINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_UTINYINT) {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<uint8_t>(dataptr, val_idx)));
		} else {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<int8_t>(dataptr, val_idx)));
		}
		break;
	case SQL_SMALLINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_USHORT) {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<uint16_t>(dataptr, val_idx)));
		} else {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<int16_t>(dataptr, val_idx)));
		}
		break;
	case SQL_INTEGER:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_ULONG) {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<uint32_t>(dataptr, val_idx)));
		} else {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<int32_t>(dataptr, val_idx)));
		}
		break;
	case SQL_BIGINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_UBIGINT) {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<uint64_t>(dataptr, val_idx)));
		} else {
			value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<int64_t>(dataptr, val_idx)));
		}
		break;
	case SQL_FLOAT:
		value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<float>(dataptr, val_idx)));
		break;
	case SQL_DOUBLE:
		value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, LoadFromSet<double>(dataptr, val_idx)));
		break;
	case SQL_NUMERIC: {
		auto numeric = (SQL_NUMERIC_STRUCT *)sql_data_ptr + val_idx;
		dataptr = numeric->val;

		auto precision = ipd->records[rec_idx].sql_desc_precision;
		auto scale = ipd->records[rec_idx].sql_desc_scale;
		if (ValidateNumeric(precision, scale) == SQL_ERROR) {
			return SQL_ERROR;
		}
		duckdb_odbc::cxx::int128_t dec_value;
		if (precision <= MAX_WIDTH_INT64) {
			int64_t dec_val = duckdb_odbc::Load<int64_t>(dataptr);
			// Handle sign: 0 = negative, 1 = positive
			if (numeric->sign == 0) {
				dec_val = -dec_val;
			}
			dec_value = ToInt128(dec_val);
		} else {
			memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
			memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
			// Handle sign: 0 = negative, 1 = positive
			if (numeric->sign == 0) {
				// two's complement negation
				dec_value.lower = ~dec_value.lower + 1;
				dec_value.upper =
				    static_cast<int64_t>(~static_cast<uint64_t>(dec_value.upper) + (dec_value.lower == 0 ? 1 : 0));
			}
		}
		value = duckdb_odbc::make_uniq<Value>(
		    Value::CreateDecimal(factory, dec_value, static_cast<uint8_t>(precision), static_cast<uint8_t>(scale)));
		break;
	}
	case SQL_TYPE_TIMESTAMP: {
		auto timestamp_struct = LoadFromSet<SQL_TIMESTAMP_STRUCT>(dataptr, val_idx);
		auto date = duckdb_odbc::Date::FromDate(timestamp_struct.year, timestamp_struct.month, timestamp_struct.day);
		auto time = duckdb_odbc::Time::FromTime(timestamp_struct.hour, timestamp_struct.minute, timestamp_struct.second,
		                                        static_cast<int32_t>(timestamp_struct.fraction / 1000));
		value = duckdb_odbc::make_uniq<Value>(Value::Create(factory, duckdb_odbc::Timestamp::FromDatetime(date, time)));
		break;
	}
	case SQL_TYPE_DATE: {
		auto date_struct = LoadFromSet<SQL_DATE_STRUCT>(dataptr, val_idx);
		value = duckdb_odbc::make_uniq<Value>(
		    Value::Create(factory, duckdb_odbc::Date::FromDate(date_struct.year, date_struct.month, date_struct.day)));
		break;
	}
	case SQL_TYPE_TIME: {
		auto time_struct = LoadFromSet<SQL_TIME_STRUCT>(dataptr, val_idx);
		value = duckdb_odbc::make_uniq<Value>(Value::Create(
		    factory, duckdb_odbc::Time::FromTime(time_struct.hour, time_struct.minute, time_struct.second, 0)));
		break;
	}

	// TODO more types
	default:
		// TODO error message?
		return SQL_PARAM_ERROR;
	}

	SetValue(std::move(*value), rec_idx);
	return SQL_PARAM_SUCCESS;
}

void ParameterDescriptor::SetValue(Value value, idx_t val_idx) {
	if (val_idx >= values.size()) {
		// the parameters are set in order
		D_ASSERT(val_idx == values.size());
		values.emplace_back(std::move(value));
		return;
	}
	// replacing value, i.e., reusing parameter in a prepared stmt
	values[val_idx] = std::move(value);
}

SQLRETURN ParameterDescriptor::SetParamIndex() {
	++paramset_idx;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr = paramset_idx;
	}
	if (paramset_idx == cur_apd->header.sql_desc_array_size) {
		return SQL_SUCCESS;
	}
	return SQL_STILL_EXECUTING;
}

SQLRETURN ParameterDescriptor::ValidateNumeric(int precision, int scale) {
	if (precision < 1 || precision > MAX_WIDTH_DECIMAL || scale < 0 || scale > MAX_WIDTH_DECIMAL || scale > precision) {
		// TODO we should use SQLGetDiagField to register the error message
		auto msg = StringUtil::Format("Numeric precision %d and scale %d not supported", precision, scale);
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

SQLPOINTER ParameterDescriptor::GetSQLDescDataPtr(DescRecord &apd_record) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return (uint8_t *)apd_record.sql_desc_data_ptr + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_data_ptr;
}

void ParameterDescriptor::SetSQLDescDataPtr(DescRecord &apd_record, SQLPOINTER data_ptr) {
	auto sql_data_ptr = &(apd_record.sql_desc_data_ptr);
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_data_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	*sql_data_ptr = data_ptr;
}

SQLLEN *ParameterDescriptor::GetSQLDescIndicatorPtr(DescRecord &apd_record, idx_t set_idx) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return apd_record.sql_desc_indicator_ptr + set_idx + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_indicator_ptr + set_idx;
}

void ParameterDescriptor::SetSQLDescIndicatorPtr(DescRecord &apd_record, SQLLEN value) {
	auto sql_ind_ptr = apd_record.sql_desc_indicator_ptr;
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_ind_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	*sql_ind_ptr = value;
}

SQLLEN *ParameterDescriptor::GetSQLDescOctetLengthPtr(DescRecord &apd_record, idx_t set_idx) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return apd_record.sql_desc_octet_length_ptr + set_idx + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_octet_length_ptr + set_idx;
}
