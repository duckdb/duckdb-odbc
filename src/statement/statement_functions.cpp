#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "odbc_interval.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "descriptor.hpp"
#include "parameter_descriptor.hpp"
#include "widechar.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

using duckdb::date_t;
using duckdb::Decimal;
using duckdb::DecimalType;
using duckdb::dtime_t;
using duckdb::EnumUtil;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using duckdb::OdbcDiagnostic;
using duckdb::OdbcInterval;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;
using duckdb::Store;
using duckdb::string;
using duckdb::string_t;
using duckdb::Timestamp;
using duckdb::timestamp_t;
using duckdb::vector;

string duckdb::GetQueryAsString(duckdb::OdbcHandleStmt *hstmt, SQLCHAR *statement_text, SQLINTEGER text_length) {
	if (hstmt->stmt) {
		hstmt->stmt.reset();
	}

	if (hstmt->res) {
		hstmt->res.reset();
	}

	hstmt->odbc_fetcher->ClearChunks();
	return OdbcUtils::ReadString(statement_text, text_length);
}

SQLRETURN duckdb::FinalizeStmt(duckdb::OdbcHandleStmt *hstmt) {
	if (hstmt->stmt->HasError()) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "PrepareStmt", hstmt->stmt->error.Message(),
		                           SQLStateType::ST_42000, hstmt->dbc->GetDataSourceName());
	}

	D_ASSERT(hstmt->stmt->data);

	SQLRETURN ret = SQL_SUCCESS;

	// Prepare call may be unable to bind the query when parameter types cannot be determined,
	// for example: 'SELECT ?'.
	// In this case the plan is not created at all and resulting columns (their types and names)
	// returned with a partially-filled state, so we need to clear them. They are going to be re-filled after
	// each execution and may be different depending on input parameters supplied for execution.
	if (!hstmt->stmt->GetStatementProperties().bound_all_parameters) {
		hstmt->stmt->data->types.clear();
		hstmt->stmt->data->names.clear();
		ret = SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "PrepareStmt", "Not all parameters are bound",
		                          SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
	}

	// named_param_map is created on successfull parse stage, does not need to be corrected on rebind
	hstmt->param_desc->ResetParams(static_cast<SQLSMALLINT>(hstmt->stmt->named_param_map.size()));

	// Bound columns and IRD records depend on resulting columns. When
	// bound_all_parameters=false, we don't have resulting columns defined, so
	// bound columns and IRD records are kept empty and are re-filled after
	// every execution.
	hstmt->bound_cols.resize(hstmt->stmt->ColumnCount());
	hstmt->FillIRD();

	return ret;
}

//! Execute stmt in a batch manner while there is a parameter set to process,
//! the stmt is executed multiple times when there is a bound array of parameters in INSERT and UPDATE statements
SQLRETURN duckdb::BatchExecuteStmt(duckdb::OdbcHandleStmt *hstmt) {
	SQLRETURN ret = SQL_SUCCESS;
	do {
		ret = SingleExecuteStmt(hstmt);
	} while (ret == SQL_STILL_EXECUTING);

	// Early exit on error
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	// now, fetching the first chunk to verify constant folding (See: PR #2462 and issue #2452)
	auto fetch_ret = hstmt->odbc_fetcher->FetchFirst(hstmt);
	if (fetch_ret == SQL_ERROR) {
		return fetch_ret;
	}

	// When parameters were not bound (plan was not created) on Prepare call, then
	// we need to correct IRD records after every execution
	if (!hstmt->stmt->GetStatementProperties().bound_all_parameters) {
		D_ASSERT(hstmt->stmt->data);
		D_ASSERT(hstmt->res);

		// Copy up to date result types and names from the execution result
		hstmt->stmt->data->types = hstmt->res->types;
		hstmt->stmt->data->names = hstmt->res->names;

		// Correct bounded columns and re-fill IRD records
		hstmt->bound_cols.resize(hstmt->stmt->ColumnCount());
		hstmt->FillIRD();
	}

	return ret;
}

//! Execute statement only once
SQLRETURN duckdb::SingleExecuteStmt(duckdb::OdbcHandleStmt *hstmt) {
	if (hstmt->res) {
		hstmt->res.reset();
	}
	hstmt->odbc_fetcher->ClearChunks();

	hstmt->open = false;
	if (hstmt->rows_fetched_ptr) {
		*hstmt->rows_fetched_ptr = 0;
	}

	duckdb::vector<Value> values;
	SQLRETURN ret = hstmt->param_desc->GetParamValues(values);
	if (ret == SQL_NEED_DATA || ret == SQL_ERROR) {
		return ret;
	}

	hstmt->res = hstmt->stmt->Execute(values);

	if (hstmt->res->HasError()) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SingleExecuteStmt", hstmt->res->GetError(),
		                                   duckdb::SQLStateType::ST_HY000, hstmt->dbc->GetDataSourceName());
	}
	hstmt->open = true;
	if (ret == SQL_STILL_EXECUTING) {
		return SQL_STILL_EXECUTING;
	}
	return SQL_SUCCESS;
}

SQLRETURN duckdb::FetchStmtResult(duckdb::OdbcHandleStmt *hstmt, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {
	if (!hstmt->open) {
		return SQL_NO_DATA;
	}
	SQLRETURN ret = hstmt->odbc_fetcher->Fetch(hstmt, fetch_orientation, fetch_offset);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	hstmt->odbc_fetcher->AssertCurrentChunk();
	return ret;
}

//! Static fuctions used by GetDataStmtResult //

static SQLRETURN ValidateType(LogicalTypeId input, LogicalTypeId expected, duckdb::OdbcHandleStmt *hstmt) {
	if (input != expected) {
		string msg = "Type mismatch error: received " + EnumUtil::ToString(input) + ", but expected " +
		             EnumUtil::ToString(expected);
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "ValidateType", msg, SQLStateType::ST_07006,
		                                   hstmt->dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

static SQLRETURN ThrowInvalidCast(const string &component, const LogicalType &from_type, const LogicalType &to_type,
                                  duckdb::OdbcHandleStmt *hstmt) {
	string msg = "Not implemented Error: Unimplemented type for cast (" + from_type.ToString() + " -> " +
	             to_type.ToString() + ")";

	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, component, msg, SQLStateType::ST_22007,
	                                   hstmt->dbc->GetDataSourceName());
}

template <class SRC, class DEST = SRC>
static SQLRETURN GetInternalValue(duckdb::OdbcHandleStmt *hstmt, const duckdb::Value &val, const LogicalType &type,
                                  SQLPOINTER target_value_ptr, SQLLEN *str_len_or_ind_ptr) {
	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function
	// When the driver returns fixed-length data, such as an integer or a date structure, the driver ignores
	// BufferLength... D_ASSERT(((size_t)buffer_length) >= sizeof(DEST));
	try {
		auto casted_value = val.CastAs(*hstmt->dbc->conn->context, type).GetValue<SRC>();
		Store<DEST>(casted_value, (duckdb::data_ptr_t)target_value_ptr);
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(casted_value);
		}
		return SQL_SUCCESS;
	} catch (std::exception &ex) {
		duckdb::ErrorData parsed_error(ex);
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetInternalValue", parsed_error.RawMessage(),
		                                   SQLStateType::ST_07006, hstmt->dbc->GetDataSourceName());
	}
}

template <class CAST_OP, typename TARGET_TYPE, class CAST_FUNC = std::function<timestamp_t(int64_t)>>
static bool CastTimestampValue(duckdb::OdbcHandleStmt *hstmt, const duckdb::Value &val, TARGET_TYPE &target,
                               CAST_FUNC cast_timestamp_fun) {
	try {
		auto input = val.GetValue<int64_t>();
		auto timestamp = timestamp_t(input);
		// FIXME: add test for casting infinity/-infinity timestamp values
		if (Timestamp::IsFinite(timestamp)) {
			timestamp = cast_timestamp_fun(input);
		}
		target = CAST_OP::template Operation<timestamp_t, TARGET_TYPE>(timestamp);
		return true;
	} catch (std::exception &ex) {
		duckdb::ErrorData parsed_error(ex);
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "CastTimestampValue", parsed_error.RawMessage(),
		                                   SQLStateType::ST_22007, hstmt->dbc->GetDataSourceName());
	}
}

// To retrieve data from a column in parts, the application calls SQLGetData multiple times in succession for the same
// column. On each call, SQLGetData returns the next part of the data. It is up to the application to reassemble the
// parts, taking care to remove the null-termination character from intermediate parts of character data. If there is
// more data to return or not enough buffer was allocated for the terminating character, SQLGetData returns
// SQL_SUCCESS_WITH_INFO and SQLSTATE 01004 (Data truncated). When it returns the last part of the data, SQLGetData
// returns SQL_SUCCESS. If SQLGetData is called after this, it returns SQL_NO_DATA.
//
// Null-termination:
//  - SQL_WCHAR: 2 bytes
//  - SQL_CHAR: 1 byte
//  - SQL_BINARY: None
template <typename CHAR_TYPE>
static SQLRETURN GetVariableValue(SQLUSMALLINT col_idx, duckdb::OdbcHandleStmt *hstmt, SQLPOINTER target_value_ptr,
                                  SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr, CHAR_TYPE *val_buf,
                                  std::size_t val_buf_len_bytes, bool null_terminate = true) {

	// Reset stored length if the column has changed
	hstmt->odbc_fetcher->SetLastFetchedVariableVal(static_cast<duckdb::row_t>(col_idx));

	// Get the length of the part of the field that was returned previously
	std::size_t last_len = hstmt->odbc_fetcher->GetLastFetchedLength();

	// Resulting data is not empty and according to last_len was already returned in full in the previous call,
	// so we just returning SQL_NO_DATA.
	if (val_buf_len_bytes > 0 && last_len >= val_buf_len_bytes) {
		return SQL_NO_DATA;
	}

	// Note that the driver counts the null-termination character when returning character data to *TargetValuePtr.
	// *TargetValuePtr must therefore contain space for the null-termination character.
	if (null_terminate && buffer_length < sizeof(CHAR_TYPE)) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "TargetValuePtr size is insufficient",
		                                   duckdb::SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}

	// Reserve space for null-termination if necessary
	std::size_t buffer_effective_size = static_cast<std::size_t>(buffer_length);
	if (null_terminate) {
		buffer_effective_size -= sizeof(CHAR_TYPE);
	}

	// Calculate the remaining length and actual out_len to copy
	std::size_t remaining_len = val_buf_len_bytes - last_len;

	// Only the length is requested by the client
	if (target_value_ptr == nullptr) {
		if (str_len_or_ind_ptr != nullptr) {
			*str_len_or_ind_ptr = static_cast<SQLLEN>(remaining_len);
			return SQL_SUCCESS;
		}
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "Could not set StrLen_or_IndPtr",
		                                   duckdb::SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}

	// Expect that remaining data fits the buffer
	SQLRETURN ret = SQL_SUCCESS;
	std::size_t out_len = remaining_len;

	// If buffer is too small - more calls from client will be required
	if (out_len > buffer_effective_size) {
		out_len = buffer_effective_size;
		ret = duckdb::SetDiagnosticRecord(
		    hstmt, SQL_SUCCESS_WITH_INFO, "SQLGetData",
		    "Not all the data for the specified column could be retrieved, the length of the data remaining in the "
		    "specific column prior to the current call to SQLGetData is returned in *StrLen_or_IndPtr.",
		    duckdb::SQLStateType::ST_01004, hstmt->dbc->GetDataSourceName());
	}

	// Copy the data
	std::memcpy(static_cast<void *>(target_value_ptr), reinterpret_cast<const char *>(val_buf) + last_len, out_len);

	// Write the null-terminator that may be 1 or 2 bytes
	if (null_terminate) {
		char *target_value_ptr_chars = reinterpret_cast<char *>(target_value_ptr);
		for (std::size_t i = 0; i < sizeof(CHAR_TYPE); i++) {
			target_value_ptr_chars[out_len + i] = '\0';
		}
	}

	// Write the length of the data after conversion and before truncation due to BufferLength.
	// Null-termination bytes are excluded.
	if (str_len_or_ind_ptr != nullptr) {
		*str_len_or_ind_ptr = remaining_len;
	}

	// Save the length of data already written for future calls
	hstmt->odbc_fetcher->SetLastFetchedLength(last_len + out_len);

	return ret;
}

// Resolve the C type based on the value type ID when SQL_C_DEFAULT is specified.
// This logic is not comprehensive, but should be good enough, in general, clients
// are not expected to use SQL_C_DEFAULT.
static SQLSMALLINT resolve_default_type(LogicalTypeId typeId, SQLLEN max_len) {
	switch (typeId) {
	case LogicalTypeId::BOOLEAN:
		return SQL_C_BIT;
	case LogicalTypeId::TINYINT:
		return SQL_C_TINYINT;
	case LogicalTypeId::UTINYINT:
		return SQL_C_UTINYINT;
	case LogicalTypeId::SMALLINT:
		if (max_len < 2) {
			return SQL_C_TINYINT;
		} else {
			return SQL_C_SHORT;
		}
	case LogicalTypeId::USMALLINT:
		if (max_len < 2) {
			return SQL_C_UTINYINT;
		} else {
			return SQL_C_USHORT;
		}
	case LogicalTypeId::INTEGER:
		if (max_len < 2) {
			return SQL_C_TINYINT;
		} else if (max_len < 4) {
			return SQL_C_SHORT;
		} else {
			return SQL_C_LONG;
		}
	case LogicalTypeId::UINTEGER:
		if (max_len < 2) {
			return SQL_C_UTINYINT;
		} else if (max_len < 4) {
			return SQL_C_USHORT;
		} else {
			return SQL_C_ULONG;
		}
	case LogicalTypeId::BIGINT:
		if (max_len < 2) {
			return SQL_C_TINYINT;
		} else if (max_len < 4) {
			return SQL_C_SHORT;
		} else if (max_len < 8) {
			return SQL_C_LONG;
		} else {
			return SQL_C_SBIGINT;
		}
	case LogicalTypeId::UBIGINT:
		if (max_len < 2) {
			return SQL_C_UTINYINT;
		} else if (max_len < 4) {
			return SQL_C_USHORT;
		} else if (max_len < 8) {
			return SQL_C_ULONG;
		} else {
			return SQL_C_UBIGINT;
		}
	case LogicalTypeId::FLOAT:
		return SQL_C_FLOAT;
	case LogicalTypeId::DOUBLE:
		if (max_len < 8) {
			return SQL_C_FLOAT;
		} else {
			return SQL_C_DOUBLE;
		}
	default:
		return SQL_C_CHAR;
	}
}

SQLRETURN duckdb::GetDataStmtResult(OdbcHandleStmt *hstmt, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                                    SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	if (!target_value_ptr && !OdbcUtils::IsCharType(target_type)) {
		return SQL_ERROR;
	}

	Value val;
	if (col_or_param_num > 0) {
		// Prevent underflow
		col_or_param_num--;
	}
	hstmt->odbc_fetcher->GetValue(col_or_param_num, val);
	if (val.IsNull()) {
		if (!str_len_or_ind_ptr) {
			return SQL_ERROR;
		}
		*str_len_or_ind_ptr = SQL_NULL_DATA;
		return SQL_SUCCESS;
	}
	if (val.type().id() == LogicalType::TIMESTAMP_TZ) {
		int64_t utc_micros = val.GetValue<int64_t>();
		int64_t utc_offset_micros = duckdb::OdbcUtils::GetUTCOffsetMicrosFromOS(hstmt, utc_micros);
		val = Value::TIMESTAMP(timestamp_t(utc_micros + utc_offset_micros));
	}

	SQLSMALLINT target_type_resolved = target_type;
	if (target_type_resolved == SQL_C_DEFAULT) {
		target_type_resolved = resolve_default_type(val.type().id(), buffer_length);
	}

	switch (target_type_resolved) {
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
		return GetInternalValue<int16_t, SQLSMALLINT>(hstmt, val, LogicalType::SMALLINT, target_value_ptr,
		                                              str_len_or_ind_ptr);
	case SQL_C_USHORT:
		return GetInternalValue<uint16_t, SQLUSMALLINT>(hstmt, val, LogicalType::USMALLINT, target_value_ptr,
		                                                str_len_or_ind_ptr);
	case SQL_C_LONG:
	case SQL_C_SLONG:
		return GetInternalValue<int32_t, SQLINTEGER>(hstmt, val, LogicalType::INTEGER, target_value_ptr,
		                                             str_len_or_ind_ptr);
	case SQL_C_ULONG:
		return GetInternalValue<uint32_t, SQLUINTEGER>(hstmt, val, LogicalType::UINTEGER, target_value_ptr,
		                                               str_len_or_ind_ptr);
	case SQL_C_FLOAT:
		return GetInternalValue<float, SQLREAL>(hstmt, val, LogicalType::FLOAT, target_value_ptr, str_len_or_ind_ptr);
	case SQL_C_DOUBLE:
		return GetInternalValue<double, SQLFLOAT>(hstmt, val, LogicalType::DOUBLE, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_BIT: {
		LogicalType char_type = LogicalType(LogicalTypeId::CHAR);
		return GetInternalValue<SQLCHAR>(hstmt, val, char_type, target_value_ptr, str_len_or_ind_ptr);
	}
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
		return GetInternalValue<int8_t, SQLSCHAR>(hstmt, val, LogicalType::TINYINT, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_UTINYINT:
		return GetInternalValue<uint8_t, uint8_t>(hstmt, val, LogicalType::UTINYINT, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_SBIGINT:
		return GetInternalValue<int64_t, SQLBIGINT>(hstmt, val, LogicalType::BIGINT, target_value_ptr,
		                                            str_len_or_ind_ptr);
	case SQL_C_UBIGINT:
		// case SQL_C_BOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
		return GetInternalValue<uint64_t, SQLUBIGINT>(hstmt, val, LogicalType::UBIGINT, target_value_ptr,
		                                              str_len_or_ind_ptr);
	case SQL_C_WCHAR: {
		std::string val_str = val.GetValue<std::string>();
		// We need to convert the result to UTF-16 to get its length (in bytes) even if we are not going to return it
		auto utf16_vec = duckdb::widechar::utf8_to_utf16_lenient(reinterpret_cast<const SQLCHAR *>(val_str.c_str()),
		                                                         val_str.length());
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        utf16_vec.data(), utf16_vec.size() * sizeof(SQLWCHAR));
	}
	// case SQL_C_VARBOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
	case SQL_C_BINARY: {
		// treating binary values as BLOB type
		std::string blob = duckdb::Blob::ToBlob(duckdb::string_t(val.GetValue<string>().c_str()));
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        blob.c_str(), blob.length(), false);
	}
	case SQL_C_CHAR: {
		std::string val_str = val.GetValue<std::string>();
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        val_str.c_str(), val_str.length());
	}
	case SQL_C_NUMERIC: {
		if (ValidateType(val.type().id(), LogicalTypeId::DECIMAL, hstmt) != SQL_SUCCESS) {
			return SQL_ERROR;
		}

		SQL_NUMERIC_STRUCT *numeric = (SQL_NUMERIC_STRUCT *)target_value_ptr;
		auto dataptr = (duckdb::data_ptr_t)numeric->val;
		// reset numeric val to remove some garbage
		memset(dataptr, '\0', SQL_MAX_NUMERIC_LEN);

		numeric->sign = 1;
		numeric->precision = numeric->scale = 0;

		string str_val = val.ToString();
		auto width = str_val.size();

		if (str_val[0] == '-') {
			numeric->sign = 0;
			str_val.erase(std::remove(str_val.begin(), str_val.end(), '-'), str_val.end());
			// uncounting negative signal '-'
			--width;
		}

		auto pos_dot = str_val.find('.');
		if (pos_dot != string::npos) {
			str_val.erase(std::remove(str_val.begin(), str_val.end(), '.'), str_val.end());
			numeric->scale = static_cast<SQLSCHAR>(str_val.size() - pos_dot);

			string str_fraction = str_val.substr(pos_dot);
			// case all digits in fraction is 0, remove them
			if (std::stoi(str_fraction) == 0) {
				str_val.erase(str_val.begin() + pos_dot, str_val.end());
			}
			width = str_val.size();
		}
		numeric->precision = static_cast<SQLCHAR>(width);

		string_t str_t(str_val.c_str(), static_cast<uint32_t>(width));
		if (numeric->precision <= Decimal::MAX_WIDTH_INT64) {
			int64_t val_i64;
			if (!duckdb::TryCast::Operation(str_t, val_i64)) {
				return SQL_ERROR;
			}
			memcpy(dataptr, &val_i64, sizeof(val_i64));
		} else {
			hugeint_t huge_int;
			string error_message;
			CastParameters parameters(false, &error_message);
			if (!duckdb::TryCastToDecimal::Operation<string_t, hugeint_t>(str_t, huge_int, parameters,
			                                                              numeric->precision, numeric->scale)) {
				return SQL_ERROR;
			}
			memcpy(dataptr, &huge_int.lower, sizeof(huge_int.lower));
			memcpy(dataptr + sizeof(huge_int.lower), &huge_int.upper, sizeof(huge_int.upper));
		}

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_NUMERIC_STRUCT);
		}
		return SQL_SUCCESS;
	}
	case SQL_C_TYPE_DATE: {
		date_t date;
		switch (val.type().id()) {
		case LogicalTypeId::DATE:
			date = val.GetValue<date_t>();
			break;
		case LogicalTypeId::TIMESTAMP_SEC: {
			if (!CastTimestampValue<duckdb::Cast, date_t>(hstmt, val, date, duckdb::Timestamp::FromEpochSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP_MS: {
			if (!CastTimestampValue<duckdb::Cast, date_t>(hstmt, val, date, duckdb::Timestamp::FromEpochMs)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			if (!CastTimestampValue<duckdb::Cast, date_t>(hstmt, val, date, duckdb::Timestamp::FromEpochMicroSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			if (!CastTimestampValue<duckdb::Cast, date_t>(hstmt, val, date, duckdb::Timestamp::FromEpochNanoSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::VARCHAR: {
			string val_str = val.GetValue<string>();
			auto str_input = string_t(val_str);
			if (!TryCast::Operation<string_t, date_t>(str_input, date)) {
				auto msg = CastExceptionText<string_t, date_t>(str_input);
				return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", msg, SQLStateType::ST_07006,
				                                   hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::DATE, hstmt);
		} // end switch "val.type().id()": SQL_C_TYPE_DATE

		SQL_DATE_STRUCT *date_struct = (SQL_DATE_STRUCT *)target_value_ptr;
		int32_t year, month, day;
		Date::Convert(date, year, month, day);
		date_struct->year = year;
		date_struct->month = month;
		date_struct->day = day;
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_DATE_STRUCT);
		}
		return SQL_SUCCESS;
	}
	case SQL_C_TYPE_TIME: {
		dtime_t time;
		switch (val.type().id()) {
		case LogicalTypeId::TIME:
			time = val.GetValue<dtime_t>();
			break;
		case LogicalTypeId::TIMESTAMP_SEC: {
			if (!CastTimestampValue<duckdb::Cast, dtime_t>(hstmt, val, time, duckdb::Timestamp::FromEpochSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP_MS: {
			if (!CastTimestampValue<duckdb::Cast, dtime_t>(hstmt, val, time, duckdb::Timestamp::FromEpochMs)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			if (!CastTimestampValue<duckdb::Cast, dtime_t>(hstmt, val, time,
			                                               duckdb::Timestamp::FromEpochMicroSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			if (!CastTimestampValue<duckdb::Cast, dtime_t>(hstmt, val, time, duckdb::Timestamp::FromEpochNanoSeconds)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::VARCHAR: {
			string val_str = val.GetValue<string>();
			auto str_input = string_t(val_str);
			if (!TryCast::Operation<string_t, dtime_t>(str_input, time)) {
				auto msg = CastExceptionText<string_t, dtime_t>(str_input);
				return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", msg, SQLStateType::ST_07006,
				                                   hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::TIME, hstmt);
		} // end switch "val.type().id()": SQL_C_TYPE_TIME

		SQL_TIME_STRUCT *time_struct = (SQL_TIME_STRUCT *)target_value_ptr;
		int32_t hour, minute, second, micros;
		duckdb::Time::Convert(time, hour, minute, second, micros);

		time_struct->hour = hour;
		time_struct->minute = minute;
		time_struct->second = second;
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_TIME_STRUCT);
		}
		return SQL_SUCCESS;
	}
	case SQL_C_TYPE_TIMESTAMP: {
		timestamp_t timestamp;
		switch (val.type().id()) {
		case LogicalTypeId::TIMESTAMP_SEC: {
			timestamp = timestamp_t(val.GetValue<int64_t>());
			// FIXME: add test for casting infinity/-infinity timestamp values
			if (!Timestamp::IsFinite(timestamp)) {
				break;
			}
			timestamp = duckdb::Timestamp::FromEpochSeconds(timestamp.value);
			break;
		}
		case LogicalTypeId::TIMESTAMP_MS: {
			timestamp = timestamp_t(val.GetValue<int64_t>());
			// FIXME: add test for casting infinity/-infinity timestamp values
			if (!Timestamp::IsFinite(timestamp)) {
				break;
			}
			timestamp = duckdb::Timestamp::FromEpochMs(timestamp.value);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			timestamp = timestamp_t(val.GetValue<int64_t>());
			timestamp = duckdb::Timestamp::FromEpochMicroSeconds(timestamp.value);
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			timestamp = timestamp_t(val.GetValue<int64_t>());
			// FIXME: add test for casting infinity/-infinity timestamp values
			if (!Timestamp::IsFinite(timestamp)) {
				break;
			}
			timestamp = duckdb::Timestamp::FromEpochNanoSeconds(timestamp.value);
			break;
		}
		case LogicalTypeId::DATE: {
			auto date_input = val.GetValue<date_t>();
			if (!TryCast::Operation<date_t, timestamp_t>(date_input, timestamp)) {
				auto msg = CastExceptionText<date_t, timestamp_t>(date_input);
				return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", msg, SQLStateType::ST_07006,
				                                   hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		case LogicalTypeId::TIME: {
			auto time_input = val.GetValue<dtime_t>();
			// PyODBC requests DB TIME field as SQL_TYPE_TIMESTAMP, but the
			// cast from dtime_t to timestamp_t is not implemented in the
			// engine, so we create timestamp_t instance manually.
			timestamp = timestamp_t(time_input.micros);
			break;
		}
		case LogicalTypeId::VARCHAR: {
			string val_str = val.GetValue<string>();
			auto str_input = string_t(val_str);
			if (!TryCast::Operation<string_t, timestamp_t>(str_input, timestamp)) {
				auto msg = CastExceptionText<string_t, timestamp_t>(str_input);
				return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", msg, SQLStateType::ST_07006,
				                                   hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::TIMESTAMP, hstmt);
		} // end switch "val.type().id()"

		SQL_TIMESTAMP_STRUCT *timestamp_struct = (SQL_TIMESTAMP_STRUCT *)target_value_ptr;
		date_t date = duckdb::Timestamp::GetDate(timestamp);

		int32_t year, month, day;
		Date::Convert(date, year, month, day);
		timestamp_struct->year = year;
		timestamp_struct->month = month;
		timestamp_struct->day = day;

		dtime_t time = duckdb::Timestamp::GetTime(timestamp);
		int32_t hour, minute, second, micros;
		duckdb::Time::Convert(time, hour, minute, second, micros);
		timestamp_struct->hour = hour;
		timestamp_struct->minute = minute;
		timestamp_struct->second = second;
		timestamp_struct->fraction = micros;
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_TIMESTAMP_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_YEAR: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetYear(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_MONTH: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetMonth(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_DAY: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetDay(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_HOUR: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetHour(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_MINUTE: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetMinute(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_SECOND: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetSecond(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_YEAR_TO_MONTH: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetYear(interval, interval_struct);
		// fraction of years stored as months
		interval_struct->intval.year_month.month = std::abs(interval.months) % duckdb::Interval::MONTHS_PER_YEAR;
		interval_struct->interval_type = SQLINTERVAL::SQL_IS_YEAR_TO_MONTH;
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_DAY_TO_HOUR: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetDayToHour(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_DAY_TO_MINUTE: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetDayToMinute(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_DAY_TO_SECOND: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetDayToSecond(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_HOUR_TO_MINUTE: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetHourToMinute(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_HOUR_TO_SECOND: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetHourToSecond(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val.type(), LogicalType::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetMinuteToSecond(interval, interval_struct);
		OdbcInterval::SetSignal(interval, interval_struct);

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_INTERVAL_STRUCT);
		}

		return SQL_SUCCESS;
	}
	// TODO other types
	default:
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", "Data type not supported",
		                                   SQLStateType::ST_HY004, hstmt->dbc->GetDataSourceName());
	} // end switch "(target_type_resolved)": SQL_C_TYPE_TIMESTAMP
}

SQLRETURN duckdb::ExecDirectStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	bool success_with_info = false;
	// Set up the statement and extract the query
	auto query = GetQueryAsString(hstmt, statement_text, text_length);

	// Extract the statements from the query
	vector<unique_ptr<SQLStatement>> statements;
	try {
		statements = hstmt->dbc->conn->ExtractStatements(query);
	} catch (std::exception &ex) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLExecDirect", ex.what(), SQLStateType::ST_42000,
		                                   hstmt->dbc->GetDataSourceName());
	}

	for (auto &statement : statements) {
		hstmt->stmt = hstmt->dbc->conn->Prepare(std::move(statement));
		ret = FinalizeStmt(hstmt);
		if (!hstmt->stmt->success || !SQL_SUCCEEDED(ret)) {
			return ret;
		} else if (ret == SQL_SUCCESS_WITH_INFO) {
			success_with_info = true;
		}

		ret = duckdb::BatchExecuteStmt(hstmt);
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		} else if (ret == SQL_SUCCESS_WITH_INFO) {
			success_with_info = true;
		}
	}
	return success_with_info ? SQL_SUCCESS_WITH_INFO : ret;
}

SQLRETURN duckdb::BindParameterStmt(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number,
                                    SQLSMALLINT input_output_type, SQLSMALLINT value_type, SQLSMALLINT parameter_type,
                                    SQLULEN column_size, SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr,
                                    SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (input_output_type != SQL_PARAM_INPUT) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLBindParameter", "Only SQL_PARAM_INPUT is supported.",
		                           SQLStateType::ST_HYC00, hstmt->dbc->GetDataSourceName());
	}
	/* check input parameters */
	if (parameter_number < 1) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLBindParameter", "Invalid descriptor index.",
		                           SQLStateType::ST_07009, hstmt->dbc->GetDataSourceName());
	}
	idx_t param_idx = parameter_number - 1;

	//! New descriptor
	auto ipd_record = hstmt->param_desc->ipd->GetDescRecord(param_idx);
	auto apd_record = hstmt->param_desc->apd->GetDescRecord(param_idx);

	ipd_record->sql_desc_parameter_type = input_output_type;
	ipd_record->sql_desc_length = column_size;
	ipd_record->sql_desc_precision = static_cast<SQLSMALLINT>(column_size);
	ipd_record->sql_desc_scale = decimal_digits;
	if (value_type == SQL_DECIMAL || value_type == SQL_NUMERIC || value_type == SQL_FLOAT || value_type == SQL_REAL ||
	    value_type == SQL_DOUBLE) {
		ipd_record->sql_desc_precision = static_cast<SQLSMALLINT>(column_size);
	}

	if (ipd_record->SetSqlDataType(parameter_type) == SQL_ERROR ||
	    apd_record->SetSqlDataType(value_type) == SQL_ERROR) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLBindParameter", "Invalid data type.", SQLStateType::ST_HY004,
		                           hstmt->dbc->GetDataSourceName());
	}

	apd_record->sql_desc_data_ptr = parameter_value_ptr;
	apd_record->sql_desc_octet_length = buffer_length;
	apd_record->sql_desc_indicator_ptr = str_len_or_ind_ptr;
	apd_record->sql_desc_octet_length_ptr = str_len_or_ind_ptr;

	return SQL_SUCCESS;
}

SQLRETURN duckdb::CloseStmt(duckdb::OdbcHandleStmt *hstmt) {
	hstmt->Close();
	return SQL_SUCCESS;
}
