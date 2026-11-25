#include "statement_functions.hpp"
#include "api_info.hpp"
#include "handle_functions.hpp"
#include "odbc_interval.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "descriptor.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "widechar.hpp"

using duckdb_odbc::Date;
using duckdb_odbc::date_t;
using duckdb_odbc::dtime_t;
using duckdb_odbc::idx_t;
using duckdb_odbc::interval_t;
using duckdb_odbc::LogicalType;
using duckdb_odbc::LogicalTypeId;
using duckdb_odbc::OdbcDiagnostic;
using duckdb_odbc::OdbcInterval;
using duckdb_odbc::OdbcType;
using duckdb_odbc::OdbcUtils;
using duckdb_odbc::SQLStateType;
using duckdb_odbc::Store;
using duckdb_odbc::string;
using duckdb_odbc::Time;
using duckdb_odbc::Timestamp;
using duckdb_odbc::timestamp_t;
using duckdb_odbc::Value;
using duckdb_odbc::vector;

void duckdb_odbc::PrepareQuery(OdbcHandleStmt *hstmt) {
	if (hstmt->stmt) {
		hstmt->stmt.reset();
	}

	hstmt->has_result = false;
	hstmt->result_error.clear();

	hstmt->odbc_fetcher->ClearChunks();
}

duckdb_odbc::unique_ptr<duckdb_odbc::OdbcPreparedStatement>
duckdb_odbc::PrepareStatement(OdbcHandleStmt *hstmt, unique_ptr<cxx::SqlStatement> statement,
                              bool for_repeated_execution) {
	auto result = make_uniq<OdbcPreparedStatement>();
	result->statement_type = statement->GetStatementType();
	result->parameter_count = statement->GetParameterNames().size();
	result->statement = std::move(statement);
	auto &conn = *hstmt->dbc->conn;
	try {
		if (for_repeated_execution) {
			result->prepared = make_uniq<cxx::PreparedStatement>(conn.Prepare(*result->statement));
		}
	} catch (std::exception &ex) {
		result->error = ErrorMessage(ex);
		return result;
	}
	if (!for_repeated_execution) {
		// SQLExecDirect: the result columns are taken from the result, and the parameter types are inferred when the
		// statement is executed
		return result;
	}

	// Binding learns the result columns and the parameter types. It can only tell when every parameter type can be
	// inferred, which it cannot for e.g. 'SELECT ?': those are known only once the statement is executed.
	try {
		auto signature = conn.Bind(*result->statement);
		bool all_known = true;
		// the signature orders the parameters by their key ("1", "10", "2", ...): put them in positional order
		auto parameter_names = result->statement->GetParameterNames();
		result->parameter_types.resize(parameter_names.size());
		for (idx_t i = 0; i < signature.parameters.GetFieldCount(); i++) {
			OdbcType type(signature.parameters.GetFieldType(i));
			all_known = all_known && type.id != LogicalTypeId::UNKNOWN && type.id != LogicalTypeId::ANY;
			auto name = signature.parameters.GetFieldName(i);
			auto position = std::find(parameter_names.begin(), parameter_names.end(), name) - parameter_names.begin();
			if (static_cast<idx_t>(position) < result->parameter_types.size()) {
				result->parameter_types[position] = std::move(type);
			}
		}
		for (idx_t i = 0; i < signature.output.GetFieldCount(); i++) {
			OdbcType type(signature.output.GetFieldType(i));
			all_known = all_known && type.id != LogicalTypeId::UNKNOWN;
			result->types.push_back(std::move(type));
			result->names.emplace_back(signature.output.GetFieldName(i));
		}
		result->bound_all_parameters = all_known;
		result->warn_unbound_parameters = !all_known;
		if (!all_known) {
			result->parameter_types.clear();
			result->types.clear();
			result->names.clear();
		}
	} catch (std::exception &ex) {
		// e.g. a statement that expands into several, whose columns are only known once it runs
		result->bound_all_parameters = false;
	}
	return result;
}

SQLRETURN duckdb_odbc::FinalizeStmt(OdbcHandleStmt *hstmt) {
	if (hstmt->stmt->HasError()) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "PrepareStmt", hstmt->stmt->GetError(), SQLStateType::ST_42000,
		                           hstmt->dbc->GetDataSourceName());
	}

	SQLRETURN ret = SQL_SUCCESS;

	// the parameters are known from parsing, and do not change on rebind
	hstmt->param_desc->ResetParams(static_cast<SQLSMALLINT>(hstmt->stmt->ParameterCount()));

	// When the parameter types cannot be determined, as for 'SELECT ?', neither can the resulting columns (their types
	// and names). Bound columns and IRD records depend on resulting columns. When bound_all_parameters=false, we don't
	// have resulting columns defined, so bound columns and IRD records are kept empty and are re-filled from the query
	// result after every execution (they may be different depending on input parameters supplied for execution).
	if (hstmt->stmt->warn_unbound_parameters) {
		ret = SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "PrepareStmt", "Not all parameters are bound",
		                          SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
	}
	if (!hstmt->stmt->prepared) {
		// SQLExecDirect: the IRD records are filled from the result once the statement ran, keeping the columns bound
		// so far
		hstmt->FillIRD({}, {});
		return ret;
	}
	if (hstmt->stmt->bound_all_parameters) {
		hstmt->FillIRD(hstmt->stmt->types, hstmt->stmt->names);
	} else {
		hstmt->FillIRD({}, {});
	}
	hstmt->bound_cols.resize(hstmt->row_desc->GetIRD()->GetRecordCount());

	return ret;
}

//! Execute stmt in a batch manner while there is a parameter set to process,
//! the stmt is executed multiple times when there is a bound array of parameters in INSERT and UPDATE statements
SQLRETURN duckdb_odbc::BatchExecuteStmt(OdbcHandleStmt *hstmt) {
	SQLRETURN ret = SQL_SUCCESS;
	bool skipped_paramset = false;
	do {
		ret = SingleExecuteStmt(hstmt);
		skipped_paramset = skipped_paramset || hstmt->param_desc->skip_paramset;
	} while (ret == SQL_STILL_EXECUTING);

	// Early exit on error
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	if (skipped_paramset) {
		// a parameter set that failed to convert was skipped, with a diagnostic
		ret = SQL_SUCCESS_WITH_INFO;
	}

	// now, fetching the first chunk to verify constant folding (See: PR #2462 and issue #2452)
	auto fetch_ret = hstmt->odbc_fetcher->FetchFirst(hstmt);
	if (fetch_ret == SQL_ERROR) {
		return fetch_ret;
	}

	// When the result columns were not known when the statement was prepared, we need to correct the IRD records
	// after every execution (unless the last parameter set was skipped)
	if (!hstmt->stmt->bound_all_parameters && hstmt->has_result) {

		// Re-fill IRD records from up to date result types and names and correct bound columns
		hstmt->FillIRD(hstmt->result_types, hstmt->result_names);
		hstmt->bound_cols.resize(hstmt->row_desc->GetIRD()->GetRecordCount());
	}

	return ret;
}

//! Runs the statement with the current parameter values and reads its result in full.
static SQLRETURN RunStatement(duckdb_odbc::OdbcHandleStmt *hstmt) {
	auto &dbc = *hstmt->dbc;
	duckdb_odbc::unique_ptr<duckdb_odbc::QueryResult> res;
	try {
		res = hstmt->stmt->Execute(*dbc.conn, hstmt->param_desc->values);
	} catch (std::exception &ex) {
		// binding the statement failed
		hstmt->result_error = duckdb_odbc::ErrorMessage(ex);
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SingleExecuteStmt", hstmt->result_error,
		                                        duckdb_odbc::SQLStateType::ST_42000, dbc.GetDataSourceName());
	}

	// DuckDB executes a statement as its result is read: read it in full, which frees the connection
	auto prefetch_ret = hstmt->odbc_fetcher->Prefetch(hstmt, *res);
	if (!SQL_SUCCEEDED(prefetch_ret)) {
		return prefetch_ret;
	}
	try {
		// only known once the result was read from, for a statement that expands into several
		auto schema = res->GetSchema();
		hstmt->result_types.clear();
		hstmt->result_names.clear();
		for (duckdb_odbc::idx_t i = 0; i < schema.GetFieldCount(); i++) {
			hstmt->result_types.emplace_back(schema.GetFieldType(i));
			hstmt->result_names.emplace_back(schema.GetFieldName(i));
		}
		hstmt->result_type = res->GetResultType();
		hstmt->result_statement_type = res->GetStatementType();
	} catch (std::exception &ex) {
		hstmt->odbc_fetcher->ClearChunks();
		hstmt->result_error = duckdb_odbc::ErrorMessage(ex);
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SingleExecuteStmt", hstmt->result_error,
		                                        duckdb_odbc::SQLStateType::ST_HY000, dbc.GetDataSourceName());
	}
	hstmt->has_result = true;
	return SQL_SUCCESS;
}

//! Execute statement only once
SQLRETURN duckdb_odbc::SingleExecuteStmt(OdbcHandleStmt *hstmt) {
	hstmt->has_result = false;
	hstmt->result_error.clear();
	hstmt->odbc_fetcher->ClearChunks();

	hstmt->open = false;
	if (hstmt->rows_fetched_ptr) {
		*hstmt->rows_fetched_ptr = 0;
	}

	SQLRETURN ret = hstmt->param_desc->GetParamValues();
	if (ret == SQL_NEED_DATA || ret == SQL_ERROR) {
		return ret;
	}
	if (hstmt->param_desc->skip_paramset) {
		// reported as SQL_PARAM_ERROR in the parameter status array, with a diagnostic
		return ret;
	}

	auto run_ret = RunStatement(hstmt);
	try {
		// also when the statement failed: e.g. a COMMIT that failed rolled the transaction back
		hstmt->dbc->AfterStatement(*hstmt->stmt->statement);
	} catch (std::exception &ex) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SingleExecuteStmt", ErrorMessage(ex),
		                                        duckdb_odbc::SQLStateType::ST_HY000, hstmt->dbc->GetDataSourceName());
	}
	if (!SQL_SUCCEEDED(run_ret)) {
		return run_ret;
	}

	hstmt->open = true;
	if (ret == SQL_STILL_EXECUTING) {
		return SQL_STILL_EXECUTING;
	}
	return SQL_SUCCESS;
}

SQLRETURN duckdb_odbc::FetchStmtResult(OdbcHandleStmt *hstmt, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {
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

static SQLRETURN ValidateType(LogicalTypeId input, LogicalTypeId expected, duckdb_odbc::OdbcHandleStmt *hstmt) {
	if (input != expected) {
		std::string msg = "Type mismatch error: received " + duckdb_odbc::TypeIdToString(input) + ", but expected " +
		                  duckdb_odbc::TypeIdToString(expected);
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "ValidateType", msg, SQLStateType::ST_07006,
		                                        hstmt->dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

static SQLRETURN ThrowInvalidCast(const std::string &component, const Value &from, LogicalTypeId to_type,
                                  duckdb_odbc::OdbcHandleStmt *hstmt) {
	std::string msg = "Not implemented Error: Unimplemented type for cast (" + from.GetLogicalType().ToText() + " -> " +
	                  duckdb_odbc::TypeIdToString(to_type) + ")";

	return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, component, msg, SQLStateType::ST_22007,
	                                        hstmt->dbc->GetDataSourceName());
}

// SQL_NUMERIC_STRUCT holds the unscaled value in SQL_MAX_NUMERIC_LEN (16) bytes, which ODBC defines as a precision of
// at most 38 digits.
static constexpr uint32_t MAX_NUMERIC_STRUCT_PRECISION = 38;

// Returns a DECIMAL wider than 38 digits as a SQL_NUMERIC_STRUCT. The type does not fit the struct, but a value may:
// it is returned exactly when its unscaled value has at most 38 digits, at the scale of the column where that is
// possible, and else at the largest scale that keeps every digit (only trailing zeros of the fraction are dropped). A
// value that needs more digits is SQLSTATE 22003, it is never truncated or rounded.
static SQLRETURN GetWideDecimalAsNumeric(duckdb_odbc::OdbcHandleStmt *hstmt, const duckdb_odbc::Value &val,
                                         SQL_NUMERIC_STRUCT *numeric, SQLLEN *str_len_or_ind_ptr) {
	auto &factory = hstmt->dbc->conn->GetFactory();
	auto type = val.GetLogicalType();
	auto scale = type.GetDecimalScale();
	// the text of a DECIMAL is its digits, with a sign when negative and a decimal point when the scale is not zero
	auto text = duckdb_odbc::ValueToString(factory, val);

	bool negative = false;
	std::string digits;
	for (auto c : text) {
		if (c == '-') {
			negative = true;
		} else if (c >= '0' && c <= '9') {
			if (c != '0' || !digits.empty()) {
				digits += c;
			}
		}
	}
	while ((digits.size() > MAX_NUMERIC_STRUCT_PRECISION || scale > MAX_NUMERIC_STRUCT_PRECISION) && scale > 0 &&
	       (digits.empty() || digits.back() == '0')) {
		if (!digits.empty()) {
			digits.pop_back();
		}
		scale--;
	}
	if (digits.size() > MAX_NUMERIC_STRUCT_PRECISION || scale > MAX_NUMERIC_STRUCT_PRECISION) {
		std::string msg = "Numeric value out of range: the " + type.ToText() +
		                  " value does not fit the 38 digits of SQL_NUMERIC_STRUCT, fetch it as SQL_C_CHAR instead";
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", msg, SQLStateType::ST_22003,
		                                        hstmt->dbc->GetDataSourceName());
	}

	// val is the little-endian magnitude: 38 digits always fit its 16 bytes
	memset(numeric->val, '\0', SQL_MAX_NUMERIC_LEN);
	for (auto c : digits) {
		uint32_t carry = static_cast<uint32_t>(c - '0');
		for (idx_t i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
			uint32_t cur = static_cast<uint32_t>(numeric->val[i]) * 10 + carry;
			numeric->val[i] = static_cast<SQLCHAR>(cur & 0xFF);
			carry = cur >> 8;
		}
	}
	numeric->precision = static_cast<SQLCHAR>(MAX_NUMERIC_STRUCT_PRECISION);
	numeric->scale = static_cast<SQLSCHAR>(scale);
	numeric->sign = negative ? 0 : 1;
	if (str_len_or_ind_ptr) {
		*str_len_or_ind_ptr = sizeof(SQL_NUMERIC_STRUCT);
	}
	return SQL_SUCCESS;
}

template <class SRC, class DEST = SRC>
static SQLRETURN GetInternalValue(duckdb_odbc::OdbcHandleStmt *hstmt, const duckdb_odbc::Value &val, LogicalTypeId type,
                                  SQLPOINTER target_value_ptr, SQLLEN *str_len_or_ind_ptr) {
	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function
	// When the driver returns fixed-length data, such as an integer or a date structure, the driver ignores
	// BufferLength... D_ASSERT(((size_t)buffer_length) >= sizeof(DEST));
	try {
		auto &factory = hstmt->dbc->conn->GetFactory();
		auto casted_value = val.Cast(factory, duckdb_odbc::CreateType(factory, type)).Get<SRC>();
		Store<DEST>(casted_value, (duckdb_odbc::data_ptr_t)target_value_ptr);
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(casted_value);
		}
		return SQL_SUCCESS;
	} catch (std::exception &ex) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetInternalValue", duckdb_odbc::RawErrorMessage(ex),
		                                        SQLStateType::ST_07006, hstmt->dbc->GetDataSourceName());
	}
}

//! The stored integer of a TIMESTAMP_SEC / TIMESTAMP_MS / TIMESTAMP / TIMESTAMP_NS value, in the type's own unit.
static int64_t GetTimestampValue(const duckdb_odbc::Value &val, LogicalTypeId type_id) {
	switch (type_id) {
	case LogicalTypeId::TIMESTAMP_SEC:
		return val.Get<duckdb_odbc::timestamp_s_t>().seconds;
	case LogicalTypeId::TIMESTAMP_MS:
		return val.Get<duckdb_odbc::timestamp_ms_t>().millis;
	case LogicalTypeId::TIMESTAMP_NS:
		return val.Get<duckdb_odbc::timestamp_ns_t>().nanos;
	default:
		return val.Get<timestamp_t>().micros;
	}
}

//! A TIMESTAMP_SEC / TIMESTAMP_MS / TIMESTAMP / TIMESTAMP_NS value as a TIMESTAMP, infinities included.
static timestamp_t GetTimestamp(const duckdb_odbc::Value &val, LogicalTypeId type_id) {
	auto input = GetTimestampValue(val, type_id);
	timestamp_t timestamp {input};
	// FIXME: add test for casting infinity/-infinity timestamp values
	if (!Timestamp::IsFinite(input)) {
		return timestamp;
	}
	switch (type_id) {
	case LogicalTypeId::TIMESTAMP_SEC:
		return Timestamp::FromEpochSeconds(input);
	case LogicalTypeId::TIMESTAMP_MS:
		return Timestamp::FromEpochMs(input);
	case LogicalTypeId::TIMESTAMP_NS:
		return Timestamp::FromEpochNanoSeconds(input);
	default:
		return timestamp;
	}
}

//! The date part of a timestamp value, false (with a diagnostic) when it does not convert.
static bool CastTimestampValue(duckdb_odbc::OdbcHandleStmt *hstmt, const duckdb_odbc::Value &val, LogicalTypeId type_id,
                               date_t &target) {
	try {
		target = Timestamp::GetDate(GetTimestamp(val, type_id));
		return true;
	} catch (std::exception &ex) {
		duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "CastTimestampValue", duckdb_odbc::RawErrorMessage(ex),
		                                 SQLStateType::ST_22007, hstmt->dbc->GetDataSourceName());
		return false;
	}
}

//! The time part of a timestamp value, false (with a diagnostic) when it does not convert.
static bool CastTimestampValue(duckdb_odbc::OdbcHandleStmt *hstmt, const duckdb_odbc::Value &val, LogicalTypeId type_id,
                               dtime_t &target) {
	try {
		target = Timestamp::GetTime(GetTimestamp(val, type_id));
		return true;
	} catch (std::exception &ex) {
		duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "CastTimestampValue", duckdb_odbc::RawErrorMessage(ex),
		                                 SQLStateType::ST_22007, hstmt->dbc->GetDataSourceName());
		return false;
	}
}

//! Casts a VARCHAR value to `type`, false (with a diagnostic) when it does not convert.
static bool CastStringValue(duckdb_odbc::OdbcHandleStmt *hstmt, const duckdb_odbc::Value &val, LogicalTypeId type,
                            duckdb_odbc::unique_ptr<Value> &result) {
	try {
		auto &factory = hstmt->dbc->conn->GetFactory();
		result = duckdb_odbc::make_uniq<Value>(val.Cast(factory, duckdb_odbc::CreateType(factory, type)));
		return true;
	} catch (std::exception &ex) {
		duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", duckdb_odbc::RawErrorMessage(ex),
		                                 SQLStateType::ST_07006, hstmt->dbc->GetDataSourceName());
		return false;
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
static SQLRETURN GetVariableValue(SQLUSMALLINT col_idx, duckdb_odbc::OdbcHandleStmt *hstmt, SQLPOINTER target_value_ptr,
                                  SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr, CHAR_TYPE *val_buf,
                                  size_t val_buf_len_bytes, bool null_terminate = true) {

	// Reset stored length if the column has changed
	hstmt->odbc_fetcher->SetLastFetchedVariableVal(static_cast<duckdb_odbc::row_t>(col_idx));

	// Get the length of the part of the field that was returned previously
	size_t last_len = hstmt->odbc_fetcher->GetLastFetchedLength();

	// Resulting data is not empty and according to last_len was already returned in full in the previous call,
	// so we just returning SQL_NO_DATA.
	if (val_buf_len_bytes > 0 && last_len >= val_buf_len_bytes) {
		return SQL_NO_DATA;
	}

	// Note that the driver counts the null-termination character when returning character data to *TargetValuePtr.
	// *TargetValuePtr must therefore contain space for the null-termination character.
	if (null_terminate && buffer_length < sizeof(CHAR_TYPE)) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "TargetValuePtr size is insufficient",
		                                        duckdb_odbc::SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}

	// Reserve space for null-termination if necessary
	size_t buffer_effective_size = static_cast<size_t>(buffer_length);
	if (null_terminate) {
		buffer_effective_size -= sizeof(CHAR_TYPE);
	}

	// Calculate the remaining length and actual out_len to copy
	size_t remaining_len = val_buf_len_bytes - last_len;

	// Only the length is requested by the client
	if (target_value_ptr == nullptr) {
		if (str_len_or_ind_ptr != nullptr) {
			*str_len_or_ind_ptr = static_cast<SQLLEN>(remaining_len);
			return SQL_SUCCESS;
		}
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "Could not set StrLen_or_IndPtr",
		                                        duckdb_odbc::SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}

	// Expect that remaining data fits the buffer
	SQLRETURN ret = SQL_SUCCESS;
	size_t out_len = remaining_len;

	// If buffer is too small - more calls from client will be required
	if (out_len > buffer_effective_size) {
		out_len = buffer_effective_size;
		ret = duckdb_odbc::SetDiagnosticRecord(
		    hstmt, SQL_SUCCESS_WITH_INFO, "SQLGetData",
		    "Not all the data for the specified column could be retrieved, the length of the data remaining in the "
		    "specific column prior to the current call to SQLGetData is returned in *StrLen_or_IndPtr.",
		    duckdb_odbc::SQLStateType::ST_01004, hstmt->dbc->GetDataSourceName());
	}

	// Copy the data
	std::memcpy(static_cast<void *>(target_value_ptr), reinterpret_cast<const char *>(val_buf) + last_len, out_len);

	// Write the null-terminator that may be 1 or 2 bytes
	if (null_terminate) {
		char *target_value_ptr_chars = reinterpret_cast<char *>(target_value_ptr);
		for (size_t i = 0; i < sizeof(CHAR_TYPE); i++) {
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

SQLRETURN duckdb_odbc::GetDataStmtResult(OdbcHandleStmt *hstmt, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                                         SQLPOINTER target_value_ptr, SQLLEN buffer_length,
                                         SQLLEN *str_len_or_ind_ptr) {
	if (!target_value_ptr && !OdbcUtils::IsCharType(target_type)) {
		return SQL_ERROR;
	}

	if (col_or_param_num > 0) {
		// Prevent underflow
		col_or_param_num--;
	}
	if (!hstmt->odbc_fetcher->HasCurrentRow()) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "The cursor is not positioned on a row",
		                                        SQLStateType::ST_24000, hstmt->dbc->GetDataSourceName());
	}
	if (col_or_param_num >= hstmt->row_desc->GetIRD()->GetRecordCount()) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetData", "Invalid descriptor index",
		                                        SQLStateType::ST_07009, hstmt->dbc->GetDataSourceName());
	}
	auto &factory = hstmt->dbc->conn->GetFactory();
	Value val = hstmt->odbc_fetcher->GetValue(col_or_param_num);
	if (val.IsNull()) {
		if (!str_len_or_ind_ptr) {
			return SQL_ERROR;
		}
		*str_len_or_ind_ptr = SQL_NULL_DATA;
		return SQL_SUCCESS;
	}
	auto val_type_id = val.GetLogicalType().GetTypeId();
	if (val_type_id == LogicalTypeId::TIMESTAMP_TZ) {
		int64_t utc_micros = val.Get<duckdb_odbc::timestamp_tz_t>().micros;
		int64_t utc_offset_micros = duckdb_odbc::OdbcUtils::GetUTCOffsetMicrosFromOS(hstmt, utc_micros);
		val = Value::Create(factory, timestamp_t {utc_micros + utc_offset_micros});
		val_type_id = LogicalTypeId::TIMESTAMP;
	}

	SQLSMALLINT target_type_resolved = target_type;
	if (target_type_resolved == SQL_C_DEFAULT) {
		target_type_resolved = resolve_default_type(val_type_id, buffer_length);
	}

	switch (target_type_resolved) {
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
		return GetInternalValue<int16_t, SQLSMALLINT>(hstmt, val, LogicalTypeId::SMALLINT, target_value_ptr,
		                                              str_len_or_ind_ptr);
	case SQL_C_USHORT:
		return GetInternalValue<uint16_t, SQLUSMALLINT>(hstmt, val, LogicalTypeId::USMALLINT, target_value_ptr,
		                                                str_len_or_ind_ptr);
	case SQL_C_LONG:
	case SQL_C_SLONG:
		return GetInternalValue<int32_t, SQLINTEGER>(hstmt, val, LogicalTypeId::INTEGER, target_value_ptr,
		                                             str_len_or_ind_ptr);
	case SQL_C_ULONG:
		return GetInternalValue<uint32_t, SQLUINTEGER>(hstmt, val, LogicalTypeId::UINTEGER, target_value_ptr,
		                                               str_len_or_ind_ptr);
	case SQL_C_FLOAT:
		return GetInternalValue<float, SQLREAL>(hstmt, val, LogicalTypeId::FLOAT, target_value_ptr, str_len_or_ind_ptr);
	case SQL_C_DOUBLE:
		return GetInternalValue<double, SQLFLOAT>(hstmt, val, LogicalTypeId::DOUBLE, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_BIT: {
		// FIXME: the driver has always converted to the CHAR type here, which no value casts to, so SQL_C_BIT is
		// refused for every column type (see the result_conversion test)
		std::string msg =
		    "Conversion Error: Unimplemented type for cast (" + val.GetLogicalType().ToText() + " -> CHAR)";
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetInternalValue", msg, SQLStateType::ST_07006,
		                                        hstmt->dbc->GetDataSourceName());
	}
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
		return GetInternalValue<int8_t, SQLSCHAR>(hstmt, val, LogicalTypeId::TINYINT, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_UTINYINT:
		return GetInternalValue<uint8_t, uint8_t>(hstmt, val, LogicalTypeId::UTINYINT, target_value_ptr,
		                                          str_len_or_ind_ptr);
	case SQL_C_SBIGINT:
		return GetInternalValue<int64_t, SQLBIGINT>(hstmt, val, LogicalTypeId::BIGINT, target_value_ptr,
		                                            str_len_or_ind_ptr);
	case SQL_C_UBIGINT:
		// case SQL_C_BOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
		return GetInternalValue<uint64_t, SQLUBIGINT>(hstmt, val, LogicalTypeId::UBIGINT, target_value_ptr,
		                                              str_len_or_ind_ptr);
	case SQL_C_WCHAR: {
		std::string val_str = duckdb_odbc::ValueToString(factory, val);
		// We need to convert the result to UTF-16 to get its length (in bytes) even if we are not going to return it
		auto utf16_vec = duckdb_odbc::widechar::utf8_to_utf16_lenient(
		    reinterpret_cast<const SQLCHAR *>(val_str.c_str()), val_str.length());
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        utf16_vec.data(), utf16_vec.size() * sizeof(SQLWCHAR));
	}
	// case SQL_C_VARBOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
	case SQL_C_BINARY: {
		// treating binary values as BLOB type
		std::string blob;
		if (val_type_id == LogicalTypeId::BLOB) {
			blob = std::string(val.Get<duckdb_odbc::cxx::blob_t>().view());
		} else {
			duckdb_odbc::unique_ptr<Value> blob_val;
			auto text = duckdb_odbc::CreateVarchar(factory, duckdb_odbc::ValueToString(factory, val));
			if (!CastStringValue(hstmt, text, LogicalTypeId::BLOB, blob_val)) {
				return SQL_ERROR;
			}
			blob = std::string(blob_val->Get<duckdb_odbc::cxx::blob_t>().view());
		}
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        blob.c_str(), blob.length(), false);
	}
	case SQL_C_CHAR: {
		std::string val_str = duckdb_odbc::ValueToString(factory, val);
		return GetVariableValue(col_or_param_num, hstmt, target_value_ptr, buffer_length, str_len_or_ind_ptr,
		                        val_str.c_str(), val_str.length());
	}
	case SQL_C_NUMERIC: {
		if (ValidateType(val_type_id, LogicalTypeId::DECIMAL, hstmt) != SQL_SUCCESS) {
			return SQL_ERROR;
		}

		SQL_NUMERIC_STRUCT *numeric = (SQL_NUMERIC_STRUCT *)target_value_ptr;
		auto dataptr = (duckdb_odbc::data_ptr_t)numeric->val;
		// reset numeric val to remove some garbage
		memset(dataptr, '\0', SQL_MAX_NUMERIC_LEN);

		if (val.GetLogicalType().GetDecimalWidth() > MAX_NUMERIC_STRUCT_PRECISION) {
			// a DECIMAL wider than SQL_NUMERIC_STRUCT: exact when the value fits the struct, an error when not
			return GetWideDecimalAsNumeric(hstmt, val, numeric, str_len_or_ind_ptr);
		}

		// precision and scale are taken from the column type, val holds the unscaled integer at that scale
		auto decimal = val.GetDecimal();
		numeric->precision = static_cast<SQLCHAR>(decimal.width);
		numeric->scale = static_cast<SQLSCHAR>(decimal.scale);

		uint64_t lower = decimal.value.lower;
		uint64_t upper = static_cast<uint64_t>(decimal.value.upper);
		numeric->sign = decimal.value.upper < 0 ? 0 : 1;
		if (numeric->sign == 0) {
			// two's complement negation; cannot overflow, max DECIMAL width is 38 digits
			lower = ~lower + 1;
			upper = ~upper + (lower == 0 ? 1 : 0);
		}
		// val is little-endian
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			dataptr[i] = static_cast<uint8_t>(lower >> (i * 8));
			dataptr[sizeof(uint64_t) + i] = static_cast<uint8_t>(upper >> (i * 8));
		}

		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_NUMERIC_STRUCT);
		}
		return SQL_SUCCESS;
	}
	case SQL_C_TYPE_DATE: {
		date_t date;
		switch (val_type_id) {
		case LogicalTypeId::DATE:
			date = val.Get<date_t>();
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS: {
			if (!CastTimestampValue(hstmt, val, val_type_id, date)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::VARCHAR: {
			duckdb_odbc::unique_ptr<Value> date_val;
			if (!CastStringValue(hstmt, val, LogicalTypeId::DATE, date_val)) {
				return SQL_ERROR;
			}
			date = date_val->Get<date_t>();
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::DATE, hstmt);
		} // end switch "val_type_id": SQL_C_TYPE_DATE

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
		switch (val_type_id) {
		case LogicalTypeId::TIME:
			time = val.Get<dtime_t>();
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS: {
			if (!CastTimestampValue(hstmt, val, val_type_id, time)) {
				return SQL_ERROR;
			}
			break;
		}
		case LogicalTypeId::VARCHAR: {
			duckdb_odbc::unique_ptr<Value> time_val;
			if (!CastStringValue(hstmt, val, LogicalTypeId::TIME, time_val)) {
				return SQL_ERROR;
			}
			time = time_val->Get<dtime_t>();
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::TIME, hstmt);
		} // end switch "val_type_id": SQL_C_TYPE_TIME

		SQL_TIME_STRUCT *time_struct = (SQL_TIME_STRUCT *)target_value_ptr;
		int32_t hour, minute, second, micros;
		Time::Convert(time, hour, minute, second, micros);

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
		switch (val_type_id) {
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS: {
			try {
				timestamp = GetTimestamp(val, val_type_id);
			} catch (std::exception &ex) {
				return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult",
				                                        duckdb_odbc::RawErrorMessage(ex), SQLStateType::ST_22007,
				                                        hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		case LogicalTypeId::DATE: {
			auto date_input = val.Get<date_t>();
			if (!Timestamp::TryFromDate(date_input, timestamp)) {
				auto msg = "Unimplemented type for cast (DATE -> TIMESTAMP)";
				return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", msg,
				                                        SQLStateType::ST_07006, hstmt->dbc->GetDataSourceName());
			}
			break;
		}
		case LogicalTypeId::TIME: {
			auto time_input = val.Get<dtime_t>();
			// PyODBC requests DB TIME field as SQL_TYPE_TIMESTAMP, but the
			// cast from dtime_t to timestamp_t is not implemented in the
			// engine, so we create timestamp_t instance manually.
			timestamp = timestamp_t {time_input.micros};
			break;
		}
		case LogicalTypeId::VARCHAR: {
			duckdb_odbc::unique_ptr<Value> timestamp_val;
			if (!CastStringValue(hstmt, val, LogicalTypeId::TIMESTAMP, timestamp_val)) {
				return SQL_ERROR;
			}
			timestamp = timestamp_val->Get<timestamp_t>();
			break;
		}
		default:
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::TIMESTAMP, hstmt);
		} // end switch "val_type_id"

		if (!Timestamp::IsFinite(timestamp.micros)) {
			return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult",
			                                        "Can't get TIME of infinite TIMESTAMP", SQLStateType::ST_22007,
			                                        hstmt->dbc->GetDataSourceName());
		}
		SQL_TIMESTAMP_STRUCT *timestamp_struct = (SQL_TIMESTAMP_STRUCT *)target_value_ptr;
		int32_t year, month, day, hour, minute, second, micros;
		Timestamp::Convert(timestamp, year, month, day, hour, minute, second, micros);
		timestamp_struct->year = year;
		timestamp_struct->month = month;
		timestamp_struct->day = day;
		timestamp_struct->hour = hour;
		timestamp_struct->minute = minute;
		timestamp_struct->second = second;
		timestamp_struct->fraction = micros * 1000;
		if (str_len_or_ind_ptr) {
			*str_len_or_ind_ptr = sizeof(SQL_TIMESTAMP_STRUCT);
		}

		return SQL_SUCCESS;
	}
	case SQL_C_INTERVAL_YEAR: {
		interval_t interval;
		if (!OdbcInterval::GetInterval(val, interval, hstmt)) {
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
		}

		SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
		OdbcInterval::SetYear(interval, interval_struct);
		// fraction of years stored as months
		interval_struct->intval.year_month.month = std::abs(interval.months) % duckdb_odbc::Interval::MONTHS_PER_YEAR;
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
			return ThrowInvalidCast("GetDataStmtResult", val, LogicalTypeId::INTERVAL, hstmt);
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
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "GetDataStmtResult", "Data type not supported",
		                                        SQLStateType::ST_HY004, hstmt->dbc->GetDataSourceName());
	} // end switch "(target_type_resolved)": SQL_C_TYPE_TIMESTAMP
}

SQLRETURN duckdb_odbc::ExecDirectStmt(OdbcHandleStmt *hstmt, const std::string &query) {
	bool success_with_info = false;
	PrepareQuery(hstmt);

	// Parse all the statements before running any of them
	vector<unique_ptr<cxx::SqlStatement>> statements;
	try {
		auto iterator = hstmt->dbc->conn->ParseSQL(query);
		while (true) {
			auto statement = iterator.Next();
			if (!statement) {
				break;
			}
			statements.push_back(make_uniq<cxx::SqlStatement>(std::move(statement)));
		}
	} catch (std::exception &ex) {
		return duckdb_odbc::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLExecDirect", ex.what(), SQLStateType::ST_42000,
		                                        hstmt->dbc->GetDataSourceName());
	}

	SQLRETURN ret = SQL_SUCCESS;
	for (auto &statement : statements) {
		hstmt->stmt = PrepareStatement(hstmt, std::move(statement), false);
		ret = FinalizeStmt(hstmt);
		if (hstmt->stmt->HasError() || !SQL_SUCCEEDED(ret)) {
			return ret;
		} else if (ret == SQL_SUCCESS_WITH_INFO) {
			success_with_info = true;
		}

		ret = duckdb_odbc::BatchExecuteStmt(hstmt);
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		} else if (ret == SQL_SUCCESS_WITH_INFO) {
			success_with_info = true;
		}
	}
	return success_with_info ? SQL_SUCCESS_WITH_INFO : ret;
}

SQLRETURN duckdb_odbc::PrepareStmt(OdbcHandleStmt *hstmt, const std::string &query) {
	PrepareQuery(hstmt);

	vector<unique_ptr<cxx::SqlStatement>> statements;
	try {
		auto iterator = hstmt->dbc->conn->ParseSQL(query);
		while (true) {
			auto statement = iterator.Next();
			if (!statement) {
				break;
			}
			statements.push_back(make_uniq<cxx::SqlStatement>(std::move(statement)));
		}
		if (statements.size() != 1) {
			throw std::runtime_error(statements.empty() ? "No statement to prepare!"
			                                            : "Cannot prepare multiple statements at once!");
		}
	} catch (std::exception &ex) {
		hstmt->stmt = make_uniq<OdbcPreparedStatement>();
		hstmt->stmt->error = ErrorMessage(ex);
		return FinalizeStmt(hstmt);
	}

	hstmt->stmt = PrepareStatement(hstmt, std::move(statements[0]), true);
	return FinalizeStmt(hstmt);
}

SQLRETURN duckdb_odbc::BindParameterStmt(OdbcHandleStmt *hstmt, SQLUSMALLINT parameter_number,
                                         SQLSMALLINT input_output_type, SQLSMALLINT value_type,
                                         SQLSMALLINT parameter_type, SQLULEN column_size, SQLSMALLINT decimal_digits,
                                         SQLPOINTER parameter_value_ptr, SQLLEN buffer_length,
                                         SQLLEN *str_len_or_ind_ptr) {
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
	// the precision field is a SQLSMALLINT: a column size that does not fit, such as the width of a wide DECIMAL, must
	// not wrap around to a small precision
	ipd_record->sql_desc_precision = ApiInfo::SaturateToSmallInt(column_size);
	ipd_record->sql_desc_scale = decimal_digits;

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

SQLRETURN duckdb_odbc::CloseStmt(duckdb_odbc::OdbcHandleStmt *hstmt) {
	hstmt->Close();
	return SQL_SUCCESS;
}
