#include "driver.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

#include <locale>

using namespace duckdb;
using duckdb::OdbcDiagnostic;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;

static SQLRETURN WriteStringDiag(const std::string &str, SQLCHAR *character_attribute_ptr, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *string_length) {
	if (buffer_length < 0) {
		return SQL_ERROR;
	}

	size_t written =
	    duckdb::OdbcUtils::WriteStringUtf8(str, character_attribute_ptr, static_cast<size_t>(buffer_length));

	if (string_length != nullptr) {
		*string_length = static_cast<SQLSMALLINT>(str.length());
	}

	if (character_attribute_ptr != nullptr && written < str.length()) {
		return SQL_SUCCESS_WITH_INFO;
	}

	return SQL_SUCCESS;
}

static SQLRETURN WriteStringDiag(const std::string &str, SQLWCHAR *character_attribute_ptr, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *string_length) {
	if (buffer_length < 0) {
		return SQL_ERROR;
	}

	auto tup = duckdb::OdbcUtils::WriteStringUtf16(str, character_attribute_ptr, static_cast<size_t>(buffer_length));
	size_t written = tup.first;
	auto &utf16_vec = tup.second;

	if (string_length != nullptr) {
		*string_length = static_cast<SQLSMALLINT>(utf16_vec.size());
	}

	if (character_attribute_ptr != nullptr && written < utf16_vec.size()) {
		return SQL_SUCCESS_WITH_INFO;
	}
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLGetDiagField
//===--------------------------------------------------------------------===//

template <typename CHAR_TYPE>
static SQLRETURN GetDiagFieldInternal(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                      SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                      SQLSMALLINT *string_length_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_ENV:
	case SQL_HANDLE_DBC:
	case SQL_HANDLE_STMT:
	case SQL_HANDLE_DESC: {
		duckdb::OdbcHandle *hdl = nullptr;
		SQLRETURN ret = ConvertHandle(handle, hdl);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		// diag header fields
		switch (diag_identifier) {
		case SQL_DIAG_CURSOR_ROW_COUNT: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			duckdb::Store<SQLLEN>(hdl->odbc_diagnostic->header.sql_diag_cursor_row_count,
			                      (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_DYNAMIC_FUNCTION: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			return WriteStringDiag(hdl->odbc_diagnostic->GetDiagDynamicFunction(),
			                       reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length, string_length_ptr);
		}
		case SQL_DIAG_DYNAMIC_FUNCTION_CODE: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			duckdb::Store<SQLINTEGER>(hdl->odbc_diagnostic->header.sql_diag_dynamic_function_code,
			                          (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_NUMBER: {
			duckdb::Store<SQLINTEGER>(hdl->odbc_diagnostic->header.sql_diag_number, (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_RETURNCODE: {
			duckdb::Store<SQLRETURN>(hdl->odbc_diagnostic->header.sql_diag_return_code,
			                         (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_ROW_COUNT: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			duckdb::Store<SQLLEN>(hdl->odbc_diagnostic->header.sql_diag_return_code, (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		default:
			break;
		}

		// verify identifier and record index
		if (!OdbcDiagnostic::IsDiagRecordField(diag_identifier)) {
			return SQL_ERROR;
		}
		if (rec_number <= 0) {
			return SQL_ERROR;
		}
		auto rec_idx = rec_number - 1;
		if (!hdl->odbc_diagnostic->VerifyRecordIndex(rec_idx)) {
			return SQL_ERROR;
		}

		auto diag_record = hdl->odbc_diagnostic->GetDiagRecord(rec_idx);

		// diag record fields
		switch (diag_identifier) {
		case SQL_DIAG_CLASS_ORIGIN: {
			return WriteStringDiag(hdl->odbc_diagnostic->GetDiagClassOrigin(rec_idx),
			                       reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length, string_length_ptr);
		}
		case SQL_DIAG_COLUMN_NUMBER: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			duckdb::Store<SQLINTEGER>(diag_record.sql_diag_column_number, (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_CONNECTION_NAME: {
			// we do not support connection names
			return WriteStringDiag("", reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length, string_length_ptr);
		}
		case SQL_DIAG_MESSAGE_TEXT: {
			auto msg = diag_record.GetMessage(buffer_length);
			return WriteStringDiag(msg, reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length, string_length_ptr);
		}
		case SQL_DIAG_NATIVE: {
			duckdb::Store<SQLINTEGER>(diag_record.sql_diag_native, (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_ROW_NUMBER: {
			// this field is available only for statement handles
			if (hdl->type != duckdb::OdbcHandleType::STMT) {
				return SQL_ERROR;
			}
			duckdb::Store<SQLLEN>(diag_record.sql_diag_row_number, (duckdb::data_ptr_t)diag_info_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_SERVER_NAME: {
			return WriteStringDiag(diag_record.sql_diag_server_name, reinterpret_cast<CHAR_TYPE *>(diag_info_ptr),
			                       buffer_length, string_length_ptr);
		}
		case SQL_DIAG_SQLSTATE: {
			return WriteStringDiag(diag_record.sql_diag_sqlstate, reinterpret_cast<CHAR_TYPE *>(diag_info_ptr),
			                       buffer_length, string_length_ptr);
		}
		case SQL_DIAG_SUBCLASS_ORIGIN: {
			return WriteStringDiag(hdl->odbc_diagnostic->GetDiagSubclassOrigin(rec_idx),
			                       reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length, string_length_ptr);
		}
		default:
			return SQL_ERROR;
		}
	}
	default:
		return SQL_ERROR;
	}
}

/**
 * @attention <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdiagfield-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                  SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr) {
	return GetDiagFieldInternal<SQLCHAR>(handle_type, handle, rec_number, diag_identifier, diag_info_ptr, buffer_length,
	                                     string_length_ptr);
}

/**
 * @details Wide char version.
 * @attention <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdiagfield-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                   SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr) {
	SQLRETURN ret = GetDiagFieldInternal<SQLWCHAR>(handle_type, handle, rec_number, diag_identifier, diag_info_ptr,
	                                               buffer_length, string_length_ptr);
	if (SQL_SUCCEEDED(ret) && string_length_ptr != nullptr) {
		*string_length_ptr = static_cast<SQLSMALLINT>(*string_length_ptr * sizeof(SQLWCHAR));
	}
	return ret;
}

//===--------------------------------------------------------------------===//
// SQLGetDiagRec
//===--------------------------------------------------------------------===//
static void WriteStringWithLenInChars(const std::string msg_str, SQLCHAR *message_text, SQLSMALLINT buffer_length_chars,
                                      SQLSMALLINT *text_length_chars_ptr) {
	OdbcUtils::WriteStringUtf8(msg_str, message_text, buffer_length_chars);
	if (text_length_chars_ptr != nullptr) {
		*text_length_chars_ptr = static_cast<SQLSMALLINT>(msg_str.length());
	}
}

static void WriteStringWithLenInChars(const std::string msg_str, SQLWCHAR *message_text,
                                      SQLSMALLINT buffer_length_chars, SQLSMALLINT *text_length_chars_ptr) {
	auto utf16_vec =
	    duckdb::widechar::utf8_to_utf16_lenient(reinterpret_cast<const SQLCHAR *>(msg_str.c_str()), msg_str.length());
	if (buffer_length_chars <= 1) {
		if (message_text != nullptr && buffer_length_chars == 1) {
			message_text[0] = 0;
		}
		if (text_length_chars_ptr != nullptr) {
			*text_length_chars_ptr = 0;
			return;
		}
	}
	size_t len_chars = std::min(utf16_vec.size(), static_cast<size_t>(buffer_length_chars - 1));
	if (message_text != nullptr) {
		std::memcpy(message_text, utf16_vec.data(), len_chars * sizeof(SQLWCHAR));
		message_text[len_chars] = 0;
	}
	if (text_length_chars_ptr != nullptr) {
		*text_length_chars_ptr = static_cast<SQLSMALLINT>(len_chars);
	}
}

template <typename CHAR_TYPE>
static SQLRETURN GetDiagRecInternal(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                    CHAR_TYPE *sql_state, SQLINTEGER *native_error_ptr, CHAR_TYPE *message_text,
                                    SQLSMALLINT buffer_length, SQLSMALLINT *text_length_ptr) {

	// lambda function that writes the diagnostic messages
	std::function<bool(duckdb::OdbcHandle *, duckdb::OdbcHandleType)> is_valid_type_func =
	    [&](duckdb::OdbcHandle *hdl, duckdb::OdbcHandleType target_type) {
		    if (hdl->type != target_type) {
			    std::string msg_str("Handle type " + duckdb::OdbcHandleTypeToString(hdl->type) + " mismatch with " +
			                        duckdb::OdbcHandleTypeToString(target_type));
			    WriteStringWithLenInChars(msg_str, message_text, buffer_length, text_length_ptr);
			    return false;
		    }
		    return true;
	    };

	duckdb::OdbcHandle *hdl = nullptr;
	SQLRETURN ret = ConvertHandle(handle, hdl);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	bool is_valid_type;
	switch (handle_type) {
	case SQL_HANDLE_ENV: {
		is_valid_type = is_valid_type_func(hdl, duckdb::OdbcHandleType::ENV);
		break;
	}
	case SQL_HANDLE_DBC: {
		is_valid_type = is_valid_type_func(hdl, duckdb::OdbcHandleType::DBC);
		break;
	}
	case SQL_HANDLE_STMT: {
		is_valid_type = is_valid_type_func(hdl, duckdb::OdbcHandleType::STMT);
		break;
	}
	case SQL_HANDLE_DESC: {
		is_valid_type = is_valid_type_func(hdl, duckdb::OdbcHandleType::DESC);
		break;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
	if (!is_valid_type) {
		// return SQL_SUCCESS because the error message was written to the message_text
		return SQL_SUCCESS;
	}

	if (rec_number <= 0) {
		WriteStringWithLenInChars("Record number is less than 1", message_text, buffer_length, text_length_ptr);
		return SQL_SUCCESS;
	}
	if (buffer_length < 0) {
		WriteStringWithLenInChars("Buffer length is negative", message_text, buffer_length, text_length_ptr);
		return SQL_SUCCESS;
	}
	if ((size_t)rec_number > hdl->odbc_diagnostic->GetTotalRecords()) {
		return SQL_NO_DATA;
	}

	auto rec_idx = rec_number - 1;
	auto &diag_record = hdl->odbc_diagnostic->GetDiagRecord(rec_idx);

	if (sql_state) {
		SQLSMALLINT *len_out = nullptr;
		WriteStringWithLenInChars(diag_record.sql_diag_sqlstate, sql_state, static_cast<SQLSMALLINT>(6), len_out);
	}
	if (native_error_ptr) {
		duckdb::Store<SQLINTEGER>(diag_record.sql_diag_native, (duckdb::data_ptr_t)native_error_ptr);
	}

	std::string msg = diag_record.GetMessage(buffer_length);

	if (message_text == nullptr) {
		if (text_length_ptr) {
			size_t len = msg.length();
			size_t max_len = std::numeric_limits<SQLSMALLINT>::max();
			*text_length_ptr = static_cast<SQLSMALLINT>(len <= max_len ? len : max_len);
		}
		return SQL_SUCCESS_WITH_INFO;
	}

	WriteStringWithLenInChars(msg, message_text, buffer_length, text_length_ptr);

	if (text_length_ptr) {
		SQLSMALLINT msg_len = 0;
		if (!msg.empty()) {
			if (sizeof(CHAR_TYPE) == sizeof(SQLWCHAR)) {
				auto msg_utf16_vec = duckdb::widechar::utf8_to_utf16_lenient(
				    reinterpret_cast<const SQLCHAR *>(msg.c_str()), msg.length());
				msg_len = static_cast<SQLSMALLINT>(msg_utf16_vec.size());
			} else {
				msg_len = static_cast<SQLSMALLINT>(msg.size());
			}
		}
		SQLSMALLINT remaining_chars = static_cast<SQLSMALLINT>(msg_len - buffer_length);
		if (remaining_chars > 0) {
			// TODO needs to split the diagnostic message
			hdl->odbc_diagnostic->AddNewRecIdx(rec_idx);
			return SQL_SUCCESS_WITH_INFO;
		}
	}

	return SQL_SUCCESS;
}

/**
 * @attention <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdiagrec-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                                SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                                SQLSMALLINT *text_length_ptr) {
	return GetDiagRecInternal(handle_type, handle, rec_number, sql_state, native_error_ptr, message_text, buffer_length,
	                          text_length_ptr);
}

/**
 * @details Wide char version.
 * @attention <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdiagrec-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLWCHAR *sql_state,
                                 SQLINTEGER *native_error_ptr, SQLWCHAR *message_text, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *text_length_ptr) {
	return GetDiagRecInternal(handle_type, handle, rec_number, sql_state, native_error_ptr, message_text, buffer_length,
	                          text_length_ptr);
}
