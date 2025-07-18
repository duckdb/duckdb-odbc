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

SQLRETURN duckdb::FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	if (!handle) {
		return SQL_INVALID_HANDLE;
	}

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		auto *hdl = static_cast<duckdb::OdbcHandleDbc *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		auto *hdl = static_cast<duckdb::OdbcHandleDesc *>(handle);
		hdl->dbc->ResetStmtDescriptors(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_ENV: {
		auto *hdl = static_cast<duckdb::OdbcHandleEnv *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = static_cast<duckdb::OdbcHandleStmt *>(handle);
		hdl->dbc->EraseStmtRef(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}

/**
 * @brief Frees a handle
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfreehandle-function?view=sql-server-ver15
 * @param handle_type
 * @param handle
 * @return SQL return code
 */
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	return duckdb::FreeHandle(handle_type, handle);
}

/**
 * @brief Allocates a handle
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver15
 * @param handle_type Can be SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT, SQL_HANDLE_DESC
 * @param input_handle Handle to associate with the new handle, if applicable
 * @param output_handle_ptr The new handle
 * @return
 */
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT handle_type, SQLHANDLE input_handle, SQLHANDLE *output_handle_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		D_ASSERT(input_handle);

		auto *env = static_cast<duckdb::OdbcHandleEnv *>(input_handle);
		D_ASSERT(env->type == duckdb::OdbcHandleType::ENV);
		*output_handle_ptr = new duckdb::OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_ENV:
		*output_handle_ptr = new duckdb::OdbcHandleEnv();
		return SQL_SUCCESS;
	case SQL_HANDLE_STMT: {
		D_ASSERT(input_handle);
		auto *dbc = static_cast<duckdb::OdbcHandleDbc *>(input_handle);
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleStmt(dbc);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		D_ASSERT(input_handle);
		auto *dbc = static_cast<duckdb::OdbcHandleDbc *>(input_handle);
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleDesc(dbc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}

static SQLUINTEGER ExtractMajorVersion(SQLPOINTER value_ptr) {
	// Values like 380 represent version 3.8, here we extract the major version (3 in this case)
	auto full_version = (SQLUINTEGER)(uintptr_t)value_ptr;
	if (full_version > 100) {
		return full_version / 100;
	}
	if (full_version > 10) {
		return full_version / 10;
	}
	return full_version;
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                SQLINTEGER string_length) {
	duckdb::OdbcHandleEnv *env = nullptr;
	SQLRETURN ret = ConvertEnvironment(environment_handle, env);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_ODBC_VERSION: {
		auto major_version = ExtractMajorVersion(value_ptr);
		switch (major_version) {
		case SQL_OV_ODBC3:
		case SQL_OV_ODBC2:
			// TODO actually do something with this?
			// auto version = (SQLINTEGER)(uintptr_t)value_ptr;
			env->odbc_version = major_version;
			return SQL_SUCCESS;
		default:
			return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLSetEnvAttr",
			                                   "ODBC version not supported: " + std::to_string(major_version),
			                                   SQLStateType::ST_HY092, "");
		}
	}
	case SQL_ATTR_CONNECTION_POOLING: {
		auto pooling = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value_ptr));
		switch (pooling) {
		case SQL_CP_OFF:
		case SQL_CP_ONE_PER_DRIVER:
		case SQL_CP_ONE_PER_HENV:
			env->connection_pooling = pooling;
			return SQL_SUCCESS;
		default:
			return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLSetConnectAttr",
			                                   "Connection pooling not supported: " + std::to_string(attribute),
			                                   SQLStateType::ST_HY092, "");
		}
	}
	case SQL_ATTR_CP_MATCH:
		return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLSetConnectAttr",
		                                   "Optional feature not implemented.", SQLStateType::ST_HY092, "");
	case SQL_ATTR_OUTPUT_NTS: /* SQLINTEGER */ {
		auto output_nts = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value_ptr));
		if (output_nts == SQL_TRUE) {
			env->output_nts = SQL_TRUE;
			return SQL_SUCCESS;
		}
		return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLSetConnectAttr",
		                                   "Optional feature not implemented.  SQL_ATTR_OUTPUT_NTS must be SQL_TRUE",
		                                   SQLStateType::ST_HY092, "");
	}
	default:
		return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLSetEnvAttr", "Invalid attribute value",
		                                   SQLStateType::ST_HY024, "");
	}
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {

	if (value_ptr == nullptr) {
		return SQL_SUCCESS;
	}

	auto *env = static_cast<duckdb::OdbcHandleEnv *>(environment_handle);
	if (env->type != duckdb::OdbcHandleType::ENV) {
		return SQL_INVALID_HANDLE;
	}

	switch (attribute) {
	case SQL_ATTR_ODBC_VERSION:
		*static_cast<SQLINTEGER *>(value_ptr) = env->odbc_version;
		break;
	case SQL_ATTR_CONNECTION_POOLING:
		*static_cast<SQLUINTEGER *>(value_ptr) = env->connection_pooling;
		break;
	case SQL_ATTR_OUTPUT_NTS:
		*static_cast<SQLINTEGER *>(value_ptr) = env->output_nts;
		break;
	case SQL_ATTR_CP_MATCH:
		return duckdb::SetDiagnosticRecord(env, SQL_SUCCESS_WITH_INFO, "SQLGetEnvAttr",
		                                   "Optional feature not implemented.", SQLStateType::ST_HYC00, "");
	}
	return SQL_SUCCESS;
}

static void WriteStringWithLenInChars(const std::string msg_str, SQLCHAR *message_text, SQLSMALLINT buffer_length_chars,
                                      SQLSMALLINT *text_length_chars_ptr) {
	OdbcUtils::WriteString(msg_str, message_text, buffer_length_chars, text_length_chars_ptr);
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

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                                SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                                SQLSMALLINT *text_length_ptr) {
	return GetDiagRecInternal(handle_type, handle, rec_number, sql_state, native_error_ptr, message_text, buffer_length,
	                          text_length_ptr);
}

SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLWCHAR *sql_state,
                                 SQLINTEGER *native_error_ptr, SQLWCHAR *message_text, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *text_length_ptr) {
	return GetDiagRecInternal(handle_type, handle, rec_number, sql_state, native_error_ptr, message_text, buffer_length,
	                          text_length_ptr);
}

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
			duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagDynamicFunction(),
			                               reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
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
			duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagClassOrigin(rec_idx),
			                               reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
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
			duckdb::OdbcUtils::WriteString("", reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_MESSAGE_TEXT: {
			auto msg = diag_record.GetMessage(buffer_length);
			duckdb::OdbcUtils::WriteString(msg, reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
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
			duckdb::OdbcUtils::WriteString(diag_record.sql_diag_server_name,
			                               reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_SQLSTATE: {
			duckdb::OdbcUtils::WriteString(diag_record.sql_diag_sqlstate, reinterpret_cast<CHAR_TYPE *>(diag_info_ptr),
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DIAG_SUBCLASS_ORIGIN: {
			duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagSubclassOrigin(rec_idx),
			                               reinterpret_cast<CHAR_TYPE *>(diag_info_ptr), buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
		}
		default:
			return SQL_ERROR;
		}
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                  SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr) {
	return GetDiagFieldInternal<SQLCHAR>(handle_type, handle, rec_number, diag_identifier, diag_info_ptr, buffer_length,
	                                     string_length_ptr);
}

SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                   SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr) {
	return GetDiagFieldInternal<SQLWCHAR>(handle_type, handle, rec_number, diag_identifier, diag_info_ptr,
	                                      buffer_length, string_length_ptr);
}

static SQLRETURN DataSourcesInternal(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                     SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                     SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	duckdb::OdbcHandleEnv *env = nullptr;
	SQLRETURN ret = ConvertEnvironment(environment_handle, env);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(env, SQL_ERROR, "SQLDataSources", "Driver Manager only function",
	                                   SQLStateType::ST_HY000, "");
}

SQLRETURN SQL_API SQLDataSources(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                 SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                 SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	return DataSourcesInternal(environment_handle, direction, server_name, buffer_length1, name_length1_ptr,
	                           description, buffer_length2, name_length2_ptr);
}

SQLRETURN SQL_API SQLDataSourcesW(SQLHENV environment_handle, SQLUSMALLINT direction, SQLWCHAR *server_name,
                                  SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLWCHAR *description,
                                  SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	auto server_name_conv = duckdb::widechar::utf16_conv(server_name, buffer_length1);
	auto description_conv = duckdb::widechar::utf16_conv(description, buffer_length2);
	return DataSourcesInternal(environment_handle, direction, server_name_conv.utf8_str,
	                           server_name_conv.utf8_len_smallint(), name_length1_ptr, description_conv.utf8_str,
	                           description_conv.utf8_len_smallint(), name_length2_ptr);
}

SQLRETURN SQL_API SQLDrivers(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *driver_description,
                             SQLSMALLINT buffer_length1, SQLSMALLINT *description_length_ptr,
                             SQLCHAR *driver_attributes, SQLSMALLINT buffer_length2,
                             SQLSMALLINT *attributes_length_ptr) {
	duckdb::OdbcHandleEnv *env = nullptr;
	SQLRETURN ret = ConvertEnvironment(environment_handle, env);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(env, SQL_ERROR, "SQLDrivers", "Driver Manager only function",
	                                   SQLStateType::ST_HY000, "");
}
