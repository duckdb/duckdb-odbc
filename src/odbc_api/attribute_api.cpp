#include "driver.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "handle_functions.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb_odbc.hpp"

#include <locale>

using duckdb::LogicalTypeId;
using duckdb::OdbcDiagnostic;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;

// Set by MS Access (Jet engine)
#define VENDOR_MSJET 30002

//===--------------------------------------------------------------------===//
// SQLGetEnvAttr
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetenvattr-function?view=sql-server-ver16">Docs</a>
 */
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

//===--------------------------------------------------------------------===//
// SQLSetEnvAttr
//===--------------------------------------------------------------------===//

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

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetenvattr-function?view=sql-server-ver16">Docs</a>
 */
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

static SQLRETURN WriteStringConnAttr(duckdb::OdbcHandleDbc *dbc, const std::string &str,
                                     SQLCHAR *character_attribute_ptr, SQLINTEGER buffer_length,
                                     SQLINTEGER *string_length) {
	if (buffer_length < 0) {
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLGetConnectAttr", "Invalid string or buffer length",
		                                   SQLStateType::ST_HY090, dbc->GetDataSourceName());
	}

	size_t written =
	    duckdb::OdbcUtils::WriteStringUtf8(str, character_attribute_ptr, static_cast<size_t>(buffer_length));

	if (string_length != nullptr) {
		*string_length = static_cast<SQLINTEGER>(str.length());
	}

	if (character_attribute_ptr != nullptr && written < str.length()) {
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLGetConnectAttr",
		                                   "String data, right truncated", SQLStateType::ST_01004,
		                                   dbc->GetDataSourceName());
	}

	return SQL_SUCCESS;
}

static SQLRETURN WriteStringConnAttr(duckdb::OdbcHandleDbc *dbc, const std::string &str,
                                     SQLWCHAR *character_attribute_ptr, SQLINTEGER buffer_length,
                                     SQLINTEGER *string_length) {
	if (buffer_length < 0) {
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLGetConnectAttr", "Invalid string or buffer length",
		                                   SQLStateType::ST_HY090, dbc->GetDataSourceName());
	}

	auto tup = duckdb::OdbcUtils::WriteStringUtf16(str, character_attribute_ptr, static_cast<size_t>(buffer_length));
	size_t written = tup.first;
	auto &utf16_vec = tup.second;

	if (string_length != nullptr) {
		*string_length = static_cast<SQLINTEGER>(utf16_vec.size());
	}

	if (character_attribute_ptr != nullptr && written < utf16_vec.size()) {
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLGetConnectAttr",
		                                   "String data, right truncated", SQLStateType::ST_01004,
		                                   dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLGetConnectAttr
//===--------------------------------------------------------------------===//

template <typename CHAR_TYPE>
static SQLRETURN GetConnectAttrInternal(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                        SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(connection_handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_AUTOCOMMIT: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER, SQLINTEGER>(dbc->autocommit, value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ACCESS_MODE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER, SQLINTEGER>(dbc->sql_attr_access_mode, value_ptr,
		                                                            string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURRENT_CATALOG: {
		return WriteStringConnAttr(dbc, dbc->sql_attr_current_catalog, reinterpret_cast<CHAR_TYPE *>(value_ptr),
		                           buffer_length, string_length_ptr);
	}
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
	case SQL_ATTR_ASYNC_DBC_EVENT:
#endif
	case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
	case SQL_ATTR_ASYNC_DBC_PCALLBACK:
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
	case SQL_ATTR_ASYNC_DBC_PCONTEXT:
#endif
	case SQL_ATTR_ASYNC_ENABLE:
	case SQL_ATTR_AUTO_IPD:
	case SQL_ATTR_CONNECTION_DEAD:
	case SQL_ATTR_CONNECTION_TIMEOUT:
#ifdef SQL_ATTR_DBC_INFO_TOKEN
	case SQL_ATTR_DBC_INFO_TOKEN:
#endif
	case SQL_ATTR_ENLIST_IN_DTC:
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
	case SQL_ATTR_TRANSLATE_OPTION:
		return SQL_NO_DATA;
	case SQL_ATTR_QUERY_TIMEOUT: {
		duckdb::OdbcUtils::StoreWithLength<SQLINTEGER, SQLINTEGER>(0, value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_TXN_ISOLATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER, SQLINTEGER>(SQL_TXN_SERIALIZABLE, value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	default:
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLGetConnectAttr", "Attribute not supported.",
		                                   SQLStateType::ST_HY092, dbc->GetDataSourceName());
	}
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetconnectattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return GetConnectAttrInternal<SQLCHAR>(connection_handle, attribute, value_ptr, buffer_length, string_length_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetconnectattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                     SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	SQLRETURN ret =
	    GetConnectAttrInternal<SQLWCHAR>(connection_handle, attribute, value_ptr, buffer_length, string_length_ptr);
	if (SQL_SUCCEEDED(ret) && string_length_ptr != nullptr) {
		*string_length_ptr = static_cast<SQLINTEGER>(*string_length_ptr * sizeof(SQLWCHAR));
	}
	return ret;
}

//===--------------------------------------------------------------------===//
// SQLSetConnectAttr
//===--------------------------------------------------------------------===//

static void SetCurrentCatalog(duckdb::OdbcHandleDbc *dbc, SQLCHAR *value, SQLINTEGER length) {
	const char *val_chars = reinterpret_cast<char *>(value);
	size_t val_len = length != SQL_NTS ? static_cast<size_t>(length) : std::strlen(val_chars);
	dbc->sql_attr_current_catalog = std::string(val_chars, val_len);
}

static void SetCurrentCatalog(duckdb::OdbcHandleDbc *dbc, SQLWCHAR *value, SQLINTEGER length) {
	auto value_conv = duckdb::widechar::utf16_conv(value, length);
	dbc->sql_attr_current_catalog = value_conv.to_utf8_str();
}

template <typename CHAR_TYPE>
static SQLRETURN SetConnectAttrInternal(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                        SQLINTEGER string_length) {
	// attributes before connection
	switch (attribute) {
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case VENDOR_MSJET:
		return SQL_SUCCESS;
	default:
		break;
	}

	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(connection_handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_AUTOCOMMIT:
		switch ((ptrdiff_t)value_ptr) {
		case (ptrdiff_t)SQL_AUTOCOMMIT_ON:
			dbc->autocommit = true;
			dbc->conn->SetAutoCommit(true);
			return SQL_SUCCESS;
		case (ptrdiff_t)SQL_AUTOCOMMIT_OFF:
			dbc->autocommit = false;
			dbc->conn->SetAutoCommit(false);
			return SQL_SUCCESS;
		case SQL_ATTR_METADATA_ID: {
			if (value_ptr) {
				dbc->sql_attr_metadata_id = OdbcUtils::SQLPointerToSQLUInteger(value_ptr);
				return SQL_SUCCESS;
			}
		}
		default:
			return SQL_SUCCESS;
		}
	case SQL_ATTR_ACCESS_MODE: {
		auto access_mode = OdbcUtils::SQLPointerToSQLUInteger(value_ptr);
		switch (access_mode) {
		case SQL_MODE_READ_WRITE:
			dbc->sql_attr_access_mode = SQL_MODE_READ_WRITE;
			return SQL_SUCCESS;
		case SQL_MODE_READ_ONLY:
			dbc->sql_attr_access_mode = SQL_MODE_READ_ONLY;
			return SQL_SUCCESS;
		}
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLSetConnectAttr", "Invalid access mode.",
		                                   SQLStateType::ST_HY024, dbc->GetDataSourceName());
	}
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
	case SQL_ATTR_ASYNC_DBC_EVENT:
#endif
	case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
	case SQL_ATTR_ASYNC_DBC_PCALLBACK:
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
	case SQL_ATTR_ASYNC_DBC_PCONTEXT:
#endif
	case SQL_ATTR_ASYNC_ENABLE: {
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLSetConnectAttr",
		                                   "DuckDB does not support asynchronous events.", SQLStateType::ST_HY024,
		                                   dbc->GetDataSourceName());
	}
	case SQL_ATTR_AUTO_IPD:
	case SQL_ATTR_CONNECTION_DEAD: {
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLSetConnectAttr", "Read-only attribute.",
		                                   SQLStateType::ST_HY092, dbc->GetDataSourceName());
	}
	case SQL_ATTR_CONNECTION_TIMEOUT:
		return SQL_SUCCESS;
	case SQL_ATTR_CURRENT_CATALOG: {
		if (dbc->conn) {
			return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLSetConnectAttr",
			                                   "Connection already established, the database name could not be set.",
			                                   SQLStateType::ST_01S00, dbc->GetDataSourceName());
		}
		SetCurrentCatalog(dbc, reinterpret_cast<CHAR_TYPE *>(value_ptr), string_length);
		return SQL_SUCCESS;
	}
#ifdef SQL_ATTR_DBC_INFO_TOKEN
	case SQL_ATTR_DBC_INFO_TOKEN:
#endif
	case SQL_ATTR_ENLIST_IN_DTC:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
	case SQL_ATTR_TRANSLATE_OPTION:
	case SQL_ATTR_TXN_ISOLATION: {
		return SQL_SUCCESS;
	}
	default:
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLSetConnectAttr",
		                                   "Option value changed:" + std::to_string(attribute), SQLStateType::ST_01S02,
		                                   dbc->GetDataSourceName());
	}
}

/**
 * @attention <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER string_length) {
	return SetConnectAttrInternal<SQLCHAR>(connection_handle, attribute, value_ptr, string_length);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                     SQLINTEGER string_length) {
	return SetConnectAttrInternal<SQLWCHAR>(connection_handle, attribute, value_ptr, string_length);
}

//===--------------------------------------------------------------------===//
// SQLGetStmtAttr
//===--------------------------------------------------------------------===//

static SQLRETURN GetStmtAttrInternal(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                     SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_APP_PARAM_DESC:
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_APP_ROW_DESC:
	case SQL_ATTR_IMP_ROW_DESC: {
		if (string_length_ptr) {
			*string_length_ptr = SQL_IS_POINTER;
		}
		SQLHDESC *desc_handle = reinterpret_cast<SQLHDESC *>(value_ptr);
		if (attribute == SQL_ATTR_APP_PARAM_DESC) {
			*desc_handle = hstmt->param_desc->GetAPD();
		}
		if (attribute == SQL_ATTR_IMP_PARAM_DESC) {
			*desc_handle = hstmt->param_desc->GetIPD();
		}
		if (attribute == SQL_ATTR_APP_ROW_DESC) {
			*desc_handle = hstmt->row_desc->GetARD();
		}
		if (attribute == SQL_ATTR_IMP_ROW_DESC) {
			*desc_handle = hstmt->row_desc->GetIRD();
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*((SQLLEN **)value_ptr) = hstmt->param_desc->GetBindOffesetPtr();
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*((SQLINTEGER *)value_ptr) = hstmt->param_desc->apd->header.sql_desc_bind_type;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMS_PROCESSED_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN **)value_ptr = hstmt->param_desc->GetParamProcessedPtr();
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMSET_SIZE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->param_desc->apd->header.sql_desc_array_size;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_ARRAY_SIZE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->row_desc->ard->header.sql_desc_array_size;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROWS_FETCHED_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN **)value_ptr = hstmt->rows_fetched_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		if ((SQLULEN)value_ptr != SQL_BIND_BY_COLUMN) {
			//! it's a row-wise binding orientation
			*(SQLULEN *)value_ptr = hstmt->row_desc->ard->header.sql_desc_bind_type;
			return SQL_SUCCESS;
		}
		*(SQLULEN *)value_ptr = SQL_BIND_BY_COLUMN;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_STATUS_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLUSMALLINT **)value_ptr = hstmt->row_desc->ird->header.sql_desc_array_status_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->odbc_fetcher->cursor_type;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CONCURRENCY: {
		//*(SQLULEN *)value_ptr = SQL_CONCUR_READ_ONLY;
		*(SQLULEN *)value_ptr = SQL_CONCUR_LOCK;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_QUERY_TIMEOUT: {
		*((SQLINTEGER *)value_ptr) = 0;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_RETRIEVE_DATA: {
		*((SQLULEN *)value_ptr) = hstmt->retrieve_data;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_SCROLLABLE: {
		*((SQLULEN *)value_ptr) = hstmt->odbc_fetcher->cursor_scrollable;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_MAX_LENGTH: {
		*((SQLULEN *)value_ptr) = hstmt->client_attrs.max_len;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ASYNC_ENABLE:
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
	case SQL_ATTR_ASYNC_STMT_EVENT:
#endif
	case SQL_ATTR_CURSOR_SENSITIVITY:
	case SQL_ATTR_ENABLE_AUTO_IPD:
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
	case SQL_ATTR_KEYSET_SIZE:
	case SQL_ATTR_MAX_ROWS:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_NOSCAN:
	case SQL_ATTR_PARAM_OPERATION_PTR:
	case SQL_ATTR_PARAM_STATUS_PTR:
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
	case SQL_ATTR_ROW_NUMBER:
	case SQL_ATTR_ROW_OPERATION_PTR:
	case SQL_ATTR_SIMULATE_CURSOR:
	case SQL_ATTR_USE_BOOKMARKS:
	default:
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetStmtAttr",
		                                   "Unsupported attribute type:" + std::to_string(attribute),
		                                   SQLStateType::ST_HY092, hstmt->dbc->GetDataSourceName());
	}
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetstmtattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return GetStmtAttrInternal(statement_handle, attribute, value_ptr, buffer_length, string_length_ptr);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetstmtattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetStmtAttrW(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                  SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return GetStmtAttrInternal(statement_handle, attribute, value_ptr, buffer_length, string_length_ptr);
}

//===--------------------------------------------------------------------===//
// SQLSetStmtAtr
//===--------------------------------------------------------------------===//

static SQLRETURN SetStmtAttrInternal(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                     SQLINTEGER string_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_PARAMSET_SIZE: {
		hstmt->param_desc->apd->header.sql_desc_array_size = (SQLULEN)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_TYPE: {
		if (value_ptr) {
			hstmt->param_desc->apd->header.sql_desc_bind_type = (SQLINTEGER)(SQLULEN)(uintptr_t)value_ptr;
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMS_PROCESSED_PTR: {
		hstmt->param_desc->SetParamProcessedPtr((SQLULEN *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_STATUS_PTR: {
		hstmt->param_desc->SetArrayStatusPtr((SQLUSMALLINT *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_ARRAY_SIZE: {
		// TODO allow fetch to put more rows in bound cols
		if (value_ptr) {
			SQLULEN new_size = (SQLULEN)value_ptr;
			if (new_size < 1) {
				return SQL_ERROR;
			}
			// This field in the ARD can also be set by calling SQLSetStmtAttr with the SQL_ATTR_ROW_ARRAY_SIZE
			// attribute.
			hstmt->row_desc->ard->header.sql_desc_array_size = new_size;
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROWS_FETCHED_PTR: {
		hstmt->rows_fetched_ptr = (SQLULEN *)value_ptr;
		hstmt->row_desc->ird->header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		hstmt->row_desc->ard->header.sql_desc_bind_type = static_cast<SQLINTEGER>(reinterpret_cast<SQLLEN>(value_ptr));
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_STATUS_PTR: {
		hstmt->row_desc->ird->header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_TYPE: {
		hstmt->odbc_fetcher->cursor_type = (SQLULEN)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_APP_ROW_DESC: {
		hstmt->SetARD((duckdb::OdbcHandleDesc *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_APP_PARAM_DESC: {
		hstmt->SetAPD((duckdb::OdbcHandleDesc *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_IMP_ROW_DESC: {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
		                                   "Option value changed:" + std::to_string(attribute), SQLStateType::ST_HY017,
		                                   hstmt->dbc->GetDataSourceName());
	}
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
		hstmt->param_desc->SetBindOffesetPtr((SQLLEN *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CONCURRENCY: {
		SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
		if (value != SQL_CONCUR_LOCK) {
			return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLSetStmtAttr",
			                                   "Option value changed:" + std::to_string(attribute),
			                                   SQLStateType::ST_01S02, hstmt->dbc->GetDataSourceName());
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_QUERY_TIMEOUT:
		return SQL_SUCCESS;
	case SQL_ATTR_RETRIEVE_DATA: {
		SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
		switch (value) {
		case SQL_RD_ON:
		case SQL_RD_OFF:
			break;
		default:
			/* Invalid attribute value */
			return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
			                                   "Invalid attribute value: " + std::to_string(attribute),
			                                   SQLStateType::ST_HY024, hstmt->dbc->GetDataSourceName());
		}
		hstmt->retrieve_data = value;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_SCROLLABLE: {
		SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
		switch (value) {
		case SQL_NONSCROLLABLE:
			hstmt->odbc_fetcher->cursor_type = SQL_CURSOR_FORWARD_ONLY;
			break;
		case SQL_SCROLLABLE:
			hstmt->odbc_fetcher->cursor_type = SQL_CURSOR_STATIC;
			break;
		default:
			return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
			                                   "Invalid attribute value:" + std::to_string(attribute),
			                                   SQLStateType::ST_HY024, hstmt->dbc->GetDataSourceName());
		}
		hstmt->odbc_fetcher->cursor_scrollable = value;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_MAX_LENGTH: {
		hstmt->client_attrs.max_len = (SQLULEN)(uintptr_t)value_ptr;
		return SQL_SUCCESS;
	}
	default:
		return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLSetStmtAttr",
		                                   "Option value changed:" + std::to_string(attribute), SQLStateType::ST_01S02,
		                                   hstmt->dbc->GetDataSourceName());
	}

	return SQL_SUCCESS;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER string_length) {
	return SetStmtAttrInternal(statement_handle, attribute, value_ptr, string_length);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                  SQLINTEGER string_length) {
	return SetStmtAttrInternal(statement_handle, attribute, value_ptr, string_length);
}
