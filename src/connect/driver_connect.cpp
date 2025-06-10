#include "connect.hpp"

#include <cstdio>

#include "duckdb_odbc.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

using duckdb::OdbcUtils;

static SQLRETURN ConvertDBCBeforeConnection(SQLHDBC connection_handle, duckdb::OdbcHandleDbc *&dbc) {
	if (!connection_handle) {
		return SQL_INVALID_HANDLE;
	}
	dbc = static_cast<duckdb::OdbcHandleDbc *>(connection_handle);
	if (dbc->type != duckdb::OdbcHandleType::DBC) {
		return SQL_INVALID_HANDLE;
	}
	return SQL_SUCCESS;
}

static SQLRETURN DriverConnectInternal(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                       SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                       SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                       SQLUSMALLINT driver_completion) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	const std::string in_conn_str = OdbcUtils::ConvertSQLCHARToString(in_connection_string, string_length1);
	duckdb::Connect connect(dbc, in_conn_str);

	ret = connect.ParseInputStr();
	if (!connect.SetSuccessWithInfo(ret)) {
		return ret;
	}

	ret = connect.SetConnection();
	if (!connect.SetSuccessWithInfo(ret)) {
		return ret;
	}

	if (out_connection_string != nullptr) {
		int available = std::snprintf(reinterpret_cast<char *>(out_connection_string),
		                              static_cast<std::size_t>(buffer_length), "%s", in_conn_str.c_str());
		if (string_length2_ptr != nullptr) {
			*string_length2_ptr = static_cast<SQLSMALLINT>(available);
		}
	} else if (string_length2_ptr != nullptr) {
		*string_length2_ptr = static_cast<SQLSMALLINT>(in_conn_str.length());
	}
	return connect.GetSuccessWithInfo() ? SQL_SUCCESS_WITH_INFO : ret;
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                   SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                   SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                   SQLUSMALLINT driver_completion) {
	return DriverConnectInternal(connection_handle, window_handle, in_connection_string, string_length1,
	                             out_connection_string, buffer_length, string_length2_ptr, driver_completion);
}

SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC connection_handle, SQLHWND window_handle, SQLWCHAR *in_connection_string,
                                    SQLSMALLINT string_length1, SQLWCHAR *out_connection_string,
                                    SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                    SQLUSMALLINT driver_completion) {
	auto in_connection_string_conv = duckdb::widechar::utf16_conv(in_connection_string, string_length1);
	auto out_connection_string_vec = duckdb::widechar::utf8_alloc_out_vec(buffer_length);
	auto ret = DriverConnectInternal(connection_handle, window_handle, in_connection_string_conv.utf8_str,
	                                 in_connection_string_conv.utf8_len_smallint(), out_connection_string_vec.data(),
	                                 static_cast<SQLSMALLINT>(out_connection_string_vec.size()), string_length2_ptr,
	                                 driver_completion);
	duckdb::widechar::utf16_write_str(ret, out_connection_string_vec, out_connection_string, buffer_length,
	                                  string_length2_ptr);
	return ret;
}

static SQLRETURN ConnectInternal(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                                 SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                                 SQLSMALLINT name_length3) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	std::string server_name_st = OdbcUtils::ConvertSQLCHARToString(server_name, name_length1);
	dbc->dsn = server_name_st;
	duckdb::Connect connect(dbc, server_name_st);

	return connect.SetConnection();
}

SQLRETURN SQL_API SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                             SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                             SQLSMALLINT name_length3) {
	return ConnectInternal(connection_handle, server_name, name_length1, user_name, name_length2, authentication,
	                       name_length3);
}

SQLRETURN SQL_API SQLConnectW(SQLHDBC connection_handle, SQLWCHAR *server_name, SQLSMALLINT name_length1,
                              SQLWCHAR *user_name, SQLSMALLINT name_length2, SQLWCHAR *authentication,
                              SQLSMALLINT name_length3) {
	auto server_name_conv = duckdb::widechar::utf16_conv(server_name, name_length1);
	auto user_name_conv = duckdb::widechar::utf16_conv(user_name, name_length2);
	auto authentication_conv = duckdb::widechar::utf16_conv(authentication, name_length3);
	return ConnectInternal(connection_handle, server_name_conv.utf8_str, server_name_conv.utf8_len_smallint(),
	                       user_name_conv.utf8_str, user_name_conv.utf8_len_smallint(), authentication_conv.utf8_str,
	                       authentication_conv.utf8_len_smallint());
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC connection_handle) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(connection_handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	dbc->conn.reset();
	return SQL_SUCCESS;
}
