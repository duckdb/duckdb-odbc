#include "connect.hpp"

#include "duckdb_odbc.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

using duckdb_odbc::OdbcUtils;

static SQLRETURN ConvertDBCBeforeConnection(SQLHDBC connection_handle, duckdb_odbc::OdbcHandleDbc *&dbc) {
	if (!connection_handle) {
		return SQL_INVALID_HANDLE;
	}
	dbc = static_cast<duckdb_odbc::OdbcHandleDbc *>(connection_handle);
	if (dbc->type != duckdb_odbc::OdbcHandleType::DBC) {
		return SQL_INVALID_HANDLE;
	}
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLConnect
//===--------------------------------------------------------------------===//

static SQLRETURN ConnectInternal(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                                 SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                                 SQLSMALLINT name_length3) {
	duckdb_odbc::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	std::string server_name_st = OdbcUtils::ConvertSQLCHARToString(server_name, name_length1);
	dbc->dsn = server_name_st;
	duckdb_odbc::Connect connect(dbc, server_name_st);

	return connect.SetConnection();
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlconnect-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                             SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                             SQLSMALLINT name_length3) {
	return ConnectInternal(connection_handle, server_name, name_length1, user_name, name_length2, authentication,
	                       name_length3);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlconnect-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLConnectW(SQLHDBC connection_handle, SQLWCHAR *server_name, SQLSMALLINT name_length1,
                              SQLWCHAR *user_name, SQLSMALLINT name_length2, SQLWCHAR *authentication,
                              SQLSMALLINT name_length3) {
	auto server_name_conv = duckdb_odbc::widechar::utf16_conv(server_name, name_length1);
	auto user_name_conv = duckdb_odbc::widechar::utf16_conv(user_name, name_length2);
	auto authentication_conv = duckdb_odbc::widechar::utf16_conv(authentication, name_length3);
	return ConnectInternal(connection_handle, server_name_conv.utf8_str, server_name_conv.utf8_len_smallint(),
	                       user_name_conv.utf8_str, user_name_conv.utf8_len_smallint(), authentication_conv.utf8_str,
	                       authentication_conv.utf8_len_smallint());
}

//===--------------------------------------------------------------------===//
// SQLDriverConnect
//===--------------------------------------------------------------------===//

static SQLRETURN DriverConnectInternal(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                       SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                       SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                       SQLUSMALLINT driver_completion) {
	duckdb_odbc::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	const std::string in_conn_str = OdbcUtils::ConvertSQLCHARToString(in_connection_string, string_length1);
	duckdb_odbc::Connect connect(dbc, in_conn_str);

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
		                              static_cast<size_t>(buffer_length), "%s", in_conn_str.c_str());
		if (string_length2_ptr != nullptr) {
			*string_length2_ptr = static_cast<SQLSMALLINT>(available);
		}
	} else if (string_length2_ptr != nullptr) {
		*string_length2_ptr = static_cast<SQLSMALLINT>(in_conn_str.length());
	}
	return connect.GetSuccessWithInfo() ? SQL_SUCCESS_WITH_INFO : ret;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                   SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                   SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                   SQLUSMALLINT driver_completion) {
	return DriverConnectInternal(connection_handle, window_handle, in_connection_string, string_length1,
	                             out_connection_string, buffer_length, string_length2_ptr, driver_completion);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC connection_handle, SQLHWND window_handle, SQLWCHAR *in_connection_string,
                                    SQLSMALLINT string_length1, SQLWCHAR *out_connection_string,
                                    SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                    SQLUSMALLINT driver_completion) {
	auto in_connection_string_conv = duckdb_odbc::widechar::utf16_conv(in_connection_string, string_length1);
	auto out_connection_string_vec = duckdb_odbc::widechar::utf8_alloc_out_vec(buffer_length);
	auto ret =
	    DriverConnectInternal(connection_handle, window_handle, in_connection_string_conv.utf8_str,
	                          in_connection_string_conv.utf8_len_smallint(), out_connection_string_vec.data(),
	                          static_cast<SQLSMALLINT>(out_connection_string_vec.size()), nullptr, driver_completion);
	size_t written =
	    duckdb_odbc::widechar::utf16_write_str(ret, out_connection_string_vec, out_connection_string, buffer_length);
	if (string_length2_ptr != nullptr) {
		*string_length2_ptr = static_cast<SQLSMALLINT>(written);
	}
	return ret;
}

//===--------------------------------------------------------------------===//
// SQLDisconnect
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldisconnect-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDisconnect(SQLHDBC connection_handle) {
	duckdb_odbc::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(connection_handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	// what the statements hold of the connection must not outlive it
	for (auto stmt : dbc->vec_stmt_ref) {
		stmt->Close();
		stmt->stmt.reset();
	}
	dbc->conn.reset();
	dbc->db.reset();
	return SQL_SUCCESS;
}
