#ifndef DRIVER_HPP
#define DRIVER_HPP

#include "duckdb_odbc.hpp"

namespace duckdb_odbc {

SQLRETURN FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle);

} // namespace duckdb_odbc

#endif
