#include "connect.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <iostream>
#include <utility>

#if WIN32
#include <windows.h>
#endif

using namespace duckdb;

//! The database instance cache, used so that multiple connections to the same file point to the same database object
DBInstanceCache instance_cache;

const std::vector<std::string> Connect::IGNORE_KEYS = {
    "driver",             // can be used by any client connecting without DSN
    "trusted_connection", // set by Power Query Connector by MotherDuck
    "uid",                // used by default for ODBC sources in Excel
    "pwd",                // likewise uid
};

bool Connect::SetSuccessWithInfo(SQLRETURN ret) {
	if (!SQL_SUCCEEDED(ret)) {
		return false;
	}
	if (ret == SQL_SUCCESS_WITH_INFO) {
		success_with_info = true;
	}
	return true;
}

SQLRETURN Connect::FindMatchingKey(const std::string &input, string &key) {
	if (seen_config_options.find(input) != seen_config_options.end()) {
		key = input;
		return SQL_SUCCESS;
	}

	auto config_names = DBConfig::GetOptionNames();

	// If the input doesn't match a keyname, find a similar keyname
	auto msg = StringUtil::CandidatesErrorMessage(config_names, input, "Did you mean: ");
	return SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect",
	                           "Invalid keyword: '" + input + "'. " + msg, SQLStateType::ST_01S09, "");
}

SQLRETURN Connect::FindKeyValPair(const std::string &row) {
	string key;

	size_t val_pos = row.find(KEY_VAL_DEL);
	if (val_pos == std::string::npos) {
		// an equal '=' char must be present (syntax error)
		return (SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Invalid connection string",
		                            SQLStateType::ST_HY000, ""));
	}

	std::string key_candidate = StringUtil::Lower(row.substr(0, val_pos));

	// Check if the key can be ignored
	if (std::find(IGNORE_KEYS.begin(), IGNORE_KEYS.end(), key_candidate) != IGNORE_KEYS.end()) {
		return SQL_SUCCESS;
	}

	SQLRETURN ret = FindMatchingKey(key_candidate, key);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	config_map[key] = row.substr(val_pos + 1);
	seen_config_options[key] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::ParseInputStr() {
	size_t row_pos;
	std::string row;

	if (input_str.empty()) {
		return SQL_SUCCESS;
	}

	while ((row_pos = input_str.find(ROW_DEL)) != std::string::npos) {
		row = input_str.substr(0, row_pos);
		SQLRETURN ret = FindKeyValPair(row);
		if (!SetSuccessWithInfo(ret)) {
			return ret;
		}
		input_str.erase(0, row_pos + 1);
	}

	if (!input_str.empty()) {
		SQLRETURN ret = FindKeyValPair(input_str);
		if (!SetSuccessWithInfo(ret)) {
			return ret;
		}
	}

	// Extract the DSN from the config map as it is needed to read from the .odbc.ini file
	dbc->dsn = seen_config_options["dsn"] ? config_map["dsn"].ToString() : "";

	return GetSuccessWithInfo() ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN Connect::ReadFromIniFile() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	if (dbc->dsn.empty()) {
		return SQL_SUCCESS;
	}

	auto converted_dsn = OdbcUtils::ConvertStringToLPCSTR(dbc->dsn);
	for (auto &key_pair : seen_config_options) {
		// if the key has already been set, skip it
		if (key_pair.second) {
			continue;
		}
		const idx_t max_val_len = 256;
		char char_val[max_val_len];
		auto converted_key = key_pair.first.c_str();
		int read_size = SQLGetPrivateProfileString(converted_dsn, converted_key, "", char_val, max_val_len, "odbc.ini");
		if (read_size == 0) {
			continue;
		} else if (read_size < 0) {
			return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Error reading from .odbc.ini",
			                           SQLStateType::ST_01S09, "");
		}
		config_map[key_pair.first] = std::string(char_val);
		seen_config_options[key_pair.first] = true;
	}
#endif
	return SQL_SUCCESS;
}

static void NormalizeWindowsPathSeparators(case_insensitive_map_t<duckdb::Value> &config_map,
                                           const std::string &option_name) {
	auto it = config_map.find(option_name);
	if (it == config_map.end()) {
		return;
	}
	auto value_str = it->second.GetValue<std::string>();
	auto value_str_norm = duckdb::StringUtil::Replace(value_str, "\\", "/");
	config_map.erase(option_name);
	config_map.emplace(option_name, std::move(value_str_norm));
}

SQLRETURN Connect::SetConnection() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	ReadFromIniFile();
#endif
	auto database = seen_config_options["database"] ? config_map["database"].ToString() : IN_MEMORY_PATH;
	dbc->SetDatabaseName(database);

	// remove the database and dsn from the config map since they aren't config options
	config_map.erase("database");
	config_map.erase("dsn");

	config.SetOptionByName("duckdb_api", "odbc");

	// When 'enable_external_access' is set to 'false' then it is not allowed to change
	// 'allowed_paths' or 'allowed_directories' options, so we are setting 'enable_external_access'
	// after all other options
	std::string enable_external_access;
	auto it = config_map.find("enable_external_access");
	if (it != config_map.end()) {
		enable_external_access = it->second.GetValue<std::string>();
		config_map.erase(it);
	}

#ifdef _WIN32
	// Back slashes are not accepted for 'allowed_paths' or 'allowed_directories' options
	NormalizeWindowsPathSeparators(config_map, "allowed_paths");
	NormalizeWindowsPathSeparators(config_map, "allowed_directories");
#endif // _WIN32

	try {
		// Validate and set all options
		config.SetOptionsByName(config_map);

		if (!enable_external_access.empty()) {
			config.SetOptionByName("enable_external_access", duckdb::Value(enable_external_access));
		}

		bool cache_instance = database != IN_MEMORY_PATH;

		dbc->env->db = instance_cache.GetOrCreateInstance(database, config, cache_instance);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", error.Message(), SQLStateType::ST_IM003, "");
	}

	if (!dbc->conn) {
		dbc->conn = make_uniq<Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

Connect::Connect(OdbcHandleDbc *dbc_p, string input_str_p) : dbc(dbc_p), input_str(std::move(input_str_p)) {
	// Get all the config options
	auto config_options = DBConfig::GetOptionNames();
	for (auto &option : config_options) {
		// They are all set to false as they haven't been set yet
		seen_config_options[option] = false;
	}
	seen_config_options["dsn"] = false;
	seen_config_options["database"] = false;

	// Required for settings like 'allowed_directories' that use
	// file separator when checking the property value.
	config.file_system = duckdb::make_uniq<duckdb::VirtualFileSystem>();
}
