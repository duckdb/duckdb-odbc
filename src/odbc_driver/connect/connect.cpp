#include "connect.hpp"

#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/common/virtual_file_system.hpp"

#include <utility>

#include "session_init.hpp"

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
	std::string key;

	size_t val_pos = row.find(KEY_VAL_DEL);
	if (val_pos == std::string::npos) {
		// an equal '=' char must be present (syntax error)
		return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Invalid connection string",
		                           SQLStateType::ST_HY000, "");
	}

	std::string key_candidate_spaces = StringUtil::Lower(row.substr(0, val_pos));
	std::string key_candidate = OdbcUtils::TrimString(key_candidate_spaces);

	// Check if the key can be ignored
	if (std::find(IGNORE_KEYS.begin(), IGNORE_KEYS.end(), key_candidate) != IGNORE_KEYS.end()) {
		return SQL_SUCCESS;
	}

	SQLRETURN ret = FindMatchingKey(key_candidate, key);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (key == SessionInit::SQL_FILE_OPTION || key == SessionInit::SQL_FILE_SHA256_OPTION) {
		return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect",
		                           "Options '" + SessionInit::SQL_FILE_OPTION + "' and '" +
		                               SessionInit::SQL_FILE_SHA256_OPTION +
		                               "' can only be specified in DSN configuration in a file or registry.",
		                           SQLStateType::ST_01S09, "");
	}

	std::string val_spaces = row.substr(val_pos + 1);
	std::string val = OdbcUtils::TrimString(val_spaces);

	config_map[key] = val;
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
	dbc->dsn = GetOptionFromConfigMap("dsn");

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

SQLRETURN Connect::SetConnection() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	ReadFromIniFile();
#endif
	std::string database = GetOptionFromConfigMap("database", IN_MEMORY_PATH);
	dbc->SetDatabaseName(database);

	config.SetOptionByName("duckdb_api", "odbc");

	// When 'enable_external_access' is set to 'false' then it is not allowed to change
	// 'allowed_paths' or 'allowed_directories' options, so we are setting 'enable_external_access'
	// after all other options
	std::string enable_external_access = GetOptionFromConfigMap("enable_external_access");

	// Back slashes are not accepted for 'allowed_paths' or 'allowed_directories' options
	NormalizeWindowsPathSeparators("allowed_paths");
	NormalizeWindowsPathSeparators("allowed_directories");

	// Session init SQL file
	std::string session_init_sql_file = GetOptionFromConfigMap(SessionInit::SQL_FILE_OPTION);
	std::string session_init_sql_file_sha256 = GetOptionFromConfigMap(SessionInit::SQL_FILE_SHA256_OPTION);
	SessionInitSQLFile session_init_content;

	// Remove ODBC-local options from the config map
	config_map.erase("database");
	config_map.erase("dsn");
	config_map.erase(SessionInit::SQL_FILE_OPTION);
	config_map.erase(SessionInit::SQL_FILE_SHA256_OPTION);

	// Remove 'enable_external_access' option because it is handled separately
	config_map.erase("enable_external_access");

	bool db_created = false;
	try {
		session_init_content = SessionInit::ReadSQLFile(session_init_sql_file, session_init_sql_file_sha256);

		// Validate and set all options
		config.SetOptionsByName(config_map);

		if (!enable_external_access.empty()) {
			config.SetOptionByName("enable_external_access", duckdb::Value(enable_external_access));
		}

		bool cache_instance = database != IN_MEMORY_PATH;

		dbc->env->db = instance_cache.GetOrCreateInstance(database, config, cache_instance,
		                                                  [&db_created](DuckDB &_) { db_created = true; });
	} catch (std::exception &ex) {
		ErrorData error(ex);
		return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", error.Message(), SQLStateType::ST_IM003, "");
	}

	if (!dbc->conn) {
		dbc->conn = make_uniq<Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}

	SQLRETURN ret = SQL_SUCCESS;

	if (!session_init_content.IsEmpty()) {
		ret = SessionInit::Run(dbc, session_init_content, db_created);
		if (!SQL_SUCCEEDED(ret)) {
			dbc->conn.reset();
		}
	}

	return ret;
}

Connect::Connect(OdbcHandleDbc *dbc_p, string input_str_p) : dbc(dbc_p), input_str(std::move(input_str_p)) {
	// Get all the config options
	auto config_options = DBConfig::GetOptionNames();
	for (auto &option : config_options) {
		// They are all set to false as they haven't been set yet
		seen_config_options[option] = false;
	}

	// Register ODBC-local options
	seen_config_options["database"] = false;
	seen_config_options["dsn"] = false;
	seen_config_options[SessionInit::SQL_FILE_OPTION] = false;
	seen_config_options[SessionInit::SQL_FILE_SHA256_OPTION] = false;

	// Required for settings like 'allowed_directories' that use
	// file separator when checking the property value.
	config.file_system = duckdb::make_uniq<duckdb::VirtualFileSystem>();
}

std::string Connect::GetOptionFromConfigMap(const std::string &option_name, const std::string &default_val) {
	D_ASSERT(seen_config_options.count(option_name) == 1);

	if (!seen_config_options[option_name]) {
		return default_val;
	}

	auto it = config_map.find(option_name);
	D_ASSERT(it != config_map.end());
	return it->second.GetValue<std::string>();
}

void Connect::NormalizeWindowsPathSeparators(const std::string &option_name) {
#ifdef _WIN32
	auto value_str = GetOptionFromConfigMap(option_name);
	if (value_str.empty()) {
		return;
	}
	auto value_str_norm = duckdb::StringUtil::Replace(value_str, "\\", "/");
	config_map.erase(option_name);
	config_map.emplace(option_name, std::move(value_str_norm));
#else  // !_WIN32
	(void)option_name;
#endif // _WIN32
}
