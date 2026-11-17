#include "connect.hpp"

#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <unordered_set>
#include <utility>

#include "session_init.hpp"

using namespace duckdb_odbc;

//! Registers the extensions linked into the driver, generated with the engine (see scripts/build_duckdb.py).
extern "C" int32_t duckdb_register_static_extensions(void);

namespace {

//! Registers the statically linked extensions, before the first database is opened.
void RegisterStaticExtensions() {
	static std::once_flag once;
	std::call_once(once, [] {
		if (duckdb_register_static_extensions() != 0) {
			throw std::runtime_error("Failed to register the extensions linked into the DuckDB ODBC driver");
		}
	});
}

//! A DuckDB option a connection string or DSN can set.
struct OptionInfo {
	std::string name;
	//! Whether the option can be set database-wide; session-only options are set on the connection.
	bool global;
};

//! The options DuckDB knows, including the aliases they go by, keyed by their lower-case name.
const std::map<std::string, OptionInfo> &GetDuckDBOptions() {
	static std::once_flag once;
	static std::map<std::string, OptionInfo> options;
	std::call_once(once, [] {
		RegisterStaticExtensions();
		// an instance with nothing attached, just to describe the options
		cxx::Instance instance;
		auto &config = instance.GetConfig();
		for (size_t i = 0; i < config.GetOptionCount(); i++) {
			auto description = config.DescribeOption(i);
			OptionInfo info {std::string(description.GetName()), description.SupportsScope(cxx::SettingScope::GLOBAL)};
			options.emplace(StringUtil::Lower(info.name), info);
			for (size_t alias_idx = 0; alias_idx < description.GetAliasCount(); alias_idx++) {
				options.emplace(StringUtil::Lower(std::string(description.GetAliasByIndex(alias_idx))), info);
			}
		}
	});
	return options;
}

//! Options that can only be written once the database is running: they are set after it starts.
const std::unordered_set<std::string> &GetRuntimeOptions() {
	static const std::unordered_set<std::string> options = {
	    "allowed_directories", "allowed_paths",   "disabled_filesystems", "enable_logging",     "logging_level",
	    "logging_mode",        "logging_storage", "enabled_log_types",    "disabled_log_types",
	};
	return options;
}

//! A database shared by the connections to the same file, and the configuration it was opened with.
struct CachedInstance {
	std::weak_ptr<cxx::Instance> instance;
	std::string configuration;
};

//! The database instance cache, used so that multiple connections to the same file point to the same database object
std::mutex instance_cache_lock;
std::unordered_map<std::string, CachedInstance> instance_cache;

//! The key a database file is cached under: its absolute path, or the path as given for e.g. "md:" databases.
std::string GetInstanceCacheKey(const std::string &database) {
	auto colon = database.find(':');
	if (colon != std::string::npos && colon > 1) {
		// a database type or URL prefix, e.g. "md:" or "s3://"
		return database;
	}
	std::string path = database;
	if (path.size() >= 1 && path[0] == '~' && (path.size() == 1 || path[1] == '/' || path[1] == '\\')) {
		const char *home = std::getenv("HOME");
#ifdef _WIN32
		if (!home) {
			home = std::getenv("USERPROFILE");
		}
#endif
		if (home) {
			path = std::string(home) + path.substr(1);
		}
	}
	try {
		return std::filesystem::absolute(std::filesystem::path(path)).lexically_normal().string();
	} catch (std::exception &) {
		return path;
	}
}

} // namespace

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

	// If the input doesn't match a keyname, find a similar keyname
	vector<string> config_name_strings;
	for (auto &option : GetDuckDBOptions()) {
		config_name_strings.emplace_back(option.first);
	}
	auto msg = StringUtil::CandidatesErrorMessage(config_name_strings, input, "Did you mean: ");
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
	// options to set on the connection, as they cannot be set database-wide
	std::vector<std::pair<std::string, std::string>> session_options;
	try {
		session_init_content = SessionInit::ReadSQLFile(session_init_sql_file, session_init_sql_file_sha256);

		// Sort the options by when they can be set: at startup, once the database runs, or on the connection
		auto &duckdb_options = GetDuckDBOptions();
		cxx::StartupOptions startup_options;
		startup_options.Set("duckdb_api", "odbc");
		std::vector<std::pair<std::string, std::string>> runtime_options;
		std::map<std::string, std::string> sorted_options(config_map.begin(), config_map.end());
		for (auto &option : sorted_options) {
			auto entry = duckdb_options.find(StringUtil::Lower(option.first));
			auto name = entry == duckdb_options.end() ? option.first : entry->second.name;
			if (entry != duckdb_options.end() && !entry->second.global) {
				session_options.emplace_back(name, option.second);
			} else if (GetRuntimeOptions().count(name)) {
				runtime_options.emplace_back(name, option.second);
			} else {
				startup_options.Set(name, option.second);
			}
		}
		bool disable_external_access = false;
		if (!enable_external_access.empty()) {
			auto value = StringUtil::Lower(OdbcUtils::TrimString(enable_external_access));
			if (value == "false" || value == "0" || value == "f" || value == "off" || value == "no" || value == "n") {
				// set last: it forbids changing 'allowed_paths' and 'allowed_directories'
				disable_external_access = true;
			} else {
				startup_options.Set("enable_external_access", enable_external_access);
			}
		}

		// The configuration a cached database must have been opened with to be shared
		std::string configuration;
		for (auto &option : sorted_options) {
			configuration += option.first + "=" + option.second + "\n";
		}
		configuration += "enable_external_access=" + enable_external_access;

		auto open_instance = [&]() {
			RegisterStaticExtensions();
			auto instance = std::make_shared<cxx::Instance>(startup_options);
			auto &instance_config = instance->GetConfig();
			for (auto &option : runtime_options) {
				instance_config.SetOption(option.first, std::string_view(option.second), cxx::SettingScope::GLOBAL);
			}
			if (disable_external_access) {
				instance_config.SetOption("enable_external_access", std::string_view(enable_external_access),
				                          cxx::SettingScope::GLOBAL);
			}
			instance->Attach(database == IN_MEMORY_PATH ? std::string() : database, true);
			db_created = true;
			return instance;
		};

		if (database == IN_MEMORY_PATH) {
			// every in-memory database is its own
			dbc->db = open_instance();
		} else {
			auto key = GetInstanceCacheKey(database);
			std::lock_guard<std::mutex> guard(instance_cache_lock);
			auto entry = instance_cache.find(key);
			shared_ptr<cxx::Instance> instance;
			if (entry != instance_cache.end()) {
				instance = entry->second.instance.lock();
				if (instance && entry->second.configuration != configuration) {
					throw std::runtime_error("Can't open a connection to same database file with a different "
					                         "configuration than existing connections");
				}
			}
			if (!instance) {
				instance = open_instance();
				instance_cache[key] = CachedInstance {instance, configuration};
			}
			dbc->db = std::move(instance);
		}
	} catch (std::exception &ex) {
		return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", ErrorMessage(ex), SQLStateType::ST_IM003, "");
	}

	if (!dbc->conn) {
		try {
			dbc->conn = make_uniq<cxx::Connection>(dbc->db->Connect());
			for (auto &option : session_options) {
				dbc->conn->GetConfig().SetOption(option.first, std::string_view(option.second),
				                                 cxx::SettingScope::SESSION);
			}
			if (!dbc->autocommit) {
				// manual-commit mode: there is always an open transaction
				dbc->conn->Execute("BEGIN TRANSACTION").Drain();
			}
		} catch (std::exception &ex) {
			dbc->conn.reset();
			return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", ErrorMessage(ex), SQLStateType::ST_IM003,
			                           "");
		}
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
	for (auto &option : GetDuckDBOptions()) {
		// They are all set to false as they haven't been set yet
		seen_config_options[option.first] = false;
	}

	// Register ODBC-local options
	seen_config_options["database"] = false;
	seen_config_options["dsn"] = false;
	seen_config_options[SessionInit::SQL_FILE_OPTION] = false;
	seen_config_options[SessionInit::SQL_FILE_SHA256_OPTION] = false;
}

std::string Connect::GetOptionFromConfigMap(const std::string &option_name, const std::string &default_val) {
	D_ASSERT(seen_config_options.count(option_name) == 1);

	if (!seen_config_options[option_name]) {
		return default_val;
	}

	auto it = config_map.find(option_name);
	D_ASSERT(it != config_map.end());
	return it->second;
}

void Connect::NormalizeWindowsPathSeparators(const std::string &option_name) {
#ifdef _WIN32
	auto value_str = GetOptionFromConfigMap(option_name);
	if (value_str.empty()) {
		return;
	}
	auto value_str_norm = duckdb_odbc::StringUtil::Replace(value_str, "\\", "/");
	config_map.erase(option_name);
	config_map.emplace(option_name, std::move(value_str_norm));
#else  // !_WIN32
	(void)option_name;
#endif // _WIN32
}
