#include "duckdb_static_extension.h"

extern "C" {
int32_t duckdb_extension_httplib_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_loadable_extensions_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_parquet_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_icu_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_json_describe(duckdb_extension_descriptor *descriptor);
}

// Prebuilt archives already define their descriptors, including in combined LTO
// objects. Register those functions without redefining them in the package loader.
extern "C" int32_t duckdb_register_static_extensions(void) {
	const duckdb_extension_describe_t extensions[] = {duckdb_extension_httplib_describe,
	                                                  duckdb_extension_loadable_extensions_describe,
	                                                  duckdb_extension_core_functions_describe,
	                                                  duckdb_extension_parquet_describe,
	                                                  duckdb_extension_icu_describe,
	                                                  duckdb_extension_json_describe};
	int32_t result = 0;
	for (auto describe : extensions) {
		if (duckdb_register_static_extension(describe) != 0) {
			result = 1;
		}
	}
	return result;
}
