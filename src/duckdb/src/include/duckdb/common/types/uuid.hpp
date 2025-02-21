//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/uuid.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {
class ClientContext;
class RandomEngine;

//! The UUID class contains static operations for the UUID type
class UUID {
public:
	constexpr static const uint8_t STRING_SIZE = 36;
	//! Convert a uuid string to a hugeint object
	static bool FromString(const string &str, hugeint_t &result);
	//! Convert a uuid string to a hugeint object
	static bool FromCString(const char *str, idx_t len, hugeint_t &result) {
		return FromString(string(str, 0, len), result);
	}
	//! Convert a hugeint object to a uuid style string
	static void ToString(hugeint_t input, char *buf);

	//! Convert a uhugeint_t object to a uuid value
	static hugeint_t FromUHugeint(uhugeint_t input);
	//! Convert a uuid value to a uhugeint_t object
	static uhugeint_t ToUHugeint(hugeint_t input);

	//! Convert a hugeint object to a uuid style string
	static hugeint_t GenerateRandomUUID(RandomEngine &engine);
	static hugeint_t GenerateRandomUUID();

	//! Convert a hugeint object to a uuid style string
	static string ToString(hugeint_t input) {
		char buff[STRING_SIZE];
		ToString(input, buff);
		return string(buff, STRING_SIZE);
	}

	static hugeint_t FromString(const string &str) {
		hugeint_t result;
		FromString(str, result);
		return result;
	}
};

} // namespace duckdb
