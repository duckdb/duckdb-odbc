
#include "widechar.hpp"

#define UTF_CPP_CPLUSPLUS 199711L
#include "utf8.h"

namespace duckdb {

static const uint32_t invalid_char_replacement = 0xfffd;

template <typename u16bit_iterator>
static u16bit_iterator utf16_find_invalid(u16bit_iterator start, u16bit_iterator end) {
	while (start != end) {
		utf8::utfchar32_t cp = utf8::internal::mask16(*start++);
		if (utf8::internal::is_lead_surrogate(cp)) { // Take care of surrogate pairs first
			if (start != end) {
				const utf8::utfchar32_t trail_surrogate = utf8::internal::mask16(*start++);
				if (!utf8::internal::is_trail_surrogate(trail_surrogate)) {
					return start - 1;
				}
			} else {
				return start - 1;
			}
		} else if (utf8::internal::is_trail_surrogate(cp)) { // Lone trail surrogate
			return start;
		}
	}
	return end;
}

template <typename u16bit_iterator, typename output_iterator>
static output_iterator utf16_replace_invalid(u16bit_iterator start, u16bit_iterator end, output_iterator out,
                                             utf8::utfchar32_t replacement) {
	while (start != end) {
		uint16_t char1 = *start++;
		utf8::utfchar32_t cp = utf8::internal::mask16(char1);
		if (utf8::internal::is_lead_surrogate(cp)) { // Take care of surrogate pairs first
			if (start != end) {
				uint16_t char2 = *start++;
				const utf8::utfchar32_t trail_surrogate = utf8::internal::mask16(char2);
				if (utf8::internal::is_trail_surrogate(trail_surrogate)) {
					*out++ = char1;
					*out++ = char2;
				} else {
					*out++ = replacement;
					*out++ = char2;
				}
			} else {
				*out++ = replacement;
			}
		} else if (utf8::internal::is_trail_surrogate(cp)) { // Lone trail surrogate
			*out++ = replacement;
		} else {
			*out++ = char1;
		}
	}
	return out;
}

std::vector<uint8_t> utf16_to_utf8_lenient(const uint16_t *in_buf, std::size_t in_buf_len,
                                           const uint16_t **first_invalid_char) {
	std::vector<uint8_t> res;
	auto res_bi = std::back_inserter(res);

	const uint16_t *in_buf_end = in_buf + in_buf_len;
	const uint16_t *fic = utf16_find_invalid(in_buf, in_buf_end);

	if (fic == in_buf_end) {
		utf8::unchecked::utf16to8(in_buf, in_buf_end, res_bi);
		if (nullptr != first_invalid_char) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf16to8(replaced.begin(), replaced.end(), res_bi);
		if (nullptr != first_invalid_char) {
			*first_invalid_char = fic;
		}
	}

	return res;
}

std::vector<uint16_t> utf8_to_utf16_lenient(const uint8_t *in_buf, std::size_t in_buf_len,
                                            const uint8_t **first_invalid_char) {
	std::vector<uint16_t> res;
	auto res_bi = std::back_inserter(res);

	const uint8_t *in_buf_end = in_buf + in_buf_len;
	const uint8_t *fic = utf8::find_invalid(in_buf, in_buf_end);

	if (fic == in_buf_end) {
		utf8::unchecked::utf8to16(in_buf, in_buf_end, res_bi);
		if (nullptr != first_invalid_char) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<uint8_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf8::replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf8to16(replaced.begin(), replaced.end(), res_bi);
		if (nullptr != first_invalid_char) {
			*first_invalid_char = fic;
		}
	}

	return res;
}

} // namespace duckdb
