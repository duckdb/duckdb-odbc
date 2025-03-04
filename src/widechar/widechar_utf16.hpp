

namespace duckdb {
namespace widechar {

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

} // namespace widechar
} // namespace duckdb
