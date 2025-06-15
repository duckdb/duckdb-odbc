#include "file_io.hpp"

#ifndef _WIN32
#include <unistd.h>
#include <sys/stat.h>
#endif // !_WIN32

#include <fstream>

#include "widechar.hpp"

namespace duckdb {

#ifdef _WIN32
static std::wstring WidenStr(const std::string &str) {
	const SQLCHAR *first_invalid_char = nullptr;
	const SQLCHAR *str_ptr = reinterpret_cast<const SQLCHAR *>(str.data());
	std::vector<SQLWCHAR> vec = widechar::utf8_to_utf16_lenient(str_ptr, str.length(), &first_invalid_char);
	if (first_invalid_char != nullptr) {
		throw FileIOException("Invalid UTF-8 file name: [" + str + "]");
	}
	wchar_t *wide_ptr = reinterpret_cast<wchar_t *>(vec.data());
	return std::wstring(wide_ptr, vec.size());
}
#endif // _WIN32

bool FileExists(const std::string &path) {
#ifdef _WIN32
	std::wstring wpath = WidenStr(path);
	DWORD attrs = GetFileAttributesW(wpath.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY));
#else  // !_WIN32
	auto ret = access(path.c_str(), F_OK);
	return ret != -1;
#endif // _WIN32
}

std::size_t FileSizeBytes(const std::string &path) {
#ifdef _WIN32
	std::wstring wpath = WidenStr(path);
	HANDLE file =
	    CreateFileW(wpath.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (file == INVALID_HANDLE_VALUE) {
		throw FileIOException("Cannot open file, path: " + path);
	}

	LARGE_INTEGER size;
	BOOL result = GetFileSizeEx(file, &size);
	CloseHandle(file);

	if (result == 0) {
		throw FileIOException("Cannot get file size, path: " + path);
	}
	return size.QuadPart;
#else  // !_WIN32
	struct stat st;
	auto ret = stat(path.c_str(), &st);
	if (ret != 0) {
		throw FileIOException("Cannot get file size, path: " + path);
	}
	return st.st_size;
#endif // _WIN32
}

std::string FileReadToString(const std::string &path, std::size_t max_size_bytes) {
	try {
		std::ifstream file;
		file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

#ifdef _WIN32
		std::wstring wpath = WidenStr(path);
		file.open(wpath, std::ios::in | std::ios::binary);
#else  // !_WIN32
		file.open(path, std::ios::in | std::ios::binary);
#endif // _WIN32

		std::string result;
		result.resize(max_size_bytes);

		std::size_t total_read = 0;
		while (total_read < max_size_bytes && file) {
			file.read(&result[total_read], max_size_bytes - total_read);
			std::size_t bytes_read = file.gcount();
			if (bytes_read == 0) {
				break; // EOF
			}
			total_read += bytes_read;
		}
		result.resize(total_read);
		return result;
	} catch (const std::ios_base::failure &e) {
		throw FileIOException("File read error, path: " + path + ", message: " + e.what());
	}
}

} // namespace duckdb
