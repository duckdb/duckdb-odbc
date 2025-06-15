#ifndef ODBC_TEST_TEMP_DIRECTORY_H
#define ODBC_TEST_TEMP_DIRECTORY_H

#include <string>

namespace odbc_test {

class TempDirectory {
public:
	std::string path;

	static std::string get_temp_dir();

	static std::string make_unique_dirname();

	static void mkdir_check(const std::string &path);

	static void remove_all(const std::string &path);

	TempDirectory();
	~TempDirectory();

	TempDirectory(const TempDirectory &) = delete;
	TempDirectory &operator=(const TempDirectory &) = delete;
};

} // namespace odbc_test

#endif // ODBC_TEST_TEMP_DIRECTORY_H
