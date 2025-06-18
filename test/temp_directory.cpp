#include "temp_directory.hpp"

#include <cstdlib>
#include <algorithm>
#include <string>
#include <random>
#include <chrono>
#include <stdexcept>
#include <cstdio>
#include <vector>
#include <cerrno>

#ifdef _WIN32
#define VC_EXTRALEAN
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <direct.h>
#define mkdir _mkdir
#else
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace odbc_test {

std::string TempDirectory::get_temp_dir() {
#ifdef _WIN32
	std::string res = ".";
	char *tmp = std::getenv("TEMP");
#else
	std::string res = "/tmp";
	char *tmp = std::getenv("TMPDIR");
#endif
	if (tmp != nullptr) {
		res = std::string(tmp);
	}
	std::replace(res.begin(), res.end(), '\\', '/');
	return res;
}

std::string TempDirectory::make_unique_dirname() {
	auto now = std::chrono::system_clock::now().time_since_epoch().count();
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, 100000);
	return "tmpdir_duckdbtest_" + std::to_string(now) + "_" + std::to_string(dis(gen));
}

void TempDirectory::mkdir_check(const std::string &path) {
#ifdef _WIN32
	auto ret = mkdir(path.c_str());
#else  // !_WiIN32
	auto ret = mkdir(path.c_str(), 0755);
#endif // _WIN32
	if (ret != 0) {
		throw std::runtime_error("Could not create temporary directory: " + path);
	}
}

void TempDirectory::remove_all(const std::string &path) {
#ifdef _WIN32
	WIN32_FIND_DATA findFileData;
	HANDLE hFind = INVALID_HANDLE_VALUE;

	std::string pattern = path + "/*";
	hFind = FindFirstFile(pattern.c_str(), &findFileData);

	if (hFind == INVALID_HANDLE_VALUE) {
		return;
	}

	do {
		const char *name = findFileData.cFileName;
		if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
			continue;
		}

		std::string fullpath = path + "/" + name;
		if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			remove_all(fullpath);
		} else {
			DeleteFile(fullpath.c_str());
		}
	} while (FindNextFile(hFind, &findFileData) != 0);
	FindClose(hFind);
	RemoveDirectory(path.c_str());
#else
	DIR *dir = opendir(path.c_str());
	if (!dir) {
		return;
	}
	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		std::string name = entry->d_name;
		if (name == "." || name == "..") {
			continue;
		}
		std::string fullpath = path + "/" + name;
		struct stat st;
		if (stat(fullpath.c_str(), &st) == 0) {
			if (S_ISDIR(st.st_mode)) {
				remove_all(fullpath);
			} else {
				std::remove(fullpath.c_str());
			}
		}
	}
	closedir(dir);
	rmdir(path.c_str());
#endif
}

TempDirectory::TempDirectory() {
	std::string base = get_temp_dir();
	int max_tries = 16;
	for (int i = 0; i < max_tries; ++i) {
		std::string candidate = base + "/" + make_unique_dirname();
		try {
			mkdir_check(candidate);
			path = candidate;
			return;
		} catch (...) {
			// Try again with a new name
		}
	}
	throw std::runtime_error("Unable to create temporary directory");
}

TempDirectory::~TempDirectory() {
	if (!path.empty()) {
		remove_all(path);
	}
}

} // namespace odbc_test
