#ifndef DUCKDB_FILE_IO_HPP
#define DUCKDB_FILE_IO_HPP

#include <cstddef>
#include <exception>
#include <string>
  
namespace duckdb {

bool FileExists(const std::string &path);

size_t FileSizeBytes(const std::string &path);

std::string FileReadToString(const std::string &path, size_t max_size_bytes);

class FileIOException : public std::exception {
public:
    explicit FileIOException(std::string msg)
        : message(std::move(msg)) {}

    const char* what() const noexcept override {
        return message.c_str();
    }

private:
    std::string message;
};

} // namespace duckdb
#endif // DUCKDB_FILE_IO_HPP