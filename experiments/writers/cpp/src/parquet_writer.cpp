#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "sqlite3.h"

namespace fs = std::filesystem;

namespace {

constexpr const char* kWriterName = "cpp";

// Appends one row to the writer_runs table shared by the C++, Go and Java writers,
// creating the table if it does not exist yet.
bool RecordRun(const std::string& sqlite_path, const std::string& dataset_name,
               double total_write_time, std::uintmax_t file_size_in_bytes) {
  sqlite3* db = nullptr;
  if (sqlite3_open(sqlite_path.c_str(), &db) != SQLITE_OK) {
    std::cerr << "failed to open sqlite database " << sqlite_path << ": " << sqlite3_errmsg(db) << "\n";
    sqlite3_close(db);
    return false;
  }
  sqlite3_busy_timeout(db, 60000);

  const char* create_sql =
      "CREATE TABLE IF NOT EXISTS writer_runs ("
      "dataset_name TEXT, writer TEXT, total_write_time REAL, file_size_in_bytes INTEGER)";
  char* error = nullptr;
  if (sqlite3_exec(db, create_sql, nullptr, nullptr, &error) != SQLITE_OK) {
    std::cerr << "failed to create writer_runs table: " << error << "\n";
    sqlite3_free(error);
    sqlite3_close(db);
    return false;
  }

  sqlite3_stmt* stmt = nullptr;
  const char* insert_sql =
      "INSERT INTO writer_runs (dataset_name, writer, total_write_time, file_size_in_bytes) "
      "VALUES (?, ?, ?, ?)";
  bool ok = sqlite3_prepare_v2(db, insert_sql, -1, &stmt, nullptr) == SQLITE_OK;
  if (ok) {
    sqlite3_bind_text(stmt, 1, dataset_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, kWriterName, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, total_write_time);
    sqlite3_bind_int64(stmt, 4, static_cast<sqlite3_int64>(file_size_in_bytes));
    ok = sqlite3_step(stmt) == SQLITE_DONE;
  }
  if (!ok) {
    std::cerr << "failed to insert into writer_runs: " << sqlite3_errmsg(db) << "\n";
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return ok;
}

std::vector<std::string> ExpandParquetFiles(const std::string& input_path) {
  std::vector<std::string> matches;
  std::error_code ec;

  const fs::path path(input_path);
  if (fs::exists(path, ec) && fs::is_directory(path, ec)) {
    for (const auto& entry : fs::recursive_directory_iterator(path, ec)) {
      if (ec) {
        break;
      }
      if (!entry.is_regular_file(ec)) {
        continue;
      }
      const auto name = entry.path().filename().string();
      if (name.size() >= 8 && name.substr(name.size() - 8) == ".parquet") {
        matches.push_back(entry.path().string());
      }
    }
  } else if (fs::exists(path, ec) && fs::is_regular_file(path, ec)) {
    const auto name = path.filename().string();
    if (name.size() >= 8 && name.substr(name.size() - 8) == ".parquet") {
      matches.push_back(path.string());
    }
  }

  std::sort(matches.begin(), matches.end());
  return matches;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "usage: " << argv[0]
              << " <input-directory-or-glob> <output-parquet> <dataset-name> <sqlite-database>\n";
    return 2;
  }

  const std::string input_pattern = argv[1];
  const std::string output_path = argv[2];
  const std::string dataset_name = argv[3];
  const std::string sqlite_path = argv[4];

  std::vector<std::string> input_files = ExpandParquetFiles(input_pattern);
  if (input_files.empty()) {
    std::cerr << "no parquet files found for input pattern: " << input_pattern << "\n";
    return 1;
  }

  const auto total_start = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds writer_only_duration(0);

  std::shared_ptr<arrow::io::FileOutputStream> out_stream;
  PARQUET_ASSIGN_OR_THROW(out_stream, arrow::io::FileOutputStream::Open(output_path));

  std::shared_ptr<arrow::Table> table;
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<parquet::arrow::FileWriter> writer;

  for (const auto& input_file : input_files) {
    std::shared_ptr<arrow::io::RandomAccessFile> in_file;
    PARQUET_ASSIGN_OR_THROW(in_file, arrow::io::ReadableFile::Open(input_file));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(in_file, arrow::default_memory_pool()));

    std::shared_ptr<arrow::Table> current_table;
    PARQUET_ASSIGN_OR_THROW(current_table, reader->ReadTable());

    if (table == nullptr) {
      table = current_table;
      schema = table->schema();
      PARQUET_ASSIGN_OR_THROW(
          writer,
          parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), out_stream,
                                           parquet::WriterProperties::Builder().build(),
                                           parquet::ArrowWriterProperties::Builder().build()));
    } else if (!table->schema()->Equals(*current_table->schema(), true)) {
      std::cerr << "schema mismatch between input files\n";
      return 1;
    }

    const auto write_start = std::chrono::high_resolution_clock::now();
    PARQUET_THROW_NOT_OK(writer->WriteTable(*current_table, current_table->num_rows()));
    writer_only_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - write_start);
  }

  if (writer != nullptr) {
    const auto close_start = std::chrono::high_resolution_clock::now();
    PARQUET_THROW_NOT_OK(writer->Close());
    writer_only_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - close_start);
  }

  const auto total_elapsed = std::chrono::duration<double>(
      std::chrono::high_resolution_clock::now() - total_start);
  const auto writer_only_seconds = std::chrono::duration<double>(writer_only_duration).count();

  std::cout << "wrote " << input_files.size() << " parquet files into " << output_path << "\n";
  std::cout << "Total file write time: " << total_elapsed.count() << " seconds\n";
  std::cout << "Disk write time: " << writer_only_seconds << " seconds\n";

  // Recorded after the timers stopped, so it does not affect the measured times.
  const std::uintmax_t file_size_in_bytes = fs::file_size(output_path);
  if (!RecordRun(sqlite_path, dataset_name, total_elapsed.count(), file_size_in_bytes)) {
    return 1;
  }
  std::cout << "Recorded run in " << sqlite_path << " (table writer_runs)\n";
  return 0;
}
