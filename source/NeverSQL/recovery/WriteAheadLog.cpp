//
// Created by Nathaniel Rupprecht on 3/10/24.
//

#include "NeverSQL/recovery/WriteAheadLog.h"

namespace neversql {

WriteAheadLog::WriteAheadLog(const std::filesystem::path& log_dir_path)
    : log_dir_path_(log_dir_path)
    , buffer_(16 * 1024) {
  if (!std::filesystem::exists(log_dir_path_)) {
    std::filesystem::create_directories(log_dir_path_);
  }
  log_file_.open(log_dir_path_ / "wal.log", std::ios::out | std::ios::binary);
  NOSQL_REQUIRE(log_file_.is_open(), "failed to open WriteAheadLog file");
}

void WriteAheadLog::BeginTransation(transaction_t transaction_id) {
  addToBuffer(RecordType::BEGIN);
  addToBuffer(transaction_id);
}

void WriteAheadLog::CommitTransation(transaction_t transaction_id) {
  addToBuffer(RecordType::COMMIT);
  addToBuffer(transaction_id);
}

void WriteAheadLog::Update(transaction_t transaction_id,
                           page_number_t page_number,
                           page_size_t offset,
                           std::span<const std::byte> data_old,
                           std::span<const std::byte> data_new) {
  if (!logging_on_) {
    return;
  }

  NOSQL_REQUIRE(data_old.size() == data_new.size(), "data_old and data_new must be the same size");
  NOSQL_REQUIRE(log_file_.is_open(), "WriteAheadLog is not open");

  auto data_size = static_cast<std::streamsize>(data_old.size());

  auto sequence_number = next_sequence_number_++;

  // Determine if there is enough room in the buffer to write the record.
  auto size_requirement = sizeof(RecordType::COMMIT) + sizeof(sequence_number) + sizeof(transaction_id)
      + sizeof(page_number) + sizeof(offset) + sizeof(data_size) + data_old.size() * 2;
  if (buffer_.size() - buffer_usage_ < size_requirement) {
    flushBuffer();
  }

  // Add all the data to the WAL buffer.
  addToBuffer(RecordType::COMMIT);
  addToBuffer(sequence_number);
  addToBuffer(transaction_id);
  addToBuffer(page_number);
  addToBuffer(offset);
  addToBuffer(data_size);
  addToBuffer(data_old);
  addToBuffer(data_new);
}

void WriteAheadLog::Flush() {
  flushBuffer();
  last_flushed_sequence_number_ = next_sequence_number_ - 1;
}

void WriteAheadLog::addToBuffer(RecordType record_type) {
  addToBuffer(static_cast<std::uint8_t>(record_type));
}

void WriteAheadLog::addToBuffer(std::span<const std::byte> data) {
  std::memcpy(buffer_.data() + buffer_usage_, data.data(), data.size());
  buffer_usage_ += data.size();
}

void WriteAheadLog::flushBuffer() {
  log_file_.write(buffer_.data(), static_cast<std::streamsize>(buffer_usage_));
  log_file_.flush();
  buffer_usage_ = 0;
}

}  // namespace neversql