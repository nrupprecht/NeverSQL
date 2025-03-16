//
// Created by Nathaniel Rupprecht on 3/7/24.
//

#pragma once

#include <span>
#include <filesystem>

#include "neversql/utility/Defines.h"

namespace neversql {

enum class RecordType : char {
  BEGIN = 'b',
  UPDATE = 'u',
  ABORT = 'a',
  COMMIT = 'c',
  CHECKPOINT = 'p',
  CLR = 'l',  // Compensation Log Record
};

class WriteAheadLog {
public:
  explicit WriteAheadLog(const std::filesystem::path& log_dir_path);

  void BeginTransation(transaction_t transaction_id);

  void CommitTransation(transaction_t transaction_id);

  //! \brief Register an update to a page.
  void Update(transaction_t transaction_id,
              page_number_t page_number,
              page_size_t offset,
              std::span<const std::byte> data_old,
              std::span<const std::byte> data_new);

  //! \brief Force a flush of the WAL.
  void Flush();

private:
  //! \brief Flush the internal (in-memory) buffer to the WAL file.
  void flushBuffer();

  //! \brief Write a record type into the internal (in-memory) buffer.
  void addToBuffer(RecordType record_type);

  //! \brief Write a span of data into the internal (in-memory) buffer.
  void addToBuffer(std::span<const std::byte> data);

  template<typename T>
    requires std::is_integral_v<T>
  void addToBuffer(T data) {
    std::memcpy(buffer_.data() + buffer_usage_, reinterpret_cast<const char*>(&data), sizeof(data));
    buffer_usage_ += sizeof(data);
  }

  //! \brief The directory in which to write WAL files.
  std::filesystem::path log_dir_path_;

  //! \brief The current WAL file path.
  std::ofstream log_file_;

  //! \brief Keep track of the next LSN to assign.
  sequence_number_t next_sequence_number_ = 1;

  //! \brief The last sequence number that was flushed to disk.
  sequence_number_t last_flushed_sequence_number_ = 0;

  //! \brief For testing, WAL can be switched off.
  bool logging_on_ = true;

  //! \brief A buffer, used to accumulate WAL records before flushing them to persistent storage.
  std::vector<char> buffer_;
  //! \brief The amount of space the buffer is currently using. To reset the bufer, we simply set this to 0.
  std::size_t buffer_usage_ = 0;
};

}  // namespace neversql