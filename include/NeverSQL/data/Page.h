//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <memory>
#include <span>
#include <string_view>

#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief A memory page for the database.
//!
//! By default, a page referenced memory, but does not own it. This is because generally, the page actually
//! references data stored in a page buffer, which manages the memory.
//!
//! For a page that is self-contained, use the FreestandingPage class, below.
class Page {
  friend class DataAccessLayer;

public:
  //! \brief Create a page structure representing a page in the database with a specific number and its size,
  //! in bytes.
  Page(page_number_t page_number, transaction_t transaction_number, page_size_t page_size)
      : page_number_(page_number)
      , transaction_number_(transaction_number)
      , page_size_(page_size) {
    NOSQL_REQUIRE(0 < page_size, "page size cannot be zero");
  }

  //! \brief Create a placeholder page that has not yet been mapped to a page in the database.
  explicit Page(page_size_t page_size)
      : page_number_(0)
      , page_size_(page_size) {
    NOSQL_REQUIRE(0 < page_size, "page size cannot be zero");
  }

  virtual ~Page() = default;

  // =================================================================================================
  //  Reading and writing from pages.
  // =================================================================================================

  //! \brief Write to a page, potentially causing a WAL to be written. Returns the offset after the write.
  virtual page_size_t WriteToPage(page_size_t offset, std::span<const std::byte> data) = 0;

  //! \brief Write a value of a particular type.
  template<typename T>
    requires std::is_trivially_copyable_v<T>
  page_size_t WriteToPage(page_size_t offset, const T& data) {
    std::span view(reinterpret_cast<const std::byte*>(&data), sizeof(T));
    return WriteToPage(offset, view);
  }

  //! \brief Write a span of data to a page.
  template<typename T>
  page_size_t WriteToPage(page_size_t offset, std::span<T> data_range) {
    return WriteToPage(
        offset,
        std::span(reinterpret_cast<const std::byte*>(data_range.data()), data_range.size() * sizeof(T)));
  }

  void MoveInPage(page_size_t src_offset, page_size_t dest_offset, page_size_t size) {
    NOSQL_REQUIRE(src_offset + size <= page_size_,
                  "MoveInPage: src_offset + size = " << src_offset + size << " is greater than page size "
                                                     << page_size_);
    NOSQL_REQUIRE(dest_offset + size <= page_size_,
                  "MoveInPage: dest_offset + size = " << dest_offset + size << " is greater than page size "
                                                      << page_size_);
    WriteToPage(dest_offset, GetSpan(src_offset, size));
  }

  //! \brief Read data from a page, as a span.
  std::span<const std::byte> ReadFromPage(page_size_t offset, page_size_t size) const {
    NOSQL_REQUIRE(offset + size <= page_size_,
                  "ReadFromPage: offset + size = " << offset + size << " is greater than page size "
                                                   << page_size_ << " on page " << page_number_);
    return {getPtr(offset), size};
  }

  template<typename T>
    requires std::is_trivially_copyable_v<T>
  NO_DISCARD T Read(page_size_t offset) const {
    T value;
    // Use memcpy to avoid alignment issues.
    std::memcpy(reinterpret_cast<std::byte*>(&value), getPtr(offset), sizeof(T));
    return value;
  }

  NO_DISCARD const char* GetChars() const { return reinterpret_cast<const char*>(data_); }

  NO_DISCARD const std::byte* GetData() const { return data_; }

  //! \brief Get a span of the page's data.
  NO_DISCARD std::span<const std::byte> GetSpan(page_size_t offset, page_size_t span_size) const {
    return ReadFromPage(offset, span_size);
  }

  template<typename T>
    requires std::is_trivially_copyable_v<T>
  NO_DISCARD std::span<const T> GetSpan(page_size_t offset, page_size_t num_elements) const {
    return std::span(reinterpret_cast<const T*>(getPtr(offset)), num_elements);
  }

  template<typename T>
    requires std::is_trivially_copyable_v<T>
  NO_DISCARD std::span<T> GetMutableSpan(page_size_t offset, page_size_t num_elements) {
    return std::span<T>(reinterpret_cast<T*>(getMutablePtr(offset)), num_elements);
  }

  // =================================================================================================
  //  Other Page functions.
  // =================================================================================================

  NO_DISCARD virtual std::unique_ptr<Page> NewHandle() const = 0;

  //! \brief Get the page number for this page.
  NO_DISCARD page_number_t GetPageNumber() const { return page_number_; }

  //! \brief Get the size of the page.
  NO_DISCARD page_size_t GetPageSize() const { return page_size_; }

  //! \brief Get a string view into the page's data.
  NO_DISCARD std::string_view GetView() const { return std::string_view(GetChars(), page_size_); }

  //! \brief Set the page number that this page corresponds to.
  void SetPageNumber(page_number_t page_number) { setPageNumber(page_number); }

  //! \brief Set the transaction number that checked out this page.
  void SetTransactionNumber(transaction_t transaction) { transaction_number_ = transaction; }

protected:
  NO_DISCARD const std::byte* getPtr(page_size_t offset) const {
    NOSQL_REQUIRE(offset < GetPageSize(),
                  "getPtr: offset " << offset << " is greater than page size (" << GetPageSize() << ")");
    return data_ + offset;
  }
  NO_DISCARD std::byte* getMutablePtr(page_size_t offset) {
    NOSQL_REQUIRE(
        offset < GetPageSize(),
        "getMutablePtr: offset " << offset << " is greater than page size (" << GetPageSize() << ")");
    return data_ + offset;
  }
  NO_DISCARD char* getChars() const { return reinterpret_cast<char*>(data_); }
  void setPageNumber(page_number_t page_number) { page_number_ = page_number; }

  std::byte* data_ {};
  page_number_t page_number_ {};
  transaction_t transaction_number_ {};
  page_size_t page_size_ = 0;
};

//! \brief A page that manages its own data buffer.
class FreestandingPage : public Page {
public:
  FreestandingPage(page_number_t page_number,
                   transaction_t transaction_number,
                   page_size_t page_size) noexcept
      : Page(page_number, transaction_number, page_size) {
    resize(page_size);
  }

  page_size_t WriteToPage(page_size_t offset, std::span<const std::byte> data) override {
    NOSQL_REQUIRE(offset + data.size() <= page_size_,
                  "WriteToPage: offset + data.size() is greater than page size");
    std::memcpy(data_ + offset, data.data(), data.size());
    return static_cast<page_size_t>(offset + data.size());
  }

  NO_DISCARD std::unique_ptr<Page> NewHandle() const override {
    auto page = std::make_unique<FreestandingPage>(page_number_, transaction_number_, page_size_);
    page->data_buffer_ = data_buffer_;
    return page;
  }

private:
  //! \brief The data is stored in the page structure itself, not in some other place referenced by the page.
  std::shared_ptr<std::vector<std::byte>> data_buffer_;

  void resize(page_size_t size) {
    page_size_ = size;
    data_buffer_ = std::make_shared<std::vector<std::byte>>(page_size_);
    data_ = data_buffer_->data();
  }
};

//! \brief A page that knows how to release its use count from a cache upon destruction.
class RCPage : public Page {
public:
  RCPage(page_number_t page_number,
         transaction_t transaction_number,
         page_size_t page_size,
         uint32_t descriptor_index,
         class PageCache* owning_cache) noexcept;

  //! \brief Create a placeholder page that has not yet been mapped to a page in the database.
  RCPage(page_size_t page_size, uint32_t descriptor_index, class PageCache* owning_cache) noexcept;

  //! \brief Release the page.
  ~RCPage() override;

  //! \brief Write to a page. This registers the write with the page cache and WAL.
  page_size_t WriteToPage(page_size_t offset, std::span<const std::byte> data) override;

  //! \brief Set the data that the page references.
  void SetData(std::byte* data) { data_ = data; }

  //! \brief Go back to the cache to get another handle to this page.
  NO_DISCARD std::unique_ptr<Page> NewHandle() const override;

private:
  //! \brief The page cache that owns this page.
  PageCache* owning_cache_;

  uint32_t descriptor_index_;
};

}  // namespace neversql
