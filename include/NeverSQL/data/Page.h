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
  Page(page_number_t page_number, page_size_t page_size) noexcept
      : page_number_(page_number)
      , page_size_(page_size) {}

  //! \brief Create a placeholder page that has not yet been mapped to a page in the database.
  explicit Page(page_size_t page_size) noexcept
      : page_number_(0)
      , page_size_(page_size) {}

  virtual ~Page() = default;

  //! \brief Get the page number for this page.
  NO_DISCARD page_number_t GetPageNumber() const { return page_number_; }

  //! \brief Get a pointer to the raw data of the page.
  NO_DISCARD std::byte* GetData() { return data_; }
  NO_DISCARD const std::byte* GetData() const { return data_; }
  NO_DISCARD std::byte* GetPtr(page_size_t offset) { return GetData() + offset; }
  NO_DISCARD const std::byte* GetPtr(page_size_t offset) const { return GetData() + offset; }

  template<typename T>
  NO_DISCARD T* GetPtr(page_size_t offset) {
    return reinterpret_cast<T*>(GetPtr(offset));
  }

  template<typename T>
  NO_DISCARD const T* GetPtr(page_size_t offset) const {
    return reinterpret_cast<T*>(GetPtr(offset));
  }

  template<typename T>
    requires std::is_trivially_copyable_v<T>
  NO_DISCARD T CopyAs(page_size_t offset) const {
    T value;
    // Use memcpy to avoid alignment issues.
    std::memcpy(reinterpret_cast<std::byte*>(&value), GetPtr(offset), sizeof(T));
    return value;
  }

  NO_DISCARD char* GetChars() { return reinterpret_cast<char*>(data_); }
  NO_DISCARD const char* GetChars() const { return reinterpret_cast<char*>(data_); }

  //! \brief Get the size of the page.
  NO_DISCARD page_size_t GetPageSize() const { return page_size_; }

  //! \brief Get a string view into the page's data.
  NO_DISCARD std::string_view GetView() const { return std::string_view(GetChars(), page_size_); }

  //! \brief Get a span of the page's data.
  NO_DISCARD std::span<const std::byte> GetSpan(page_size_t offset, page_size_t span_size) const {
    return {GetPtr(offset), span_size};
  }

  NO_DISCARD std::span<std::byte> GetSpan(page_size_t offset, page_size_t span_size) {
    return {GetPtr(offset), span_size};
  }

  //! \brief Set the data that the page references.
  void SetData(std::byte* data) { data_ = data; }

  //! \brief Set the page number that this page corresponds to.
  void SetPageNumber(page_number_t page_number) { setPageNumber(page_number); }
protected:
  //! \brief Assign the data pointer.
  void setData(std::byte* data) { data_ = data; }

  virtual void setPageNumber(page_number_t page_number) { page_number_ = page_number; }

  std::byte* data_;
  page_number_t page_number_;
  page_size_t page_size_ = 0;
};

//! \brief A page that manages its own data buffer.
class FreestandingPage : public Page {
public:
  FreestandingPage(page_number_t page_number, page_size_t page_size) noexcept
      : Page(page_number, page_size) {
    resize(page_size);
  }

private:
  //! \brief The data is stored in the page structure itself, not in some other place referenced by the page.
  std::unique_ptr<std::byte[]> data_buffer_;

  void resize(page_size_t size) {
    page_size_ = size;
    data_buffer_ = std::make_unique<std::byte[]>(page_size_);
    setData(data_buffer_.get());
  }
};

//! \brief Structure that takes care of releasing a page from a cache when it goes out of scope.
struct PageCounter {
  PageCounter(std::optional<page_number_t> page_number, class PageCache* owning_cache) noexcept;
  ~PageCounter();

  std::optional<page_number_t> page_number{};
  class PageCache* owning_cache{};
};

//! \brief A page that knows how to release its use count from a cache upon destruction.
class RCPage : public Page {
public:
  RCPage(page_number_t page_number, page_size_t page_size, class PageCache* owning_cache) noexcept;

  //! \brief Create a placeholder page that has not yet been mapped to a page in the database.
  RCPage(page_size_t page_size, class PageCache* owning_cache) noexcept;

private:
  void setPageNumber(page_number_t page_number) override;

  //! \brief Object that keeps track of the page's use count in the cache. We allow RCPage to be easily
  //! copyable without incrementing or decrementing the use count in the cache, we instead treat an RCPage and
  //! any copies made of the same RCPage as just one useage, and use the lifetime of the counter to control
  //! releasing the page.
  std::shared_ptr<PageCounter> counter_{};
};

}  // namespace neversql
