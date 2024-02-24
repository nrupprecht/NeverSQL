//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <memory>
#include <string_view>
#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief A memory page for the database.
//!
class Page {
  friend class DataAccessLayer;

public:
  //! \brief Get the page number for this page.
  NO_DISCARD page_number_t GetPageNumber() const { return page_number_; }

  //! \brief Get a pointer to the raw data of the page.
  NO_DISCARD std::byte* GetData() { return reinterpret_cast<std::byte*>(data_.get()); }
  NO_DISCARD const std::byte* GetData() const { return reinterpret_cast<std::byte*>(data_.get()); }
  NO_DISCARD std::byte* GetPtr(page_size_t offset) { return GetData() + offset; }
  NO_DISCARD const std::byte* GetPtr(page_size_t offset) const { return GetData() + offset; }

  template<typename T>
  NO_DISCARD T* GetPtr(page_size_t offset) { return reinterpret_cast<T*>(GetPtr(offset)); }

  template<typename T>
  NO_DISCARD const T* GetPtr(page_size_t offset) const { return reinterpret_cast<T*>(GetPtr(offset)); }

  template<typename T>
  requires std::is_trivially_copyable_v<T>
  NO_DISCARD T CopyAs(page_size_t offset) const {
    T value;
    // Use memcpy to avoid alignment issues.
    std::memcpy(reinterpret_cast<std::byte*>(&value), GetPtr(offset), sizeof(T));
    return value;
  }

  NO_DISCARD char* GetChars() { return data_.get(); }
  NO_DISCARD const char* GetChars() const { return data_.get(); }

  //! \brief Get the size of the page.
  NO_DISCARD page_size_t GetPageSize() const { return page_size_; }

  //! \brief Get a string view into the page's data.
  NO_DISCARD std::string_view GetView() const { return std::string_view(data_.get(), page_size_); }

private:
  //! \brief Private constructor, only the DataAccessLayer can create pages.
  Page(page_number_t page_number, page_size_t page_size) noexcept
      : page_number_(page_number) {
    resize(page_size);
  }

  std::unique_ptr<char[]> data_;
  page_number_t page_number_;
  page_size_t page_size_ = 0;

  void resize(page_size_t size) {
    if (data_ == nullptr || size < page_size_) {
      page_size_ = size;
      data_ = std::make_unique<char[]>(page_size_);
    }
  }
};

}  // namespace neversql
