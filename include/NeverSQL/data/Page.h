//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <memory>
#include <string_view>

namespace neversql {

//! \brief A memory page for the database.
//!
class Page {
  friend class DataAccessLayer;
 public:
  //! \brief Get the page number for this page.
  PageNumber GetPageNumber() const { return page_number_; }

  //! \brief Get a pointer to the raw data of the page.
  char* GetData() { return data_.get(); }
  const char* GetData() const { return data_.get(); }

  //! \brief Get the size of the page.
  std::size_t GetPageSize() const { return page_size_; }

  //! \brief Get a string view into the page's data.
  std::string_view GetView() const { return std::string_view(data_.get(), page_size_); }

 private:
  //! \brief Private constructor, only the DataAccessLayer can create pages.
  Page(PageNumber page_number, std::size_t page_size) noexcept
      : page_number_(page_number) {
    resize(page_size);
  }

  PageNumber page_number_;

  std::unique_ptr<char[]> data_;
  std::size_t page_size_ = 0;
  std::size_t page_capacity_ = 0;

  void resize(std::size_t size) {
    page_size_ = size;
    if (data_ == nullptr || page_capacity_ < page_size_) {
      data_ = std::make_unique<char[]>(page_size_);
    }
  }
};

}  // namespace neversql
