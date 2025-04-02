#pragma once

#include "neversql/data/Page.h"

namespace neversql {

namespace utility {
class PageInspector;
}  // namespace utility

class MetaPage {
  friend class DataManager;

  friend class utility::PageInspector;

  struct Layout {
    STRUCT_DATA(uint64_t, magic_number);         // 8 bytes
    STRUCT_DATA(uint8_t, page_size_power);       // 1 byte
    STRUCT_DATA(page_number_t, free_list_page);  // 8 bytes
    STRUCT_DATA(page_number_t, index_page);      // 8 bytes
    STRUCT_DATA(uint64_t, next_transaction_id);  // 8 bytes
  };

public:
  uint64_t GetMagicNumber() const noexcept { return page_->Read<uint64_t>(offset(&Layout::magic_number)); }
  
  page_size_t GetPageSize() const noexcept { return 1 << GetPageSizePower(); }

  uint8_t GetPageSizePower() const noexcept { return page_->Read<uint8_t>(offset(&Layout::page_size_power)); }
  
  page_number_t GetFreeListPage() const noexcept {
    return page_->Read<page_number_t>(offset(&Layout::free_list_page));
  }

  page_number_t GetIndexPage() const noexcept {
    return page_->Read<page_number_t>(offset(&Layout::index_page));
  }

  uint64_t GetNextTransactionId() const noexcept {
    return page_->Read<uint64_t>(offset(&Layout::next_transaction_id));
  }

  void SetMagicNumber(Transaction& transaction, uint64_t magic_number) {
    transaction.WriteToPage(*page_, offset(&Layout::magic_number), magic_number);
  }

  void SetPageSizePower(Transaction& transaction, uint8_t page_size_power) {
    transaction.WriteToPage(*page_, offset(&Layout::page_size_power), page_size_power);
  }

  void SetFreeListPage(Transaction& transaction, page_number_t free_list_page) {
    transaction.WriteToPage(*page_, offset(&Layout::free_list_page), free_list_page);
  }

  void SetIndexPage(Transaction& transaction, page_number_t index_page) {
    transaction.WriteToPage(*page_, offset(&Layout::index_page), index_page);
  }

  void SetNextTransactionId(Transaction& transaction, uint64_t next_transaction_id) {
    transaction.WriteToPage(*page_, offset(&Layout::next_transaction_id), next_transaction_id);
  }

private:
  explicit MetaPage(std::unique_ptr<Page>&& page) noexcept {}

  std::unique_ptr<Page> page_;
};

}  // namespace neversql