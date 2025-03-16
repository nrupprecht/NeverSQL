//
// Created by Nathaniel Rupprecht on 3/26/24.
//

#pragma once

#include "neversql/data/internals/DatabaseEntry.h"
#include "neversql/data/btree/BTreeNodeMap.h"

namespace neversql::internal {

//! \brief Represents an entry that is stored across one or more overflow pages.
class OverflowEntry final : public DatabaseEntry {
public:
  OverflowEntry(std::span<const std::byte> entry_header,
                const BTreeManager* btree_manager);

  std::span<const std::byte> GetData() const noexcept override;

  bool Advance() override;

  bool IsValid() const override;

private:
  //! \brief Set up the data for the current overflow page.
  void setup();

  //! \brief The overflow key for the overflow entry.
  primary_key_t overflow_key_ = 0;

  //! \brief The next page that part of the overflow entry is on.
  page_number_t next_page_number_ = 0;

  //! \brief A tree manager, which lets the entry load new pages, getting the next part of the overflow entry
  //!        if it is on more than one page.
  const BTreeManager* btree_manager_;

  //! \brief The current node that the overflow entry is on.
  std::optional<BTreeNodeMap> node_;
};

}  // namespace neversql::internal