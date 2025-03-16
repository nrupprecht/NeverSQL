//
// Created by Nathaniel Rupprecht on 3/30/24.
//

#pragma once

#include "neversql/data/btree/EntryCreator.h"

namespace neversql::internal {

//! \brief Entry copier that copies the payload, no matter what type of payload it is (single page entry or
//!        overflow page header / entry).
class EntryCopier : public EntryCreator {
public:
  EntryCopier(std::byte flags, std::span<const std::byte> payload);

  //! \brief Return the stored flags
  std::byte GenerateFlags() const override { return flags_; }
private:
  std::byte flags_;
};

}  // namespace neversql::internal