//
// Created by Nathaniel Rupprecht on 3/28/24.
//

#pragma once

#include "neversql/data/internals/DatabaseEntry.h"

namespace neversql::internal {

//! \brief A payload serializer that serializes a span of data, without any additional information or
//!        treatments based on the type of the data.
class SpanPayloadSerializer final : public EntryPayloadSerializer {
public:
  explicit SpanPayloadSerializer(std::span<const std::byte> data)
      : data_(data) {}

  bool HasData() override { return current_index_ < data_.size(); }

  std::byte GetNextByte() override {
    if (HasData()) {
      return data_[current_index_++];
    }
    return {};
  }

  std::size_t GetRequiredSize() const override { return data_.size(); }

private:
  std::span<const std::byte> data_;
  std::size_t current_index_ = 0;
};

}  // namespace neversql::internal
