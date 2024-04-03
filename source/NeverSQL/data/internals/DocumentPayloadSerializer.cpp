//
// Created by Nathaniel Rupprecht on 3/27/24.
//

#include "NeverSQL/data/internals/DocumentPayloadSerializer.h"
// Other files.
#include <NeverSQL/data/Page.h>
#include <NeverSQL/data/btree/BTree.h>

namespace neversql::internal {

bool DocumentPayloadSerializer::HasData() {
  return current_index_ < buffer_.Size();
}

std::byte DocumentPayloadSerializer::GetNextByte() {
  if (HasData()) {
    return buffer_.Data()[current_index_++];
  }
  return {};
}

std::size_t DocumentPayloadSerializer::GetRequiredSize() const {
  return buffer_.Size();
}

void DocumentPayloadSerializer::initialize() {
  std::visit([this](auto& document) { document->WriteToBuffer(buffer_); }, document_);
}

const Document& DocumentPayloadSerializer::getDocument() const {
  if (std::holds_alternative<std::unique_ptr<Document>>(document_)) {
    return *std::get<std::unique_ptr<Document>>(document_);
  }
  return *std::get<const Document*>(document_);
}

}  // namespace neversql::internal