//
// Created by Nathaniel Rupprecht on 3/27/24.
//

#pragma once

#include "neversql/data/Document.h"
#include "neversql/data/internals/EntryPayloadSerializer.h"

namespace neversql::internal {

//! \brief An entry creator that will serialize a document into its entry.
//!
//! \note This is currently a naieve implementation, which just serializes the document up front to a buffer.
//!       This is not ideal, as it means that the entire document must be in memory at once. This is not
//!       scalable for large documents. A better implementation would be to serialize the document in chunks
//!       as it is being written to disk.
class DocumentPayloadSerializer final : public EntryPayloadSerializer {
public:
  explicit DocumentPayloadSerializer(std::unique_ptr<Document> document)
      : document_(std::move(document)) {
    initialize();
  }

  explicit DocumentPayloadSerializer(const Document& document)
      : document_(&document) {
    initialize();
  }

  bool HasData() override;
  std::byte GetNextByte() override;
  std::size_t GetRequiredSize() const override;

private:
  void initialize();
  const Document& getDocument() const;

  //! \brief The document to be stored, can be owned or not.
  std::variant<std::unique_ptr<Document>, const Document*> document_;

  std::size_t current_index_ = 0;

  lightning::memory::MemoryBuffer<std::byte> buffer_;
};

}  // namespace neversql::internal
