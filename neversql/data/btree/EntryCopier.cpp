//
// Created by Nathaniel Rupprecht on 3/30/24.
//

#include "neversql/data/btree/EntryCopier.h"
// Other files.
#include "neversql/data/internals/SpanPayloadSerializer.h"

namespace neversql::internal {

EntryCopier::EntryCopier(std::byte flags, std::span<const std::byte> payload)
    : EntryCreator(std::make_unique<SpanPayloadSerializer>(payload), GetIsEntrySizeSerialized(flags))
    , flags_(flags & std::byte {0xFF}) {}

}  // namespace neversql::internal