#include "NeverSQL/data/btree/BTree.h"
// Other files.

namespace neversql {

BTreeNodeMap BTreeManager::newNodePage(BTreePageType type) const {
  auto page = data_access_layer_->GetNewPage();
  auto page_size = page.GetPageSize();
  BTreeNodeMap node_map(std::move(page));

  BTreePageHeader& header = node_map.GetHeader();
  header.magic_byte = ToUInt67("NOSQLBTR");  // Set the magic number.
  header.page_number = page.GetPageNumber();
  header.flags = static_cast<uint8_t>(type);

  // Right now, not allocating any reserved space.
  header.free_start = sizeof(BTreePageHeader);
  header.reserved_start = page_size;
  header.free_end = header.reserved_start;

  // Write the node back to the file.
  writeBack(node_map);

  return node_map;
}

std::optional<BTreeNodeMap> BTreeManager::loadNodePage(page_number_t page_number) const {
  if (auto page = data_access_layer_->GetPage(page_number)) {
    BTreeNodeMap node(std::move(*page));
    // Make sure the magic number is correct. This is an assert, because if it's not correct, something is
    // very wrong with the database itself.
    NOSQL_ASSERT(node.GetHeader().magic_byte == ToUInt67("NOSQLBTR"), "invalid magic number in page");
    return node;
  }
  return {};
}

bool BTreeManager::addElementToNode(BTreeNodeMap& node_map,
                                    primary_key_t key,
                                    std::span<const std::byte> serialized_value) {
  BTreePageHeader& header = node_map.GetHeader();

  // Check if there is enough free space to add the data.
  // Must store:
  // ============ Pointer space ============
  // Offset to value: sizeof(page_size_t)
  // ============   Cell space  ============
  // Primary key: sizeof(primary_key_t)
  // Size of value: sizeof(entry_size_t)
  // Value: serialized_value.size()
  // =======================================

  // TODO: If the keys have variable sizes, this needs to change.
  // TODO: If we allow for overflow pages, this needs to change.
  auto pointer_space = sizeof(page_size_t);
  auto cell_space = sizeof(primary_key_t) + sizeof(entry_size_t) + serialized_value.size();
  auto required_space = pointer_space + cell_space;

  // Check whether we would need an overflow page.
  // TODO: Implement overflow pages.

  if (auto defragmented_space = header.GetDefragmentedFreeSpace(); defragmented_space < required_space) {
    LOG_SEV(Trace) << "Not enough space to add element to node, required space was " << required_space
                   << ", defragmented space was " << defragmented_space << "." << lightning::NewLineIndent
                   << "Page number: " << header.page_number;
    return false;
  }

  auto entry_end = header.free_end;
  auto* entry_end_ptr = node_map.page_.GetData() + entry_end;
  // Cell needs cell_space bytes.
  auto* entry_start = entry_end_ptr - cell_space;

  std::byte* ptr = entry_start;

  // =======================================
  // Cell layout (leaf cell):
  // [primary-key][size-of-value][value]
  // =======================================

  // If the page is set up to use fixed primary_key_t length keys, just write the key.
  if (header.IsUInt64Key()) {
    // Write the key.
    std::memcpy(ptr, reinterpret_cast<const std::byte*>(&key), sizeof(primary_key_t));
    ptr += sizeof(primary_key_t);
  }
  // Otherwise, write the size of the key and the key.
  // TODO: What if the key has a different type? Investigate alternatives later on.
  else {
    NOSQL_FAIL("unhandled case, keys must be of type primary_key_t, and pages must use fixed size keys");
  }

  // Write the size of the value.
  // TODO: What to do if we need an overflow page. This obviously only works as is if we are storing the whole
  //  entry here.
  auto data_size = static_cast<entry_size_t>(serialized_value.size());
  std::memcpy(ptr, reinterpret_cast<const std::byte*>(&data_size), sizeof(entry_size_t));
  ptr += sizeof(entry_size_t);

  // Write the value.
  std::ranges::copy(serialized_value, ptr);
  ptr += serialized_value.size();

  // Make sure we wrote the correct amount of data.
  // clang-format off
  NOSQL_ASSERT(ptr == entry_end_ptr,
               "incorrect amount of data written to node, expected " << required_space << " bytes, wrote "
               << (ptr - entry_start) << " bytes");
  // clang-format on

  // Where to write the next pointer.
  auto* next_ptr = header.GetNextPointer();

  // Added the cell.
  header.free_end -= cell_space;  // Cell size.
  // Store a new pointer with that offset.
  *next_ptr = header.free_end;
  std::memcpy(next_ptr, reinterpret_cast<const std::byte*>(&header.free_end), sizeof(header.free_end));
  header.free_start += sizeof(page_size_t);  // Added an offset pointer to the start of free space.

  // Make sure keys are all in ascending order. Only need to do this if the keys are not already sorted
  // (i.e. this was not a rightmost append).
  if (auto greatest_pk = node_map.GetLargestKey(); greatest_pk && key < *greatest_pk) {
    node_map.sortKeys();
  }

  // TODO: Don't always write back pages immediately?
  writeBack(node_map);

  return true;
}

void BTreeManager::writeBack(const BTreeNodeMap& node_map) const {
  data_access_layer_->WriteBackPage(node_map.page_);
}

SearchResult BTreeManager::search(primary_key_t key) const {
  // TODO/NOTE(Nate): I have not tested this function yet.

  SearchResult result;

  auto root = [this] {
    auto root = loadNodePage(index_page_);
    NOSQL_ASSERT(root, "could not find root node");
    return std::move(*root);
  }();

  BTreeNodeMap node = std::move(root);
  result.path.Push(node.GetPageNumber());
  // Loop until found. Since this is a (presumably well-formed) B-tree, this should always terminate.
  for (;;) {
    if (node.IsLeaf()) {
      result.node = std::move(node);
      break;
    }

    // Look for the child node that would contain the key.
    // Find the first cell with a PK >= key.
    if (auto offset = node.getCellLowerBoundByPK(key)) {
      // Load the child node.
      auto interior_cell = std::get<InteriorNodeCell>(node.getCell(*offset));
      auto child = loadNodePage(interior_cell.page_number);
      result.path.Push(interior_cell.page_number);
      NOSQL_ASSERT(child, "could not load child node");
      node = std::move(*child);
    }
    // Could not find a child.
    break;
  }

  return result;
}

}  // namespace neversql