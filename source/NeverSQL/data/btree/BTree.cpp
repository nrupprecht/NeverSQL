#include "NeverSQL/data/btree/BTree.h"
// Other files.

namespace neversql {

void BTreeManager::AddValue(primary_key_t key, std::span<const std::byte> value) {
  LOG_SEV(Debug) << "Adding value with key " << key << " to the B-tree.";

  // Search for the leaf node where the key should be inserted.
  auto result = search(key);
  NOSQL_ASSERT(result.node, "could not find node to add element to");

  if (auto handler = LOG_HANDLER_FOR(lightning::Global::GetLogger(), Trace)) {
    handler << "Search path (root is " << index_page_ << "):";
    for (std::size_t i = 0; i < result.path.Size(); ++i) {
      handler << lightning::NewLineIndent << "  * Page " << *result.path[i];
    }
    if (result.node) {
      handler << lightning::NewLineIndent << "Found node is " << result.node->GetPageNumber() << ".";
    }
  }

  // Check if we can add the element to the node (without re-balancing).

  // For now, don't do anything fancy, just check if there is enough de-fragmented space to add the
  // element.
  // TODO: More complex strategies could include vacuuming, looking for fragmented free space, etc.
  auto space_available = result.node->GetDefragmentedFreeSpace();
  auto necessary_space = sizeof(page_size_t) + sizeof(primary_key_t) + sizeof(entry_size_t) + value.size();
  auto num_elements = result.node->GetNumPointers();
  LOG_SEV(Trace) << "Free space in node " << result.node->GetPageNumber() << " is " << space_available
                 << " bytes. Number of elements is " << num_elements << ". Total size of this entry is "
                 << necessary_space << " bytes.";

  if (necessary_space <= space_available && num_elements + 1 <= max_entries_per_page_) {
    // TODO: Return expected type, or some more detailed info, generally, this will fail b/c of key
    //  uniqueness violations.
    NOSQL_ASSERT(
        addElementToNode(*result.node, key, value),
                 "could not add element to node " << result.node->GetPageNumber() << " with pk " << key
                                                  << ", but this should be possible");
  }
  else {
    // Else, we have to split the node and re-balance the tree.
    LOG_SEV(Trace) << "Not enough free space, node " << result.node->GetPageNumber() << " must be split.";

    splitNode(*result.node, result, StoreData {.key = key, .serialized_value = value});

    // Sanity check.
    auto&& header = result.node->GetHeader();
    NOSQL_ASSERT(!header.IsPointersPage() || header.GetAdditionalData() != 0,
                 "page " << result.node->GetPageNumber()
                         << " is a pointers page with no additional data, there must be a right pointer");
  }
}

void BTreeManager::AddValue(std::span<const std::byte> value) {
  LOG_SEV(Debug) << "Adding value to the B-tree with auto-incrementing key.";

  // Get the next primary key.
  auto next_key = getNextPrimaryKey();
  // Add the value with the next primary key.
  AddValue(next_key, value);
}

primary_key_t BTreeManager::getNextPrimaryKey() const {
  auto root = loadNodePage(index_page_);
  primary_key_t pk {};

  auto counter_offset = root->GetHeader().GetReservedStart();
  pk = root->GetPage().Read<primary_key_t>(counter_offset);

  primary_key_t next_primary_key = pk + 1;
  root->GetPage().WriteToPage(counter_offset, next_primary_key);

  LOG_SEV(Trace) << "Next primary key is " << pk << ".";
  return pk;
}

BTreeNodeMap BTreeManager::newNodePage(BTreePageType type, page_size_t reserved_space) const {
  BTreeNodeMap node_map(page_cache_->GetNewPage());
  node_map.GetHeader().InitializePage(node_map.GetPageNumber(), type, reserved_space);
  return node_map;
}

std::optional<BTreeNodeMap> BTreeManager::loadNodePage(page_number_t page_number) const {
  BTreeNodeMap node(page_cache_->GetPage(page_number));

  auto&& header = node.GetHeader();

  // Make sure the magic number is correct. This is an assert, because if it's not correct, something is
  // very wrong with the database itself.
  NOSQL_ASSERT(header.GetMagicNumber() == ToUInt64("NOSQLBTR"),
               "invalid magic number in page " << page_number << " expected " << ToUInt64("NOSQLBTR")
                                               << ", got " << header.GetMagicNumber());
  // Another sanity check.
  NOSQL_ASSERT(header.GetPageNumber() == page_number,
               "page number mismatch, expected " << page_number << ", got " << header.GetPageNumber());
  NOSQL_ASSERT(
      !header.IsPointersPage() || header.GetAdditionalData() != 0,
      "page " << page_number << " is a pointers page with no additional data, there must be a right pointer");

  return node;
}

bool BTreeManager::addElementToNode(BTreeNodeMap& node_map, const StoreData& data, bool unique_keys) const {
  auto header = node_map.GetHeader();
  LOG_SEV(Debug) << "Adding element with pk = " << data.key << " to page " << node_map.GetPageNumber()
                 << ", data size is " << data.serialized_value.size()
                 << " bytes, unique-keys = " << unique_keys << ".";

  // =======================================
  // Check if there is enough free space to add the data.
  // Must store:
  // ============ Pointer space ============
  // Offset to value: sizeof(page_size_t)
  // ============   Cell space  ============
  // Primary key: sizeof(primary_key_t)
  // Size of value: sizeof(entry_size_t) [if store_size is true]
  // Value: serialized_value.size()
  // =======================================

  if (unique_keys) {
    if (auto offset = node_map.getCellLowerBoundByPK(data.key)) {
      // If the key is already in the node, we cannot add it again.
      auto cell = node_map.getCell(*offset);
      bool found_key = std::visit([data](auto&& cell) { return cell.key == data.key; }, cell);
      if (found_key) {
        LOG_SEV(Trace) << "Key " << data.key << " already in node on page " << header.GetPageNumber() << ".";
        return false;
      }
    }
  }

  auto& page = node_map.GetPage();

  // TODO: If the keys have variable sizes, this needs to change.
  // TODO: If we allow for overflow pages, this needs to change.
  auto pointer_space = sizeof(page_size_t);
  auto cell_space = sizeof(primary_key_t) + (data.serialize_data_size ? sizeof(entry_size_t) : 0)
      + data.serialized_value.size();
  auto required_space = pointer_space + cell_space;
  LOG_SEV(Trace) << "Entry will take up " << pointer_space << " bytes of pointer space and " << cell_space
                 << " bytes of cell space, for a total of " << required_space << " bytes.";

  // Check whether we would need an overflow page.
  // TODO: Implement overflow pages.

  if (auto defragmented_space = header.GetDefragmentedFreeSpace(); defragmented_space < required_space) {
    LOG_SEV(Trace) << "Not enough space to add element to node " << node_map.GetPageNumber()
                   << ", required space was " << required_space << ", defragmented space was "
                   << defragmented_space << "." << lightning::NewLineIndent
                   << "Page number: " << header.GetPageNumber();
    return false;
  }

  auto entry_end_offset = header.GetFreeEnd();
  // Cell needs cell_space bytes.
  auto entry_start_offset = entry_end_offset - cell_space;

  auto offset = static_cast<page_size_t>(entry_start_offset);

  // =======================================
  // Cell layout (leaf cell):
  // [primary-key][size-of-value][value]
  // =======================================

  // If the page is set up to use fixed primary_key_t length keys, just write the key.
  if (header.IsUInt64Key()) {
    offset = page.WriteToPage(offset, data.key);
  }
  // Otherwise, write the size of the key and the key.
  // TODO: What if the key has a different type? Investigate alternatives later on.
  else {
    NOSQL_FAIL("unhandled case, keys must be of type primary_key_t, and pages must use fixed size keys");
  }

  if (data.serialize_data_size) {
    // Write the size of the value.
    // TODO: What to do if we need an overflow page. This obviously only works as is if we are storing the
    //  whole entry here.
    auto data_size = static_cast<entry_size_t>(data.serialized_value.size());
    offset = page.WriteToPage(offset, data_size);
  }

  offset = page.WriteToPage(offset, data.serialized_value);

  // Make sure we wrote the correct amount of data.
  // clang-format off
  NOSQL_ASSERT(offset == entry_end_offset,
               "incorrect amount of data written to node, expected " << required_space << " bytes, wrote "
               << (offset - entry_start_offset) << " bytes");
  // clang-format on

  // Added the cell.
  header.SetFreeEnd(static_cast<page_size_t>(header.GetFreeEnd() - cell_space));
  page.WriteToPage(header.GetFreeStart(), header.GetFreeEnd());
  header.SetFreeBegin(header.GetFreeStart() + sizeof(page_size_t));

  // Make sure keys are all in ascending order. Only need to do this if the keys are not already sorted
  // (i.e. this was not a rightmost append).
  if (auto greatest_pk = node_map.GetLargestKey(); greatest_pk && data.key < *greatest_pk) {
    node_map.sortKeys();
  }

  return true;
}

void BTreeManager::splitNode(BTreeNodeMap& node, SearchResult& result, std::optional<StoreData> data) {
  LOG_SEV(Debug) << "Splitting node on page " << node.GetPageNumber() << ".";
  if (node.GetHeader().IsRootPage()) {
    LOG_SEV(Trace) << "  * Splitting root node.";
    splitRoot(data);
    return;
  }

  // Split the node.
  auto split_data = splitSingleNode(node, data);
  LOG_SEV(Trace) << "  * Split node " << node.GetPageNumber() << " into left page " << split_data.left_page
                 << " and right page " << split_data.right_page << ".";
  result.path.Pop();

  // Left page is the original page, right page has to be added to the parent.
  auto parent_page_number = result.path.Top();
  LOG_SEV(Trace) << "  * Adding right page " << split_data.right_page << " to parent page "
                 << parent_page_number << ".";

  auto parent = loadNodePage(parent_page_number);
  NOSQL_ASSERT(parent, "could not find parent node");

  if (!addElementToNode(*parent, split_data.split_key, split_data.left_page, false /* DON'T store size */)) {
    // If there is not enough space to add the new right page to the parent, we have to split the parent.

    LOG_SEV(Trace) << "  * Parent node " << parent_page_number << " is full, splitting it.";
    StoreData entry_to_add {
        .key = split_data.split_key,
        .serialized_value = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&split_data.left_page), sizeof(page_number_t)),
        .serialize_data_size = false};

    splitNode(*parent, result, entry_to_add);
  }

  NOSQL_ASSERT(!parent->IsPointersPage() || parent->GetHeader().GetAdditionalData() != 0,
               "page " << parent_page_number
                       << " is a pointers page with no additional data, there must be a right pointer");
}

SplitPage BTreeManager::splitSingleNode(BTreeNodeMap& node, std::optional<StoreData> data) {
  // Balanced split: create a new page, move half of the elements to the new page.
  // Unbalanced split: move all, or almost all, elements to the new page. Most efficient for adding
  //  consecutive keys.

  auto&& header = node.GetHeader();
  LOG_SEV(Debug) << "Splitting node on page " << node.GetPageNumber() << " with " << node.GetNumPointers()
                 << " pointers.";

  // We can use this function to split leaf or interior nodes.
  auto new_node = newNodePage(node.GetType(), 0);

  SplitPage return_data {.left_page = new_node.GetPageNumber(), .right_page = node.GetPageNumber()};

  // Divide elements between the two nodes.
  page_size_t num_elements = node.GetNumPointers();
  page_size_t num_elements_to_move = num_elements - 1;

  // Interior node.
  auto pointers = node.getPointers();

  // Get the split key.
  if (node.IsPointersPage()) {
    // New rightmost pointer for the left cell is the rightmost pointer. This used to be in a cell, now we
    // move it to be the rightmost pointer. The value that this cell corresponded to will be bubbled up to be
    // the split value in the parent.
    auto pointers_cell = std::get<PointersNodeCell>(
        node.getCell(pointers[static_cast<unsigned long>(num_elements_to_move - 1)]));
    new_node.GetHeader().SetAdditionalData(pointers_cell.page_number);  // TODO: WriteToPage.

    return_data.split_key = pointers_cell.key;
  }
  else {
    auto data_cell =
        std::get<DataNodeCell>(node.getCell(pointers[static_cast<unsigned long>(num_elements_to_move - 1)]));
    return_data.split_key = data_cell.key;
  }
  LOG_SEV(Trace) << "Split key will be " << return_data.split_key << ".";

  // Move the low nodes to the new node.
  // That way, we can just add the new node with the split key as a single cell to the parent.
  // We do not have to do anything special about the right page, because if it was the rightmost page, it
  // stays the rightmost page, and otherwise, it's cell is still valid.
  for (auto i = 0; i < num_elements_to_move; ++i) {
    auto cell = node.getCell(pointers[static_cast<unsigned long>(i)]);
    // clang-format off
    std::visit([&new_node, this](auto&& cell) {
          using T = std::decay_t<decltype(cell)>;
          if constexpr (std::is_same_v<T, PointersNodeCell>)
            addElementToNode(new_node, cell.key, cell.page_number, false /* DON'T store size */, true);
          else if constexpr (std::is_same_v<T, DataNodeCell>)
            addElementToNode(new_node, cell.key, cell.SpanValue(), true /* DO store size */, true);
          else NOSQL_FAIL("unhandled case");
        }, cell);
    // clang-format on
  }

  // =======================================
  //  Compress the pointers in the original node.
  // =======================================

  // Span of the offsets to elements that were not moved to the next page.

  std::span<const page_size_t> remaining_pointers(pointers.data() + num_elements_to_move,
                                                  pointers.size() - num_elements_to_move);
  std::vector<page_size_t> pointers_copy(remaining_pointers.size());
  std::ranges::copy(remaining_pointers, pointers_copy.begin());
  // Span for the vector
  remaining_pointers = pointers_copy;
  node.GetPage().WriteToPage(header.GetPointersStart(), remaining_pointers);

  // "Free" the rightmost num_elements_to_move pointers in the original node.
  // TODO: Create a linked list of blocks of newly freed space?
  header.SetFreeBegin(header.GetFreeStart()
                      - (static_cast<page_size_t>(num_elements_to_move) * sizeof(page_size_t)));

  // =======================================
  // Potentially add data.
  // =======================================

  if (data) {
    LOG_SEV(Trace) << "Data requested to be added to a node, pk = " << data->key << ".";
    if (data->key <= return_data.split_key) {
      // Add to left node.
      addElementToNode(new_node, data->key, data->serialized_value);
    }
    else {
      // Add to right node.
      addElementToNode(node, data->key, data->serialized_value);
    }
  }

  // =======================================
  // Write back node and new node.
  // =======================================

  vacuum(node);

  LOG_SEV(Trace) << "  * After split, original node has " << node.GetDefragmentedFreeSpace()
                 << " bytes of de-fragmented free space.";
  LOG_SEV(Trace) << "  * After split, new node has " << new_node.GetDefragmentedFreeSpace()
                 << " bytes of de-fragmented free space.";

  {
    auto&& check_header = node.GetHeader();
    NOSQL_ASSERT(!check_header.IsPointersPage() || check_header.GetAdditionalData() != 0,
                 "page " << node.GetPageNumber()
                         << " is a pointers page with no additional data, there must be a right pointer");
  }
  {
    auto&& check_header = new_node.GetHeader();
    NOSQL_ASSERT(!check_header.IsPointersPage() || check_header.GetAdditionalData() != 0,
                 "page " << new_node.GetPageNumber()
                         << " is a pointers page with no additional data, there must be a right pointer");
  }

  return return_data;
}

void BTreeManager::splitRoot(std::optional<StoreData> data) {
  LOG_SEV(Debug) << "Splitting root node.";

  // Create two child pages, spit the nodes between them.
  auto root = loadNodePage(index_page_);
  auto&& root_header = root->GetHeader();
  NOSQL_ASSERT(root, "could not find root node");

  // The type of child trees that we need depends on the type of the root node.
  BTreePageType child_type = root->IsPointersPage() ? BTreePageType::Internal : BTreePageType::Leaf;

  // We will write back these nodes below.
  auto left_child = newNodePage(child_type, 0);
  auto right_child = newNodePage(child_type, 0);
  auto left_page_number = left_child.GetPageNumber();
  auto right_page_number = right_child.GetPageNumber();

  LOG_SEV(Trace) << "Created left and right children with page numbers " << left_page_number << " and "
                 << right_page_number << ".";

  // Put all but one pointer in the left tree.
  page_size_t num_for_left = root->GetNumPointers() - 1;
  auto split_key = root->getKeyForNthCell(num_for_left);
  LOG_SEV(Trace) << "Split key will be " << split_key << ".";

  for (page_size_t i = 0; i < root->GetNumPointers(); ++i) {
    auto cell = root->getNthCell(i);
    auto& node_to_add_to = i <= num_for_left ? left_child : right_child;
    std::visit(
        [&](auto&& cell) {
          using T = std::decay_t<decltype(cell)>;
          if constexpr (std::is_same_v<T, PointersNodeCell>) {
            if (i == num_for_left) {
              // Add this page as the rightmost pointer.
              node_to_add_to.GetHeader().SetAdditionalData(cell.page_number);
              LOG_SEV(Trace) << "Setting the rightmost pointer in the right child (P"
                             << node_to_add_to.GetPageNumber() << ") to " << cell.page_number << ".";
            }
            else {
              NOSQL_ASSERT(
                  addElementToNode(
                      node_to_add_to, cell.key, cell.page_number, false /* DON'T store size */, true),
                  "we should be able to add to this cell");
            }
          }
          else if constexpr (std::is_same_v<T, DataNodeCell>) {
            NOSQL_ASSERT(
                addElementToNode(node_to_add_to, cell.key, cell.SpanValue(), true /* DO store size */, true),
                "we should be able to add to this cell");
          }
          else {
            NOSQL_FAIL("unhandled case");
          }
        },
        cell);
  }

  // If the root was a pointers page, we need to set the rightmost pointer in the root to the right child.
  if (root_header.IsPointersPage()) {
    right_child.GetHeader().SetAdditionalData(root_header.GetAdditionalData());
    LOG_SEV(Trace) << "Setting the rightmost pointer in the right child (P" << right_child.GetPageNumber()
                   << ") to " << root_header.GetAdditionalData() << ".";
  }

  // Add data, if any.
  if (data) {
    LOG_SEV(Trace) << "Data requested to be added to a node, pk = " << data->key << ".";
    auto& node_to_add_to = data->key <= split_key ? left_child : right_child;
    // Only store the size of the root was NOT a pointers page (meaning we expect data to be stored, not
    // pointers).
    addElementToNode(node_to_add_to, *data, !root->IsPointersPage());
    LOG_SEV(Debug) << "Added the data to node on page " << node_to_add_to.GetPageNumber() << ".";
  }

  // Clear the entire root page.
  root_header.SetFreeBegin(root_header.GetPointersStart());
  root_header.SetFreeEnd(root_header.GetReservedStart());

  // Set the root page to be a pointers page.
  root_header.SetFlags(root_header.GetFlags() | 0b1);

  // Add the two child pages to the root page.
  addElementToNode(*root, split_key, left_page_number, false /* DON'T store size */, true);
  // The right page is the "rightmost" pointer.
  root_header.SetAdditionalData(right_page_number);
  LOG_SEV(Trace) << "Set the rightmost pointer in the root node to " << right_page_number << ".";
}

void BTreeManager::vacuum(BTreeNodeMap& node) const {
  // TODO: This is not actually a hard requirement, but I need to write the fallback.
  NOSQL_REQUIRE(node.GetNumPointers() < 256,
                "vacuuming not implemented for nodes with more than 256 pointers");

  LOG_SEV(Debug) << "Vacuuming node on page " << node.GetPageNumber() << ". Node has "
                 << node.GetDefragmentedFreeSpace() << " bytes of defragmented free space.";

  // TODO: More WAL friendly version: take a snapshot of the pointers, sort them (without going through the
  // WAL), then
  //  submit the sorted changes all at once.

  auto&& header = node.GetHeader();

  auto& page = node.GetPage();

  std::pair<page_size_t, page_size_t> offsets[256];
  page_size_t num_pointers = node.GetNumPointers();
  auto pointers = node.getPointers();
  for (page_size_t i = 0; i < num_pointers; ++i) {
    offsets[i] = {pointers[i], i};
  }

  page_size_t next_point = header.GetReservedStart();
  std::sort(offsets, offsets + num_pointers, std::greater {});
  for (page_size_t i = 0; i < num_pointers; ++i) {
    auto [offset, pointer_number] = offsets[i];
    // Move the cell to the rightmost position possible.
    auto cell = node.getCell(offset);
    // Get the size of the cell.
    auto cell_size = std::visit([](auto&& cell) { return cell.GetSize(); }, cell);

    // Adjust the next point to be at the start of where the cell must be copied.
    next_point -= cell_size;

    LOG_SEV(Trace) << "  * Moving cell " << i << " from offset " << offset << " to offset " << next_point
                   << " (cell size " << cell_size << ").";

    // Copy the cell to the new location.
    page.MoveInPage(offset, next_point, cell_size);

    // Update the pointer.
    page.WriteToPage(header.GetPointersStart() + (pointer_number * sizeof(page_size_t)), next_point);
  }
  // Set the updated free-end location.
  header.SetFreeEnd(next_point);

  LOG_SEV(Debug) << "Finished vacuuming node on page " << node.GetPageNumber() << ". Node now has "
                 << node.GetDefragmentedFreeSpace() << " bytes of defragmented free space.";
}

SearchResult BTreeManager::search(primary_key_t key) const {
  SearchResult result;

  auto node = [this] {
    auto root = loadNodePage(index_page_);
    NOSQL_ASSERT(root, "could not find root node");
    return std::move(*root);
  }();

  result.path.Push(node.GetPageNumber());
  // Loop until found. Since this is a (presumably well-formed) B-tree, this should always terminate.
  for (;;) {
    if (!node.IsPointersPage()) {
      // Elements are allocated directly in this page.
      result.node = std::move(node);
      break;
    }

    auto next_page_number = node.searchForNextPageInPointersPage(key);

    NOSQL_REQUIRE(next_page_number != node.GetPageNumber(), "infinite loop detected in search");

    result.path.Push(next_page_number);
    auto child = loadNodePage(next_page_number);
    node = std::move(*child);
  }

  return result;
}

}  // namespace neversql