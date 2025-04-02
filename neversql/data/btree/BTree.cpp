//
// Create by Nathaniel Rupprecht
//

#include "neversql/data/btree/BTree.h"
// Other files.
#include "neversql/data/btree/EntryCopier.h"
#include "neversql/data/internals/DatabaseEntry.h"
#include "neversql/data/internals/KeyComparison.h"
#include "neversql/data/internals/KeyPrinting.h"
#include "neversql/data/internals/SpanPayloadSerializer.h"
#include "neversql/data/internals/Utility.h"

namespace neversql {

// ================================================================================================
//  BTreeManager::Iterator.
// ================================================================================================

BTreeManager::Iterator::Iterator(const BTreeManager& manager)
    : manager_(&manager) {
  auto root = manager_->loadNodePage(manager.GetRootPageNumber());
  // If the tree is empty, begin is end.
  if (root->GetNumPointers() != 0) {
    progress_.Push({manager.GetRootPageNumber(), 0});
    descend(*root, 0);
  }
}

BTreeManager::Iterator::Iterator(const BTreeManager& manager,
                                 FixedStack<std::pair<page_number_t, page_size_t>> progress)
    : manager_(&manager)
    , progress_(std::move(progress)) {}

//! \brief Create an end B-Tree iterator
BTreeManager::Iterator::Iterator(const BTreeManager& manager, [[maybe_unused]] bool)
    : manager_(&manager) {}

BTreeManager::Iterator& BTreeManager::Iterator::operator++() {
  if (done()) {
    return *this;
  }

  auto& [current_page_number, current_index] = progress_.Top()->get();
  auto current_page = *manager_->loadNodePage(current_page_number);
  current_index++;
  // There is no more data in the current data page.
  if (current_page.GetNumPointers() <= current_index) {
    progress_.Pop();

    while (!done()) {
      auto& [page_number, index] = progress_.Top()->get();
      auto page = *manager_->loadNodePage(page_number);
      ++index;
      // Note: index can be == num pointers, since this means go to the "rightmost page."
      if (index <= page.GetNumPointers()) {
        descend(page, index);
        break;
      }
      progress_.Pop();
    }
  }
  return *this;
}

BTreeManager::Iterator BTreeManager::Iterator::operator++(int) {
  auto it = *this;
  ++(*this);
  return it;
}

std::unique_ptr<internal::DatabaseEntry> BTreeManager::Iterator::operator*() const {
  if (done()) {
    return {};
  }

  auto [page_number, cell_index] = progress_.Top()->get();
  auto node = *manager_->loadNodePage(page_number);
  auto cell = node.getNthCell(cell_index);
  // Should be a data cell.
  NOSQL_ASSERT(std::holds_alternative<DataNodeCell>(cell), "Cell is not a data cell.");

  const auto cell_offset = node.getCellOffsetByIndex(cell_index);

  return internal::ReadEntry(cell_offset, std::move(node.GetPage()), manager_);
}

bool BTreeManager::Iterator::operator==(const Iterator& other) const {
  return progress_ == other.progress_ || (done() && other.done());
}

bool BTreeManager::Iterator::operator!=(const Iterator& other) const {
  return !(*this == other);
}

bool BTreeManager::Iterator::done() const noexcept {
  return !manager_ || progress_.Empty();
}

void BTreeManager::Iterator::descend(const BTreeNodeMap& page, page_size_t index) {
  if (!page.IsPointersPage()) {
    return;
  }

  // if the index is == num_pointers, go to the "rightmost" page.
  page_number_t next_page_number {};
  if (index == page.GetNumPointers()) {
    next_page_number = page.getHeader().GetAdditionalData();
  }
  else {
    next_page_number = std::get<PointersNodeCell>(page.getNthCell(index)).page_number;
  }

  auto descending_page = *manager_->loadNodePage(next_page_number);
  index = 0;

  progress_.Push({next_page_number, index});
  while (descending_page.IsPointersPage()) {
    next_page_number = std::get<PointersNodeCell>(descending_page.getNthCell(index)).page_number;
    descending_page = *manager_->loadNodePage(next_page_number);
    progress_.Push({next_page_number, index});
  }
}

// ================================================================================================
//  BTreeManager.
// ================================================================================================

BTreeManager::BTreeManager(page_number_t root_page, PageCache& page_cache)
    : page_cache_(page_cache)
    , root_page_(root_page)
    , cmp_(internal::CompareTrivial<primary_key_t>)
    , debug_key_func_(internal::PrintUInt64) {
  // Initialize the tree from its root page.
  initialize();
}

std::unique_ptr<BTreeManager> BTreeManager::CreateNewBTree(PageCache& page_cache, DataTypeEnum key_type) {
  BTreeNodeMap root_node(page_cache.GetNewPage());

  Transaction transaction {0};  // TODO

  // Set the comparison and string debug functions. These are a B-tree property, not stored per-page.

  // === Reserved space. =================================================================================
  // 1 byte [Key type enum] int8_t
  // 1 byte [Flags] uint8_t
  // 8 byte Current overflow page number
  // 8 byte Next overflow page key
  // === If using auto-incrementing keys, space for the key. This is only possible with primary_key_t. ===
  // 8 byte (optional) [Auto-incrementing key] primary_key_t
  page_size_t reserved_space = 2 + 2 * sizeof(primary_key_t);
  if (key_type == DataTypeEnum::UInt64) {
    reserved_space += sizeof(primary_key_t);
  }

  auto header = root_node.GetHeader();
  header.InitializePage(root_node.GetPageNumber(), BTreePageType::RootLeaf, reserved_space);
  // TODO: Right now, we only support string and uint64_t keys, and so assume that key size is specified if
  //       the key type is a string.
  if (key_type == DataTypeEnum::String) {
    const auto flags = header.GetFlags();
    header.SetFlags(transaction, flags | 0b100);
  }

  LOG_SEV(Trace) << "Root page allocated to be page " << root_node.GetPageNumber() << ".";

  // Write zero into the reserved space.
  auto offset = root_node.GetHeader().GetReservedStart();
  offset = transaction.WriteToPage<int8_t>(*root_node.GetPage(), offset, static_cast<int8_t>(key_type));
  offset = transaction.WriteToPage<uint8_t>(*root_node.GetPage(), offset, 0);
  if (key_type == DataTypeEnum::UInt64) {
    transaction.WriteToPage<primary_key_t>(*root_node.GetPage(), offset, 0);
  }

  return std::make_unique<BTreeManager>(root_node.GetPageNumber(), page_cache);
}

void BTreeManager::AddValue(GeneralKey key, internal::EntryCreator& entry_creator) {
  LOG_SEV(Debug) << "Adding value with key " << debugKey(key) << " to the B-tree.";

  // Search for the leaf node where the key should be inserted.
  auto result = search(key);
  NOSQL_ASSERT(result.node, "could not find node to add element to");

  if (auto handler = LOG_HANDLER_FOR(lightning::Global::GetLogger(), Trace)) {
    handler << "Search path (root is " << root_page_ << "):";
    for (std::size_t i = 0; i < result.path.Size(); ++i) {
      handler << lightning::NewLineIndent << "  * Page " << result.path[i]->first << ", index "
              << result.path[i]->second << ".";
    }
    if (result.node) {
      handler << lightning::NewLineIndent << "Found node is " << result.node->GetPageNumber() << ".";
    }
  }

  // Check if we can add the element to the node (without re-balancing).

  // TODO: Use GetSpaceRequirements

  // For now, don't do anything fancy, just check if there is enough de-fragmented space to add the
  // element.
  // TODO: More complex strategies could include vacuuming, looking for fragmented free space, etc.
  auto space_available = result.node->GetDefragmentedFreeSpace();
  // Cell offset, entry size, and the entry itself.
  auto necessary_space = sizeof(page_size_t) + entry_creator.GetMinimumEntrySize();
  if (!entry_creator.GetNeedsOverflow()) {
    // Serialize entry size.
    necessary_space += sizeof(entry_size_t);
  }
  // Space required for the key.
  if (serialize_key_size_) {
    necessary_space += sizeof(uint16_t);
  }
  necessary_space += key.size();

  auto num_elements = result.node->GetNumPointers();
  LOG_SEV(Trace) << "Free space in node " << result.node->GetPageNumber() << " is " << space_available
                 << " bytes. Number of elements is " << num_elements << ". Total size of this entry is "
                 << necessary_space << " bytes.";

  // We must have at least `space_available` space to add an entry to this page.
  if (min_space_for_entry_ <= space_available && necessary_space <= space_available
      && num_elements + 1 <= max_entries_per_page_)
  {
    // TODO: Return expected type, or some more detailed info, generally, this will fail b/c of key
    //  uniqueness violations.
    StoreData store_data {.key = key,
                          .entry_creator = &entry_creator,
                          .serialize_key_size = serialize_key_size_,
                          .serialize_data_size = true};
    NOSQL_ASSERT(addElementToNode(*result.node, store_data),
                 "could not add element to node " << result.node->GetPageNumber() << " with pk "
                                                  << debugKey(key) << ", but this should be possible");
  }
  else {
    // Else, we have to split the node and re-balance the tree.
    LOG_SEV(Trace) << "Not enough free space, node " << result.node->GetPageNumber() << " must be split.";

    StoreData store_data {.key = key, .entry_creator = &entry_creator};
    splitNode(*result.node, result, store_data);

    // Sanity check.
    auto&& header = result.node->GetHeader();
    NOSQL_ASSERT(!header.IsPointersPage() || header.GetAdditionalData() != 0,
                 "page " << result.node->GetPageNumber()
                         << " is a pointers page with no additional data, there must be a right pointer");
  }
}

void BTreeManager::AddValue(internal::EntryCreator& entry_creator) {
  NOSQL_REQUIRE(key_type_ == DataTypeEnum::UInt64,
                "cannot add value with auto-incrementing key to B-tree with non-uint64_t key type");

  LOG_SEV(Debug) << "Adding value to the B-tree with auto-incrementing key.";

  // Get the next primary key.
  auto next_key = getNextPrimaryKey();
  auto key_span = internal::SpanValue(next_key);

  // Add the value with the next primary key.
  AddValue(key_span, entry_creator);
}

void BTreeManager::initialize() {
  auto root = loadNodePage(root_page_);

  // Get the key type from the root page.
  key_type_ = static_cast<DataTypeEnum>(root->GetPage()->Read<int8_t>(root->GetHeader().GetReservedStart()));

  serialize_key_size_ = key_type_ == DataTypeEnum::String;

  // Get default key comparison and debug functions.
  if (key_type_ == DataTypeEnum::UInt64) {
    cmp_ = internal::CompareTrivial<primary_key_t>;
    debug_key_func_ = internal::PrintUInt64;
  }
  else if (key_type_ == DataTypeEnum::String) {
    cmp_ = internal::CompareString;
    debug_key_func_ = internal::PrintString;
  }
  else {
    // TODO: Implement for other key types.
    NOSQL_FAIL("unsupported key type");
  }
}

primary_key_t BTreeManager::getNextPrimaryKey() const {
  Transaction transaction {0};  // TODO

  // Only possible if the key type is uint64_t.
  NOSQL_ASSERT(key_type_ == DataTypeEnum::UInt64, "cannot get next primary key for non-uint64_t key type");

  auto root = loadNodePage(root_page_);
  primary_key_t pk {};

  auto offset = 2 + 2 * sizeof(primary_key_t);
  auto counter_offset = static_cast<page_size_t>(root->GetHeader().GetReservedStart() + offset);
  pk = root->GetPage()->Read<primary_key_t>(counter_offset);

  primary_key_t next_primary_key = pk + 1;
  transaction.WriteToPage(*root->GetPage(), counter_offset, next_primary_key);

  LOG_SEV(Trace) << "Next primary key is " << pk << ".";
  return pk;
}

page_number_t BTreeManager::getNextOverflowPage() {
  Transaction transaction {0};  // TODO

  auto root = loadNodePage(root_page_);

  // Get a new page.
  const auto new_page = newNodePage(BTreePageType::OverflowPage, 0);
  // TODO: Any setup needed for the page to be an overflow page? Set some flags?

  current_overflow_page_number_ = new_page.GetPageNumber();

  auto offset = 2;
  const auto counter_offset = static_cast<page_size_t>(root->GetHeader().GetReservedStart() + offset);
  transaction.WriteToPage<page_number_t>(*root->GetPage(), counter_offset, current_overflow_page_number_);

  return current_overflow_page_number_;
}

page_number_t BTreeManager::getCurrentOverflowPage() {
  if (current_overflow_page_number_ == 0) {
    // No overflow page was created yet, get a new one.
    current_overflow_page_number_ = getNextOverflowPage();
  }
  return current_overflow_page_number_;
}

primary_key_t BTreeManager::getNextOverflowEntryNumber() {
  Transaction transaction {0};  // TODO

  auto root = loadNodePage(root_page_);

  ++next_overflow_entry_number_;

  // Next overflow entry is stored right after the flags.
  auto offset = 2 + sizeof(primary_key_t);
  const auto counter_offset = static_cast<page_size_t>(root->GetHeader().GetReservedStart() + offset);
  transaction.WriteToPage<page_number_t>(*root->GetPage(), counter_offset, next_overflow_entry_number_);

  return next_overflow_entry_number_ - 1;
}

BTreeNodeMap BTreeManager::newNodePage(BTreePageType type, page_size_t reserved_space) const {
  BTreeNodeMap node(page_cache_.GetNewPage());
  // Set the comparison and string debug functions. These are a B-tree property, not stored per-page.
  node.cmp_ = cmp_;
  node.debug_key_func_ = debug_key_func_;

  if (type == BTreePageType::OverflowPage) {
    node.GetHeader().InitializeOverflowPage(node.GetPageNumber());
  }
  else {
    node.GetHeader().InitializePage(node.GetPageNumber(), type, reserved_space);
  }
  return node;
}

std::optional<BTreeNodeMap> BTreeManager::loadNodePage(page_number_t page_number) const {
  BTreeNodeMap node(page_cache_.GetPage(page_number));

  // Set the comparison and string debug functions. These are a B-tree property, not stored per-page.
  node.cmp_ = cmp_;
  node.debug_key_func_ = debug_key_func_;

  auto&& header = node.GetHeader();

  // Make sure the magic number is correct. This is an assert, because if it's not correct, something is
  // very wrong with the database itself.
  NOSQL_ASSERT(
      header.GetMagicNumber() == ToUInt64("NOSQLBTR") || header.GetMagicNumber() == ToUInt64("OVERFLOW"),
      "invalid magic number in page " << page_number << " expected " << ToUInt64("NOSQLBTR") << ", got "
                                      << header.GetMagicNumber());
  // Another sanity check.
  NOSQL_ASSERT(header.GetPageNumber() == page_number,
               "page number mismatch, expected " << page_number << ", got " << header.GetPageNumber());
  NOSQL_ASSERT(
      !header.IsPointersPage() || header.GetAdditionalData() != 0,
      "page " << page_number << " is a pointers page with no additional data, there must be a right pointer");

  return node;
}

bool BTreeManager::addElementToNode(BTreeNodeMap& node_map, const StoreData& data, bool unique_keys) {
  Transaction transaction {0};  // TODO

  auto header = node_map.GetHeader();
  auto is_overflow_page = header.IsOverflowPage();
  LOG_SEV(Debug) << "Adding element with pk = " << debugKey(data.key) << " to page "
                 << node_map.GetPageNumber() << ", unique-keys = " << unique_keys << ".";

  auto& entry_creator = *data.entry_creator;

  // =======================================
  // Check if there is enough free space to add the data.
  // Must store:
  // ============ Pointer space ============
  // Offset to value: sizeof(page_size_t)
  // ============   Cell space  ============
  // [flags: 1 byte]
  // [Key size: 2 bytes]? [Key: 8 bytes | variable]
  // ---------------- Entry ----------------
  // [Entry size: 2 bytes]
  // [Entry data: Entry size bytes]
  // =======================================

  if (unique_keys && !isUniqueKey(node_map, data)) {
    return false;
  }

  // Store the current greatest key, so after we add a key, we can check if we need to sort the page.
  auto greatest_pk = node_map.GetLargestKey();

  // Given the current free space and the space needed for the pointer and the other parts of the cell, what
  // is the maximum amount of space available for the entry (not counting any page entry space restrictions).
  auto space_requirements = node_map.CalculateSpaceRequirements(data.key);
  auto maximum_available_space_for_entry = space_requirements.max_entry_space;
  // If this is an overflow page, there is no maximum entry size.
  auto page_max_entry_size = is_overflow_page ? std::numeric_limits<page_size_t>::max() : max_entry_size_;
  auto maximum_entry_size = std::min(page_max_entry_size, maximum_available_space_for_entry);
  auto entry_size = entry_creator.GetRequestedSize(maximum_entry_size);

  LOG_SEV(Trace) << "Entry creator requested " << entry_size
                 << " bytes of space, maximum available space was " << maximum_available_space_for_entry
                 << ". Will use overflow page: " << entry_creator.GetNeedsOverflow() << ".";
  auto pointer_space = space_requirements.pointer_space;
  auto cell_space = space_requirements.cell_header_space + entry_size;

  auto required_space = pointer_space + cell_space;
  LOG_SEV(Trace) << "Entry will take up " << pointer_space << " bytes of pointer space and " << cell_space
                 << " bytes of cell space, for a total of " << required_space << " bytes.";

  // Sanity check.
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
  LOG_SEV(Trace) << "Starting to write cell at offset " << offset << ".";

  // =======================================
  //  B-tree responsible part.
  // =======================================

  const auto& page = node_map.GetPage();

  // Write the flags.
  offset = writeFlags(*page, header, entry_creator, offset);

  // Write the key.
  offset = writeKey(*page, header, offset, data.key);

  // Ask the EntryCreator to create the entry itself at the given offset.
  LOG_SEV(Trace) << "Creating entry at offset " << offset << " on page " << page->GetPageNumber() << ".";
  offset = entry_creator.Create(offset, page.get(), this);

  // Make sure we wrote the correct amount of data.
  NOSQL_ASSERT(offset == entry_end_offset,
               "incorrect amount of data written to cell in node "
                   << header.GetPageNumber() << ", expected " << cell_space << " bytes, wrote "
                   << (offset - entry_start_offset) << " bytes");

  // The cell has been added, update the page header to indicate that the cell is there.
  header.SetFreeEnd(transaction, static_cast<page_size_t>(header.GetFreeEnd() - cell_space));
  transaction.WriteToPage(*page, header.GetFreeBegin(), header.GetFreeEnd());
  header.SetFreeBegin(transaction, header.GetFreeBegin() + sizeof(page_size_t));

  // Make sure keys are all in ascending order. Only need to do this if the keys are not already sorted
  // (i.e. this was not a rightmost append).
  if (greatest_pk && cmp_(data.key, *greatest_pk)) {
    LOG_SEV(Debug) << "New key is not the largest key, sorting keys in node " << header.GetPageNumber()
                   << ".";
    node_map.sortKeys(transaction);
  }

  return true;
}

void BTreeManager::splitNode(BTreeNodeMap& node,
                             SearchResult& result,
                             std::optional<std::reference_wrapper<StoreData>> data) {
  Transaction transaction {0};  // TODO

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
  auto parent_page_number = result.path.Top()->get().first;
  LOG_SEV(Trace) << "  * Adding right page " << split_data.right_page << " to parent page "
                 << parent_page_number << ".";

  auto parent = loadNodePage(parent_page_number);
  NOSQL_ASSERT(parent, "could not find parent node");

  // Create the data store specification.

  auto entry_creator = internal::MakeSizelessCreator<internal::SpanPayloadSerializer>(
      transaction.GetTransactionID(), internal::SpanValue(split_data.left_page));
  StoreData store_data {.key = split_data.split_key,
                        .entry_creator = &entry_creator,
                        .serialize_key_size = serialize_key_size_,
                        .serialize_data_size = false};

  // Need to make sure there is enough space in the parent node.
  const auto space_requirements = parent->CalculateSpaceRequirements(store_data.key);
  auto maximum_entry_size = std::min(max_entry_size_, space_requirements.max_entry_space);

  if (max_entries_per_page_ <= parent->GetHeader().GetNumPointers()) {
    // If the entry is too large to fit in the parent, we have to split the parent.
    LOG_SEV(Trace) << "  * Parent node " << parent_page_number
                   << " cannot store another entry (has max allowed, " << max_entries_per_page_
                   << "), splitting.";
    splitNode(*parent, result, store_data);
  }
  else if (maximum_entry_size < store_data.entry_creator->GetMinimumEntrySize()) {
    // If the entry is too large to fit in the parent, we have to split the parent.
    LOG_SEV(Trace) << "  * Parent node " << parent_page_number << " is too small to add the new right page.";
    splitNode(*parent, result, store_data);
  }
  else if (!addElementToNode(*parent, store_data)) {
    // If there is not enough space to add the new right page to the parent, we have to split the parent.
    LOG_SEV(Trace) << "  * Parent node " << parent_page_number << " is full, splitting it.";
    splitNode(*parent, result, store_data);
  }

  NOSQL_ASSERT(!parent->IsPointersPage() || parent->GetHeader().GetAdditionalData() != 0,
               "page " << parent_page_number
                       << " is a pointers page with no additional data, there must be a right pointer");
}

SplitPage BTreeManager::splitSingleNode(BTreeNodeMap& node,
                                        std::optional<std::reference_wrapper<StoreData>> data) {
  Transaction transaction {0};

  // Balanced split: create a new page, move half of the elements to the new page.
  // Unbalanced split: move all, or almost all, elements to the new page. Most efficient for adding
  //                   consecutive keys.

  // If the key type is UInt64, we are using auto-incrementing primary keys (this is the assumption for now,
  // at least), so we should not do a balanced split.
  bool do_balanced_split = key_type_ != DataTypeEnum::UInt64;

  auto&& header = node.GetHeader();
  LOG_SEV(Debug) << "Splitting node on page " << node.GetPageNumber() << " with " << node.GetNumPointers()
                 << " pointers.";

  // We can use this function to split leaf or interior nodes.
  auto new_node = newNodePage(node.GetType(), 0);

  SplitPage return_data {.left_page = new_node.GetPageNumber(), .right_page = node.GetPageNumber()};

  // Divide elements between the two nodes.
  page_size_t num_elements = node.GetNumPointers();
  page_size_t num_elements_to_move = do_balanced_split ? num_elements / 2 : num_elements - 1;

  // Interior node.
  auto pointers = node.getPointers();

  // Get the split key.
  if (node.IsPointersPage()) {
    // New rightmost pointer for the left cell is the rightmost pointer. This used to be in a cell, now we
    // move it to be the rightmost pointer. The value that this cell corresponded to will be bubbled up to be
    // the split value in the parent.
    auto pointers_cell =
        std::get<PointersNodeCell>(node.getCell(pointers[static_cast<uint64_t>(num_elements_to_move - 1)]));
    new_node.GetHeader().SetAdditionalData(transaction, pointers_cell.page_number);

    return_data.SetKey(pointers_cell.key);
  }
  else {
    auto data_cell =
        std::get<DataNodeCell>(node.getCell(pointers[static_cast<uint64_t>(num_elements_to_move - 1)]));
    return_data.SetKey(data_cell.key);
  }
  LOG_SEV(Trace) << "Split key will be " << debugKey(return_data.split_key) << ".";

  // Move the low nodes to the new node.
  // That way, we can just add the new node with the split key as a single cell to the parent.
  // We do not have to do anything special about the right page, because if it was the rightmost page, it
  // stays the rightmost page, and otherwise, it's cell is still valid.
  for (auto i = 0; i < num_elements_to_move; ++i) {
    auto cell = node.getCell(pointers[static_cast<uint64_t>(i)]);

    std::visit(
        [&new_node, &transaction, this]<typename Node_t>(const Node_t& cell) {
          using T = std::decay_t<Node_t>;
          StoreData store_data {.key = cell.key, .serialize_key_size = serialize_key_size_};
          if constexpr (std::is_same_v<T, PointersNodeCell>) {
            auto creator = internal::MakeSizelessCreator<internal::SpanPayloadSerializer>(
                transaction.GetTransactionID(), internal::SpanValue(cell.page_number));
            store_data.entry_creator = &creator;
            store_data.serialize_data_size = false;
            addElementToNode(new_node, store_data);
          }
          else if constexpr (std::is_same_v<T, DataNodeCell>) {
            // Note: We only need to copy literally the entry in the page. In particular, it does not matter
            //       if the entry is the header for an overflow page.
            //       Use the entry copier, so we copy the correct flags.
            internal::EntryCopier creator(transaction.GetTransactionID(), cell.flags, cell.SpanValue());
            store_data.entry_creator = &creator;

            store_data.serialize_data_size = true;
            addElementToNode(new_node, store_data);
          }
          else {
            static_assert(lightning::typetraits::always_false_v<T>, "unhandled case");
          }
        },
        cell);
  }

  // =======================================
  //  Compress the pointers in the original node.
  // =======================================

  // Span of the offsets to elements that were not moved to the next page.

  std::span remaining_pointers(pointers.data() + num_elements_to_move,
                               pointers.size() - num_elements_to_move);
  std::vector<page_size_t> pointers_copy(remaining_pointers.size());
  std::ranges::copy(remaining_pointers, pointers_copy.begin());
  // Span for the vector
  remaining_pointers = pointers_copy;
  transaction.WriteToPage(*node.GetPage(), header.GetPointersStart(), remaining_pointers);

  // "Free" the rightmost num_elements_to_move pointers in the original node.
  // TODO: Create a linked list of blocks of newly freed space?
  header.SetFreeBegin(transaction, header.GetFreeBegin() - (num_elements_to_move * sizeof(page_size_t)));

  // =======================================
  // Potentially add data.
  // =======================================

  if (data) {
    auto& data_ref = data->get();
    LOG_SEV(Trace) << "Data requested to be added to a node, pk = " << debugKey(data_ref.key) << ".";
    auto& node_to_add_to = lte(data_ref.key, return_data.split_key) ? new_node : node;
    addElementToNode(node_to_add_to, *data);
  }

  // =======================================
  // Write back node and new node.
  // =======================================

  vacuum(node);

  LOG_SEV(Trace) << "  * After split, original node (on page " << node.GetPageNumber() << ") has "
                 << node.GetDefragmentedFreeSpace() << " bytes of de-fragmented free space.";
  LOG_SEV(Trace) << "  * After split, new node (on page " << new_node.GetPageNumber() << ") has "
                 << new_node.GetDefragmentedFreeSpace() << " bytes of de-fragmented free space.";

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

void BTreeManager::splitRoot(std::optional<std::reference_wrapper<StoreData>> data) {
  Transaction transaction {0};  // TODO

  LOG_SEV(Debug) << "Splitting root node.";

  // If the key type is UInt64, we are using auto-incrementing primary keys (this is the assumption for now,
  // at least), so we should not do a balanced split.
  bool do_balanced_split = key_type_ != DataTypeEnum::UInt64;

  // Create two child pages, spit the nodes between them.
  auto root = loadNodePage(root_page_);
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

  // Balanced or unbalanced split.
  page_size_t num_for_left = do_balanced_split ? root->GetNumPointers() / 2 : root->GetNumPointers() - 1;
  auto split_key = root->getKeyForNthCell(num_for_left);
  LOG_SEV(Trace) << "Split key will be " << debugKey(split_key) << ".";

  for (page_size_t i = 0; i < root->GetNumPointers(); ++i) {
    auto nth_cell = root->getNthCell(i);
    auto& node_to_add_to = i <= num_for_left ? left_child : right_child;
    std::visit(
        [&]<typename Cell_t>(Cell_t&& cell) {
          StoreData store_data {.key = cell.key, .serialize_key_size = serialize_key_size_};

          using T = std::decay_t<Cell_t>;
          if constexpr (std::is_same_v<T, PointersNodeCell>) {
            if (i == num_for_left) {
              // Add this page as the rightmost pointer.
              node_to_add_to.GetHeader().SetAdditionalData(transaction, cell.page_number);
              LOG_SEV(Trace) << "Setting the rightmost pointer in the right child (P"
                             << node_to_add_to.GetPageNumber() << ") to " << cell.page_number << ".";
            }
            else {
              auto creator = internal::MakeSizelessCreator<internal::SpanPayloadSerializer>(
                  transaction.GetTransactionID(), internal::SpanValue(cell.page_number));
              store_data.entry_creator = &creator;
              store_data.serialize_data_size = false;
              NOSQL_ASSERT(addElementToNode(node_to_add_to, store_data),
                           "we should be able to add to this cell");
            }
          }
          else if constexpr (std::is_same_v<T, DataNodeCell>) {
            // Note: We only need to copy literally the entry in the page. In particular, it does not matter
            //       if the entry is the header for an overflow page.
            //       Use the entry copier, so we copy the correct flags.
            auto creator =
                internal::EntryCopier(transaction.GetTransactionID(), cell.flags, cell.SpanValue());
            store_data.entry_creator = &creator;
            store_data.serialize_data_size = true;
            NOSQL_ASSERT(addElementToNode(node_to_add_to, store_data),
                         "we should be able to add to this cell");
          }
          else {
            static_assert(lightning::typetraits::always_false_v<T>, "unhandled case");
          }
        },
        nth_cell);
  }

  // If the root was a pointers page, we need to set the rightmost pointer in the root to the right child.
  if (root_header.IsPointersPage()) {
    right_child.GetHeader().SetAdditionalData(transaction, root_header.GetAdditionalData());
    LOG_SEV(Trace) << "Setting the rightmost pointer in the right child (P" << right_child.GetPageNumber()
                   << ") to " << root_header.GetAdditionalData() << ".";
  }

  // Add data, if any.
  if (data) {
    const auto& data_ref = data->get();
    LOG_SEV(Trace) << "Data requested to be added to a node, pk = " << debugKey(data_ref.key) << ".";
    auto& node_to_add_to = lte(data_ref.key, split_key) ? left_child : right_child;
    // Only store the size of the root was NOT a pointers page (meaning we expect data to be stored, not
    // pointers).
    addElementToNode(node_to_add_to, *data, !root->IsPointersPage());
    LOG_SEV(Debug) << "Added the data to node on page " << node_to_add_to.GetPageNumber() << ".";
  }

  // Clear the entire root page.
  root_header.SetFreeBegin(transaction, root_header.GetPointersStart());
  root_header.SetFreeEnd(transaction, root_header.GetReservedStart());

  // Set the root page to be a pointers page.
  root_header.SetFlags(transaction, root_header.GetFlags() | 0b1);

  // Add the two child pages to the root page, which is a pointers page (so we don't serialize the data size).
  auto entry_creator = internal::MakeSizelessCreator<internal::SpanPayloadSerializer>(
      transaction.GetTransactionID(), internal::SpanValue(left_page_number));

  StoreData store_data {.key = split_key,
                        .entry_creator = &entry_creator,
                        .serialize_key_size = serialize_key_size_,
                        .serialize_data_size = false};
  addElementToNode(*root, store_data);
  // The right page is the "rightmost" pointer.
  root_header.SetAdditionalData(transaction, right_page_number);
  LOG_SEV(Trace) << "Set the rightmost pointer in the root node to " << right_page_number << ".";
}

void BTreeManager::vacuum(BTreeNodeMap& node) const {
  Transaction transaction {0};

  // TODO: This is not actually a hard requirement, but I need to write the fallback.
  NOSQL_REQUIRE(node.GetNumPointers() < 256,
                "vacuuming not implemented for nodes with more than 256 pointers");

  LOG_SEV(Debug) << "Vacuuming node on page " << node.GetPageNumber() << ". Node has "
                 << node.GetDefragmentedFreeSpace() << " bytes of defragmented free space.";

  // TODO: More WAL friendly version: take a snapshot of the pointers, sort them (without going through the
  //       WAL), then submit the sorted changes all at once.

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
    auto cell_size = std::visit([](auto&& cell) { return cell.GetCellSize(); }, cell);

    // Adjust the next point to be at the start of where the cell must be copied.
    next_point -= cell_size;

    LOG_SEV(Trace) << "  * Moving cell " << i << " from offset " << offset << " to offset " << next_point
                   << " (cell size " << cell_size << ").";

    // Copy the cell to the new location.
    transaction.MoveInPage(*page, offset, next_point, cell_size);

    // Update the pointer.
    transaction.WriteToPage(
        *page, header.GetPointersStart() + (pointer_number * sizeof(page_size_t)), next_point);
  }
  // Set the updated free-end location.
  header.SetFreeEnd(transaction, next_point);

  LOG_SEV(Debug) << "Finished vacuuming node on page " << node.GetPageNumber() << ". Node now has "
                 << node.GetDefragmentedFreeSpace() << " bytes of defragmented free space.";
}

SearchResult BTreeManager::search(GeneralKey key) const {
  SearchResult result;

  auto node = [this] {
    auto root = loadNodePage(root_page_);
    NOSQL_ASSERT(root, "could not find root node");
    return std::move(*root);
  }();

  auto current_page_number = node.GetPageNumber();

  // Loop until found. Since this is a (presumably well-formed) B-tree, this should always terminate.
  for (;;) {
    if (!node.IsPointersPage()) {
      if (auto lower_bound = node.getCellLowerBoundByPK(key)) {
        result.path.Emplace(current_page_number, lower_bound->second);
      }
      else {
        result.path.Emplace(current_page_number, node.GetNumPointers());
      }

      // Elements are allocated directly in this page.
      result.node = std::move(node);
      break;
    }

    auto [next_page_number, offset] = node.searchForNextPageInPointersPage(key);

    NOSQL_REQUIRE(next_page_number != node.GetPageNumber(), "infinite loop detected in search");

    result.path.Push({current_page_number, offset});
    current_page_number = next_page_number;

    auto child = loadNodePage(next_page_number);
    node = std::move(*child);
  }

  return result;
}

RetrievalResult BTreeManager::retrieve(GeneralKey key) const {
  RetrievalResult result;
  result.search_result = search(key);
  if (result.search_result.IsFound()) {
    // Get cell index.
    const auto cell_index = result.search_result.path.Top()->get().second;
    const auto cell_offset = result.search_result.node->getCellOffsetByIndex(cell_index);

    // Have to pass in a new page handle to read entry.
    result.entry = internal::ReadEntry(cell_offset, result.search_result.node->GetPage()->NewHandle(), this);
  }
  return result;
}

bool BTreeManager::lte(GeneralKey key1, GeneralKey key2) const {
  if (cmp_(key1, key2)) {
    return true;
  }
  return std::ranges::equal(key1, key2);
}

std::string BTreeManager::debugKey(GeneralKey key) const {
  if (debug_key_func_) {
    return debug_key_func_(key);
  }
  // Hex dump the key.
  return internal::HexDumpBytes(key);
}

bool BTreeManager::isUniqueKey(BTreeNodeMap& node_map, const StoreData& data) const noexcept {
  if (auto lower_bound = node_map.getCellLowerBoundByPK(data.key)) {
    // If the key is already in the node, we cannot add it again.
    auto cell = node_map.getCell(lower_bound->first);
    if (std::visit([&data](const auto& c) { return std::ranges::equal(c.key, data.key); }, cell)) {
      LOG_SEV(Trace) << "Key " << debugKey(data.key) << " already in node on page "
                     << node_map.GetPageNumber() << ".";
      return false;
    }
  }
  return true;
}

page_size_t BTreeManager::writeFlags(Page& page,
                                     BTreePageHeader& header,
                                     internal::EntryCreator& entry_creator,
                                     page_size_t offset) noexcept {
  Transaction transaction {0};  // TODO

  auto flags = entry_creator.GenerateFlags();
  // Set flags the the B-tree is responsible for.
  flags |= static_cast<std::byte>(internal::EntryFlags::IsActive);
  if (header.AreKeySizesSpecified()) {
    flags |= static_cast<std::byte>(internal::EntryFlags::KeySizeIsSerialized);
  }
  return transaction.WriteToPage(page, offset, flags);
}

page_size_t BTreeManager::writeKey(Page& page,
                                   BTreePageHeader& header,
                                   page_size_t offset,
                                   GeneralKey key) noexcept {
  Transaction transaction {0};  // TODO

  // If the page is set up to use fixed primary_key_t length keys, just write the key.
  // Otherwise, we have to write the key size, then the key.
  if (header.AreKeySizesSpecified()) {
    // Write the size of the key, key size is stored as a uint16_t.
    auto key_size = static_cast<uint16_t>(key.size());
    offset = transaction.WriteToPage(page, offset, key_size);
  }
  return transaction.WriteToPage(page, offset, key);
}

}  // namespace neversql