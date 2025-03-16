//
// Created by Nathaniel Rupprecht on 3/3/24.
//

#include "neversql/data/PageCache.h"
// Other files.

namespace neversql {

PageCache::PageCache(const std::filesystem::path& wal_directory,
                     std::size_t cache_size,
                     DataAccessLayer* data_access_layer)
    : wal_(wal_directory)
    , page_descriptors_(cache_size)
    , data_access_layer_(data_access_layer)
    , cache_size_(cache_size)
    , cache_free_list_(cache_size, false) {
  page_cache_ = std::make_unique<std::byte[]>(cache_size * data_access_layer_->GetPageSize());
}

PageCache::~PageCache() {
  for (std::size_t i = 0; i < cache_size_; ++i) {
    try {
      tryReleasePage(i);
    } catch (const std::exception& ex) {
      LOG_SEV(Error) << "Error releasing page " << i
                     << " when destructing PageCache:" << lightning::NewLineIndent << ex.what();
    }
  }
}

std::unique_ptr<Page> PageCache::GetPage(page_number_t page_number) {
  // Check if the page is in the cache.
  if (auto it = page_number_to_slot_.find(page_number); it != page_number_to_slot_.end()) {
    // If the page is in the cache, we can just return it.
    LOG_SEV(Trace) << "Page " << page_number << " was in the cache, returning.";
    auto page = getPageFromSlot(it->second);
    NOSQL_ASSERT(page->GetPageNumber() == page_number,
                 "requested page number " << page_number
                                          << " does not match the page number loaded from slot " << it->second
                                          << ", which was page number " << page->GetPageNumber());
    return page;
  }
  LOG_SEV(Trace) << "Page " << page_number << " was not in the cache, loading into the cache.";

  // If the page is not in the cache, get a slot into which to load the page.
  std::size_t slot = getSlot();
  initializePage(slot, page_number);
  auto page_slot = getPageFromSlot(slot);
  data_access_layer_->GetPage(page_number, *page_slot);

  return page_slot;
}

std::unique_ptr<Page> PageCache::GetNewPage() {
  auto slot = getSlot();

  auto page = mapPageFromSlot(slot);
  data_access_layer_->GetNewPage(*page);

  // Set up descriptor.
  initializePage(slot, page->GetPageNumber());

  return page;
}

void PageCache::ReleasePage(page_number_t page_number) {
  // Find the page in the cache.
  if (auto it = page_number_to_slot_.find(page_number); it != page_number_to_slot_.end()) {
    decrementUsage(it->second);
  }
  else {
    NOSQL_FAIL("page number " << page_number << " could not found in cache to release");
  }
}

void PageCache::SetDirty(std::size_t slot) {
  page_descriptors_[slot].SetIsDirty(true);
}

void PageCache::flushPage(const Page& page) {
  LOG_SEV(Debug) << "Flushing page " << page.GetPageNumber() << ".";
  // TODO: Write to log?
  data_access_layer_->WriteBackPage(page);
}

std::size_t PageCache::getSlot() {
  LOG_SEV(Debug) << "Acquiring cache slot.";

  // If there are free slots, use these.
  if (auto next_slot = cache_free_list_.GetNextPage()) {
    LOG_SEV(Trace) << "Next free slot is " << next_slot.value() << ".";
    return next_slot.value();
  }
  LOG_SEV(Trace) << "No slots are free, a victim must be evicted from the cache.";

  // Otherwise, we must, evict a page.
  auto newly_freed_slot = evictNextVictim();
  auto slot = cache_free_list_.GetNextPage();
  // We expect these slots to be the same. Sanity check.
  NOSQL_ASSERT(slot, "no free slots after eviction");
  NOSQL_ASSERT(newly_freed_slot == slot,
               "evicted slot (" << newly_freed_slot << ") does not match the slot from the free list ("
                                << *slot << ")");
  return newly_freed_slot;
}

std::unique_ptr<Page> PageCache::mapPageFromSlot(std::size_t slot) {
  NOSQL_REQUIRE(slot < cache_size_,
                "slot is out of range, tried to map slot " << slot << " from cache of size " << cache_size_);

  auto* page_start_ptr = page_cache_.get() + slot * data_access_layer_->GetPageSize();
  auto page = std::make_unique<RCPage>(data_access_layer_->GetPageSize(), slot, this);
  page->SetData(page_start_ptr);

  return page;
}

std::unique_ptr<Page> PageCache::getPageFromSlot(std::size_t slot) {
  NOSQL_REQUIRE(slot < cache_size_, "slot is out of range");
  auto& descriptor = page_descriptors_[slot];
  NOSQL_REQUIRE(descriptor.IsValid(), "slot is not valid");

  // Map the page contained in the slot to a Page object.
  auto page = mapPageFromSlot(slot);
  page->SetPageNumber(descriptor.page_number);

  // Increment usage.
  ++descriptor.usage_count;
  descriptor.SetSecondChance(true);

  LOG_SEV(Trace) << "Returning page " << descriptor.page_number << " from slot " << slot
                 << ", usage count is " << descriptor.usage_count << ".";

  return page;
}

void PageCache::initializePage(std::size_t page_slot, page_number_t page_number) {
  LOG_SEV(Debug) << "Initializing page " << page_number << " in slot " << page_slot << ".";

  // Set slot index.
  page_number_to_slot_[page_number] = page_slot;
  NOSQL_ASSERT(page_number_to_slot_.size() <= cache_size_,
               "page number to slot map is larger than cache size, cache size is "
                   << cache_size_ << ", map size is " << page_number_to_slot_.size());

  // Set up the page descriptor.
  auto& descriptor = page_descriptors_[page_slot];
  descriptor.SetValid(true);
  descriptor.page_number = page_number;
  descriptor.usage_count = 0;
}

void PageCache::decrementUsage(std::size_t slot) {
  NOSQL_REQUIRE(slot < cache_size_, "slot is out of range");

  auto& descriptor = page_descriptors_[slot];
  if (descriptor.IsValid() && 0 < descriptor.usage_count) {
    --descriptor.usage_count;
  }
}

bool PageCache::tryReleasePage(std::size_t slot) {
  LOG_SEV(Debug) << "Trying to release the page in slot " << slot << ".";
  NOSQL_REQUIRE(slot < cache_size_, "slot is out of range");

  // NOTE: Right now, I'm always writing back the page to the disk immediately (if it is dirty), but this may
  // not be necessary.

  // Make sure the slot is not empty or pinned.

  auto& descriptor = page_descriptors_[slot];
  if (0 < descriptor.usage_count) {
    LOG_SEV(Warning) << "Page in slot " << slot << " (page " << descriptor.page_number
                     << ") is referenced, useage count = " << descriptor.usage_count << ", cannot release.";
    // If the page is pinned, we can't release it.
    return false;
  }

  if (!descriptor.IsValid()) {
    LOG_SEV(Debug) << "No page to release in slot " << slot << ".";
    // No errors, just no page to release.
    return true;
  }

  auto page_number = descriptor.page_number;
  LOG_SEV(Trace) << "Releasing page " << page_number << " from slot " << slot
                 << ". Page is valid and has no usages.";

  // Write the page back to the disk.
  {
    // We just map the page so we don't introduce any use counts - just in case.
    // NOTE that the page number is not actually set in the page, but we don't need it to be.
    auto page = mapPageFromSlot(slot);
    // Set the page number, so we can write it back.
    page->SetPageNumber(page_number);

    if (descriptor.IsDirty()) {
      flushPage(*page);
    }
  }
  // Reset flags.
  descriptor.ReleaseDescriptor();

  // Remove the entry from the page number to slot map.
  page_number_to_slot_.erase(page_number);
  // Add the slot to the free list.
  NOSQL_ASSERT(cache_free_list_.ReleasePage(slot), "tried to release a page that was already free");

  // Sanity check.
  NOSQL_ASSERT(cache_free_list_.GetNumFreePages() == cache_size_ - page_number_to_slot_.size(),
               "free list is incorrect, there should be "
                   << cache_size_ - page_number_to_slot_.size() << " free pages, but there are "
                   << cache_free_list_.GetNumFreePages() << " free pages");

  // Page was successfully released.
  return true;
}

std::size_t PageCache::evictNextVictim() {
  LOG_SEV(Debug) << "Finding victim to evict.";

  // Clock page replacement.
  std::size_t count = 0;
  while (page_descriptors_[next_victim_].HasSecondChance()) {
    // Reset the second chance bit.
    page_descriptors_[next_victim_].SetSecondChance(false);

    next_victim_ = (next_victim_ + 1) % cache_size_;
    ++count;
    if (count == cache_size_) {
      LOG_SEV(Trace) << "All pages had second chances, made it back to the start, " << next_victim_ << ".";
      break;
    }
  }

  LOG_SEV(Trace) << "Victim chosen, slot " << next_victim_ << ".";
  NOSQL_ASSERT(tryReleasePage(next_victim_), "failed to release page from cache");

  auto next_victim = next_victim_;
  // Next time, start on the next entry.
  next_victim_ = (next_victim_ + 1) % cache_size_;
  return next_victim;
}

}  // namespace neversql
