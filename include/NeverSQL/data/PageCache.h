//
// Created by Nathaniel Rupprecht on 3/2/24.
//

#pragma once

#include <unordered_map>
#include <vector>

#include "NeverSQL/data/DataAccessLayer.h"
#include "NeverSQL/data/FreeList.h"
#include "NeverSQL/recovery/WriteAheadLog.h"

namespace neversql {

//! \brief Class that keeps a cache of pages in memory. This is useful for reducing the number of reads and
//!        writes to the disk, pages that are frequently used can be kept in memory.
class PageCache {
public:
  //! \brief Construct a new page cache with a prescribed cache size operating over a particular data access
  //!        layer.
  PageCache(const std::filesystem::path& wal_directory,
            std::size_t cache_size,
            DataAccessLayer* data_access_layer);

  //! \brief Write back all uncommitted pages to the disk.
  ~PageCache();

  //! \brief Request a page from the page cache.
  std::unique_ptr<Page> GetPage(page_number_t page_number);

  //! \brief Get a new page from the page cache.
  std::unique_ptr<Page> GetNewPage();

  //! \brief Release a page back to the page cache.
  void ReleasePage(page_number_t page_number);

  //! \brief Indicates that data has been written to the page in a particular slot.
  void SetDirty(std::size_t slot);

  //! \brief Get the write ahead log.
  WriteAheadLog& GetWAL() { return wal_; }

private:
  //! \brief A description of the page that is in a particular slot in the cache.
  struct PageDescriptor {
    // TODO: Generally, the flags in a page cache are much more complex and keep track of more. Start simple,
    //  improve as necessary.

    page_number_t page_number = 5675675675675675675;
    uint8_t usage_count {};
    //! \brief Descriptor flags.
    //! 0b 0000 0CDU
    //! D: Dirty bit, 1 if the page has been modified.
    //! V: Valid bit, 1 if an actual page is stored here.
    //! C: Second chance bit, set to 1 whenever the page is referenced, set to 0 whenever the clock hand
    //! passes by it.
    uint8_t flags {};

    NO_DISCARD bool IsValid() const noexcept { return (flags & 0x1) != 0; }
    NO_DISCARD bool IsDirty() const noexcept { return (flags & 0x2) != 0; }
    NO_DISCARD bool HasSecondChance() const noexcept { return (flags & 0x4) != 0; }

    void SetIsDirty(bool is_dirty) noexcept {
      if (is_dirty) {
        flags |= 0x2;
      }
      else {
        flags &= ~0x2;
      }
    }

    void SetValid(bool is_valid) noexcept {
      if (is_valid) {
        flags |= 0x1;
      }
      else {
        flags &= ~0x1;
      }
    }

    void SetSecondChance(bool second_chance) noexcept {
      if (second_chance) {
        flags |= 0x4;
      }
      else {
        flags &= ~0x4;
      }
    }

    //! \brief Release the descriptor so it can be used again.
    void ReleaseDescriptor() noexcept {
      // "Magic number" to indicate that the slot is empty.
      page_number = 5675675675675675675;
      usage_count = 0;
      flags = 0;
    }
  };

  //! \brief Flush a page to the disk.
  void flushPage(const Page& page);

  //! \brief Get a free slot in the cache in which to store a page.
  std::size_t getSlot();

  //! \brief Map the data from a slot into a page. The slot does not have to be valid.
  std::unique_ptr<Page> mapPageFromSlot(std::size_t slot);

  //! \brief Get a page object tied to a particular slot in the cache. The slot must be valid.
  std::unique_ptr<Page> getPageFromSlot(std::size_t slot);

  //! \brief Set up the descriptor for a page that has been newly entered into a slot.
  //!
  //! \param page_slot The slot in the cache where the page is being stored.
  //! \param page_number The page number of the page being stored.
  void initializePage(std::size_t page_slot, page_number_t page_number);

  //! \brief Decrement the usage count of a page in a particular slot.
  void decrementUsage(std::size_t slot);

  //! \brief Try to release a page from a particular slot.
  bool tryReleasePage(std::size_t slot);

  //! \brief Choose the next "victim" to evict from the cache and release it from the page cache.
  std::size_t evictNextVictim();

  // =================================================================================================
  //  Private members.
  // =================================================================================================

  //! \brief The write ahead logging manager.
  WriteAheadLog wal_;

  //! \brief Map from page numbers of pages in the cache to their slot in the cache.
  std::unordered_map<page_number_t, std::size_t> page_number_to_slot_;

  //! \brief Vector of page descriptors, which are used to keep track of the pages in the cache.
  std::vector<PageDescriptor> page_descriptors_;

  //! \brief Allocate a large chunk of space for storing pages.
  std::unique_ptr<std::byte[]> page_cache_;

  //! \brief The data access layer, which is used to read and write pages to the disk.
  //! NOTE(Nate): Probably, the PageCache should just own the DAL, if you want to get the data you *have* to
  //! go through
  //!     the page cache.
  DataAccessLayer* data_access_layer_;

  //! \brief The number of pages that can fit in the cache.
  std::size_t cache_size_;

  //! \brief Free list for the cache.
  FreeList cache_free_list_;

  // TEMPORARY.
  std::size_t next_victim_ = 0;
};

}  // namespace neversql
