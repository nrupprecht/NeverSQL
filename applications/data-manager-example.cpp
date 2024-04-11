//
// Created by Nathaniel Rupprecht on 2/27/24.
//

#include <iostream>
#include <string>

#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/database/DataManager.h"
#include "NeverSQL/utility/HexDump.h"
#include "NeverSQL/utility/PageDump.h"

using namespace lightning;
using namespace neversql;

void SetupLogger(Severity min_severity = Severity::Info);

int main() {
  // 0.004311 ms per addition, 233,612 pages, 10,000,000 entries.
  SetupLogger(Severity::Info);

  // ---> Your database path here.
  std::filesystem::path database_path =
      "/Users/nathaniel/Documents/Nathaniel/Programs/C++/NeverSQL/database-dmgr-test";

  remove_all(database_path);

  primary_key_t num_to_insert = 10'000'000;  // 48

  DataManager manager(database_path);

  LOG_SEV(Info) << formatting::Format("Database has {:L} pages.", manager.GetDataAccessLayer().GetNumPages());

  manager.AddCollection("elements", DataTypeEnum::UInt64);

  primary_key_t pk = 0;
  auto starting_time_point = std::chrono::high_resolution_clock::now();
  auto time_point = starting_time_point;
  std::size_t batch_count = 0;
  const std::size_t batch_size = 100'000;
  try {
    for (; pk < num_to_insert; ++pk) {
      // Create a document.
      Document builder;
      builder.AddElement("data", StringValue {formatting::Format("Brave new world.\nEntry number {}.", pk)});
      builder.AddElement("pk", IntegralValue {static_cast<int32_t>(pk)});
      builder.AddElement("is_even", BooleanValue {pk % 2 == 0});
      // Add the document.
      manager.AddValue("elements", builder);

      if ((pk + 1) % batch_size == 0) {
        auto next_time_point = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(next_time_point - time_point);
        LOG_SEV(Info) << formatting::Format("Inserted {:>10L} values in {:L} ms", pk + 1, duration.count());
        time_point = next_time_point;
        ++batch_count;
      }
    }
  } catch (const std::exception& ex) {
    LOG_SEV(Error) << "Caught exception when trying to add entry with pk " << pk << ":" << ex;
  }
  if (0 < batch_count) {
    auto average_time =
        static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::high_resolution_clock::now() - starting_time_point)
                                .count())
        / static_cast<double>(batch_count);
    LOG_SEV(Major) << formatting::Format(
        "Finished inserting {:L} values in {:L} batches, average time was {} ms per {:L} values ({} ms per "
        "addition).",
        pk,
        batch_count,
        average_time,
        batch_size,
        average_time / batch_size);
  }

  auto total_pages = manager.GetDataAccessLayer().GetNumPages();
  LOG_SEV(Major) << lightning::formatting::Format("Database has {:L} pages.", total_pages);

  // Node dump the main index page.
  manager.NodeDumpPage(3, std::cout);
  std::cout << std::endl;
  try {
    manager.NodeDumpPage(4, std::cout);
    std::cout << std::endl;
  } catch (...) {
  }

  // Search for some elements.
  auto first_to_probe = num_to_insert / 2;
  auto last_to_probe = std::min(num_to_insert / 2 + 10, num_to_insert);
  for (primary_key_t pk_probe = first_to_probe; pk_probe < last_to_probe; ++pk_probe) {
    auto result = manager.Retrieve("elements", pk_probe);
    if (result.IsFound()) {
      memory::MemoryBuffer<std::byte> buffer;

      auto& entry = *result.entry;
      do {
        auto data = entry.GetData();
        buffer.Append(data);
      } while (entry.Advance());

      auto view = std::span {buffer.Data(), buffer.Size()};

      // Interpret the data as a document.
      if (auto document = neversql::ReadDocumentFromBuffer(view)) {
        LOG_SEV(Info) << formatting::Format(
            "Found key {:L} on page {:L}, search depth {}, value: \n{@BYELLOW}{}{@RESET}",
            pk_probe,
            result.search_result.node->GetPageNumber(),
            result.search_result.GetSearchDepth(),
            neversql::PrettyPrint(*document));
      }
      else {
        LOG_SEV(Error) << "Could not read document.";
      }
    }
    else {
      LOG_SEV(Info) << formatting::Format("{@BRED}Key {} was not found.{@RESET}", pk_probe);
    }
  }

  auto it_begin = manager.Begin("elements");
  auto end_it = manager.End("elements");
  for (auto it = it_begin; it != end_it; ++it) {
    auto entry = *it;
    // Interpret the data as a document.
    auto document = EntryToDocument(*entry);
    LOG_SEV(Info) << formatting::Format("{@BYELLOW}{}{@RESET}", neversql::PrettyPrint(*document));
  }

  return 0;
}

void SetupLogger(Severity min_severity) {
  auto console = lightning::NewSink<lightning::StdoutSink>();
  Global::GetCore()->AddSink(console);
  console->SetFilter(min_severity <= LoggingSeverity);

  // Formatter for "low levels" of severity displace file and line number.
  auto verbose_formatter = MakeMsgFormatter("[{}] [{}:{}] [{}] {}",
                                            formatting::DateTimeAttributeFormatter {},
                                            formatting::FileNameAttributeFormatter {true},
                                            formatting::FileLineAttributeFormatter {},
                                            formatting::SeverityAttributeFormatter {false},
                                            formatting::MSG);
  Global::GetCore()->SetAllFormatters(verbose_formatter);
}