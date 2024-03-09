//
// Created by Nathaniel Rupprecht on 2/27/24.
//

#include <iostream>
#include <string>

#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/database/DataManager.h"
#include "NeverSQL/utility/HexDump.h"
#include "NeverSQL/utility/PageDump.h"

using neversql::primary_key_t;
using namespace lightning;

void SetupLogger(Severity min_severity = Severity::Info);

int main() {
  SetupLogger(Severity::Info);

  // ---> Your database path here.
  std::filesystem::path database_path =
      "/Users/nathaniel/Documents/Nathaniel/Programs/C++/NeverSQL/database-dmgr-test.db";

  // std::filesystem::remove(database_path);

  primary_key_t num_to_insert = 100'000'000;

  neversql::DataManager manager(database_path);

  LOG_SEV(Info) << lightning::formatting::Format("Database has {:L} pages.",  manager.GetDataAccessLayer().GetNumPages());

  primary_key_t pk = 0;
  auto starting_time_point = std::chrono::high_resolution_clock::now();
  auto time_point = starting_time_point;
  std::size_t batch_count = 0;
  const std::size_t batch_size = 100'000;
  try {
    for (; pk < num_to_insert; ++pk) {
      std::string str = formatting::Format("Brave new world, page {}.", pk);
      manager.AddValue(
          std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
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
    auto average_time = static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::high_resolution_clock::now() - starting_time_point)
                            .count())
        / static_cast<double>(batch_count);
    LOG_SEV(Major) << formatting::Format(
        "Finished inserting {:L} values in {:L} batches, average time was {} ms per {:L} values ({} per "
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
  manager.NodeDumpPage(2, std::cout);
  std::cout << std::endl;

  // Search for some elements.
  auto first_to_probe = num_to_insert / 2;
  auto last_to_probe = std::min(num_to_insert / 2 + 10, num_to_insert);
  for (primary_key_t pk_probe = first_to_probe; pk_probe < last_to_probe; ++pk_probe) {
    auto result = manager.Retrieve(pk_probe);
    if (result.IsFound()) {
      auto& view = result.value_view;
      LOG_SEV(Info) << formatting::Format(
          "Found key {:L} on page {}, value: \"{@BYELLOW}{}{@RESET}\".",
          pk_probe,
          result.search_result.node->GetPageNumber(),
          std::string_view(reinterpret_cast<const char*>(view.data()), view.size()));
    }
    else {
      LOG_SEV(Info) << formatting::Format("{@BRED}Key {} was not found.{@RESET}", pk_probe);
    }
  }

  return 0;
}

void SetupLogger(Severity min_severity) {
  auto console = lightning::NewSink<lightning::StdoutSink>();
  lightning::Global::GetCore()->AddSink(console);
  console->SetFilter(min_severity <= LoggingSeverity);

  // Formatter for "low levels" of severity displace file and line number.
  auto verbose_formatter = MakeMsgFormatter("[{}] [{}:{}] [{}] {}",
                                            formatting::DateTimeAttributeFormatter {},
                                            formatting::FileNameAttributeFormatter {true},
                                            formatting::FileLineAttributeFormatter {},
                                            formatting::SeverityAttributeFormatter {false},
                                            formatting::MSG);

  //  formatting::FormatterBySeverity formatter;
  //  {
  //    auto default_fmt = MakeMsgFormatter("[{}] [{}] {}",
  //                                        formatting::DateTimeAttributeFormatter {},
  //                                        formatting::SeverityAttributeFormatter {false},
  //                                        formatting::MSG);
  //    // Formatter for "low levels" of severity displace file and line number.
  //    auto verbose_formatter = MakeMsgFormatter("[{}] [{}:{}] [{}] {}",
  //                                              formatting::DateTimeAttributeFormatter {},
  //                                              formatting::FileNameAttributeFormatter {true},
  //                                              formatting::FileLineAttributeFormatter {},
  //                                              formatting::SeverityAttributeFormatter {false},
  //                                              formatting::MSG);
  //
  //    formatter
  //        // Set formatter for Trace and Debug severities.
  //        .SetFormatterForSeverity(LoggingSeverity <= Severity::Debug || LoggingSeverity > Severity::Major,
  //                                 *verbose_formatter)
  //        // Set formatter for all other severities (including records without severities).
  //        .SetDefaultFormatter(std::move(default_fmt));
  //  }
  Global::GetCore()->SetAllFormatters(verbose_formatter);
}