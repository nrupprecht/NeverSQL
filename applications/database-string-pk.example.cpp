//
// Created by Nathaniel Rupprecht on 3/14/24.
//

#include <iostream>
#include <string>

#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/database/DataManager.h"
#include "NeverSQL/utility/HexDump.h"
#include "NeverSQL/utility/PageDump.h"
#include "NeverSQL/data/internals/Utility.h"

using neversql::primary_key_t;
using namespace lightning;

void SetupLogger(Severity min_severity = Severity::Info);

int main() {
  SetupLogger(Severity::Trace);

  // ---> Your database path here.
  std::filesystem::path database_path = "your-path-here";

  std::filesystem::remove_all(database_path);

  neversql::DataManager manager(database_path);

  LOG_SEV(Info) << lightning::formatting::Format("Database has {:L} pages.",
                                                 manager.GetDataAccessLayer().GetNumPages());

  manager.AddCollection("elements", neversql::DataTypeEnum::String);

  // Create a document.
  {
    neversql::DocumentBuilder builder;
    builder.AddEntry("name", "George");
    builder.AddEntry("age", 24);
    builder.AddEntry("favorite_color", "blue");
    // Add the document.
    std::string key = "George";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }
  {
    neversql::DocumentBuilder builder;
    builder.AddEntry("name", "Helen");
    builder.AddEntry("age", 25);
    // Add the document.
    std::string key = "Helen";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }
  {
    neversql::DocumentBuilder builder;
    builder.AddEntry("name", "Carson");
    builder.AddEntry("age", 44);
    // Add the document.
    std::string key = "Carson";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }

  auto total_pages = manager.GetDataAccessLayer().GetNumPages();
  LOG_SEV(Major) << lightning::formatting::Format("Database has {:L} pages.", total_pages);

  // Node dump the main index page.
  manager.NodeDumpPage(2, std::cout);
  std::cout << std::endl;
  manager.NodeDumpPage(3, std::cout);
  std::cout << std::endl;

  // Search for some elements.

  std::vector<std::string> names_to_check{"Helen"};

  for (auto& name : names_to_check) {
    auto result = manager.Retrieve("elements", neversql::internal::SpanValue(name));
    if (result.IsFound()) {
      auto& view = result.value_view;

      // Interpret the data as a document.
      neversql::DocumentReader reader(view);

      LOG_SEV(Info) << formatting::Format(
          "Found key {:?} on page {:L}, search depth {}, value: \n{@BYELLOW}{}{@RESET}",
          name,
          result.search_result.node->GetPageNumber(),
          result.search_result.GetSearchDepth(),
          neversql::PrettyPrint(reader));
    }
    else {
      LOG_SEV(Info) << formatting::Format("{@BRED}Key {:?} was not found.{@RESET}", name);
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
  Global::GetCore()->SetAllFormatters(verbose_formatter);
}