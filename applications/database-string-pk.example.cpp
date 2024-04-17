//
// Created by Nathaniel Rupprecht on 3/14/24.
//

#include <iostream>
#include <string>

#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/data/internals/Utility.h"
#include "NeverSQL/database/DataManager.h"
#include "NeverSQL/database/Query.h"
#include "NeverSQL/utility/HexDump.h"
#include "NeverSQL/utility/PageDump.h"

using neversql::primary_key_t;
using namespace lightning;

void SetupLogger(Severity min_severity = Severity::Info);

int main() {
  SetupLogger(Severity::Info);

  // ---> Your database path here.
  std::filesystem::path database_path =
      "/Users/nathaniel/Documents/Nathaniel/Programs/C++/NeverSQL/database-string";

  std::filesystem::remove_all(database_path);

  neversql::DataManager manager(database_path);

  LOG_SEV(Info) << lightning::formatting::Format("Database has {:L} pages.",
                                                 manager.GetDataAccessLayer().GetNumPages());

  manager.AddCollection("elements", neversql::DataTypeEnum::String);

  // Create a document.
  {
    neversql::Document builder;
    builder.AddElement("name", neversql::StringValue{"George"});
    builder.AddElement("age", neversql::IntegralValue{24});
    builder.AddElement("favorite_color", neversql::StringValue{"blue"});
    // Add the document.
    std::string key = "George";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }
  {
    neversql::Document builder;
    builder.AddElement("name", neversql::StringValue{"Helen"});
    builder.AddElement("age", neversql::IntegralValue{25});

    // Sub-document.
    {
      auto sub_builder = std::make_unique<neversql::Document>();
      sub_builder->AddElement("favorite_color", neversql::StringValue{"green"});

      auto array = std::make_unique<neversql::ArrayValue>(neversql::DataTypeEnum::Int32);
      array->AddElement(neversql::IntegralValue{33});
      array->AddElement(neversql::IntegralValue{42});
      array->AddElement(neversql::IntegralValue{109});
      sub_builder->AddElement("favorite_numbers", std::move(array));
      builder.AddElement("favorites", std::move(sub_builder));
    }

    // Add the document.
    std::string key = "Helen";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }
  {
    neversql::Document builder;
    builder.AddElement("name", neversql::StringValue{"Carson"});
    builder.AddElement("age", neversql::IntegralValue{44});
    // Add the document.
    std::string key = "Carson";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }
  {
    neversql::Document builder;
    builder.AddElement("name", neversql::StringValue{"Julia"});
    builder.AddElement("age", neversql::IntegralValue{18});
    // Add the document.
    std::string key = "Julia";
    manager.AddValue("elements", neversql::internal::SpanValue(key), builder);
  }


  // Execute a query.
  auto iterator = neversql::query::BTreeQueryIterator(manager.Begin("elements"),
                                                      neversql::query::LessEqual<int>("age", 40));
  for (; !iterator.IsEnd(); ++iterator) {
    auto entry = *iterator;
    // Interpret the data as a document.
    auto document = EntryToDocument(*entry);
    LOG_SEV(Info) << "Found: " << neversql::PrettyPrint(*document);
  }

  auto total_pages = manager.GetDataAccessLayer().GetNumPages();
  LOG_SEV(Major) << lightning::formatting::Format("Database has {:L} pages.", total_pages);

  // Node dump the main index page.
  manager.NodeDumpPage(2, std::cout);
  std::cout << std::endl;
  manager.NodeDumpPage(3, std::cout);
  std::cout << std::endl;

  // Search for some elements.

  std::vector<std::string> names_to_check {"Helen"};

  for (auto& name : names_to_check) {
    auto result = manager.Retrieve("elements", neversql::internal::SpanValue(name));
    if (result.IsFound()) {
      // Interpret the data as a document.
      auto document = EntryToDocument(*result.entry);

      LOG_SEV(Info) << formatting::Format(
          "Found key {:?} on page {:L}, search depth {}, value: \n{@BYELLOW}{}{@RESET}",
          name,
          result.search_result.node->GetPageNumber(),
          result.search_result.GetSearchDepth(),
          neversql::PrettyPrint(*document));
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