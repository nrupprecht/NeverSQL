//
// Created by Nathaniel Rupprecht on 4/8/24.
//

#include <iostream>
#include <string>

#include "neversql/data/btree/BTree.h"
#include "neversql/data/internals/Utility.h"
#include "neversql/database/DataManager.h"
#include "neversql/database/Query.h"
#include "neversql/utility/HexDump.h"
#include "neversql/utility/PageDump.h"

using neversql::primary_key_t;
using namespace lightning;

void SetupLogger(Severity min_severity = Severity::Info);

int main() {
  std::filesystem::path path =
      "/Users/nrupprecht/Library/Mobile "
      "Documents/com~apple~CloudDocs/Documents/Nathaniel/Programs/C++/NeverSQL/";

  SetupLogger(Severity::Info);

  // ---> Your database path here.
  std::filesystem::path database_path = path / "dbs/shakespeare-database";

  remove_all(database_path);

  neversql::DataManager manager(database_path);

  LOG_SEV(Info) << formatting::Format("Database has {:L} pages.", manager.GetDataAccessLayer().GetNumPages());

  manager.AddCollection("elements", neversql::DataTypeEnum::UInt64);

  std::ifstream file(path / "shakespeare.txt");
  if (!file.is_open()) {
    LOG_SEV(Error) << "Could not open file.";
    return 1;
  }

  // Find quotes, and who quoted them. There are double newlines that separate quotes, then the speaker's
  // name, followed by a ':' and a newline, then the quote (which ends with double newlines).
  std::string line;
  std::string quote;
  std::string speaker;
  uint64_t count = 0;
  while (std::getline(file, line)) {
    if (line.empty()) {
      if (!quote.empty()) {
        ++count;
        speaker.pop_back();  // Get rid of the trailing ':'

        neversql::Document builder;
        builder.AddElement("number", neversql::IntegralValue {count});
        builder.AddElement("speaker", neversql::StringValue {speaker});
        builder.AddElement("quote", neversql::StringValue {quote});

        manager.AddValue("elements", builder);
        LOG_SEV(Info) << formatting::Format(
            "Added entry {} to the database, speaker {@BBLUE}{:?}{@RESET}, quote {@BGREEN}{:?}{@RESET}.",
            count,
            speaker,
            quote);

        quote.clear();
        speaker.clear();
      }
    }
    else if (speaker.empty()) {
      speaker = line;
    }
    else {
      if (!quote.empty()) {
        quote += " ";
      }
      quote += line;
    }
  }

  LOG_SEV(Major) << formatting::Format("Database has {:L} pages.",
                                       manager.GetDataAccessLayer().GetNumPages());

  auto it_begin = manager.Begin("elements");
  auto end_it = manager.End("elements");
  for (auto it = it_begin; it != end_it; ++it) {
    auto entry = *it;
    // Interpret the data as a document.

    if (!entry->IsValid()) {
      continue;
    }

    auto document = EntryToDocument(*entry);
    LOG_SEV(Info) << formatting::Format("{@BYELLOW}{}{@RESET}", neversql::PrettyPrint(*document));
  }

  // neversql::query::BTreeQueryIterator query_iterator(manager.Begin("elements"),
  //                                                    neversql::query::LessEqual<uint64_t>("number", 10));
  // for (; !query_iterator.IsEnd(); ++query_iterator) {
  //   auto entry = *query_iterator;
  //   // Interpret the data as a document.
  //   if (!entry->IsValid()) {
  //     continue;
  //   }
  //
  //   auto document = EntryToDocument(*entry);
  //   LOG_SEV(Info) << formatting::Format("{@BYELLOW}{}{@RESET}", neversql::PrettyPrint(*document));
  // }

  neversql::query::BTreeQueryIterator query_iterator(
      manager.Begin("elements"), neversql::query::Equal<std::string>("speaker", "First Citizen"));

  neversql::query::BTreeQueryIterator end_query_it;
  auto num_elements = std::ranges::distance(query_iterator, end_query_it);
  LOG_SEV(Info) << "Num elements: " << num_elements;

  // for (; !query_iterator.IsEnd(); ++query_iterator) {
  //   auto entry = *query_iterator;
  //   // Interpret the data as a document.
  //   if (!entry->IsValid()) {
  //     continue;
  //   }
  //
  //   auto document = EntryToDocument(*entry);
  //   LOG_SEV(Info) << formatting::Format("{@BYELLOW}{}{@RESET}", neversql::PrettyPrint(*document));
  // }

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