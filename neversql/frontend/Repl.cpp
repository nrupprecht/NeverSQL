#include "neversql/frontend/Repl.h"

namespace neversql::frontend {

namespace {

std::vector<std::string> split(std::string_view str) {
  if (str.empty()) {
    return {};
  }

  std::vector<std::string> result;
  // Split the input string at spaces, putting each segment into the result vector.
  const auto* it_start = std::ranges::find_if_not(str, [](auto c) { return std::isspace(c); });
  const auto* it_end = std::ranges::find(it_start, str.end(), ' ');

  while (it_start != str.end()) {
    result.emplace_back(it_start, it_end);

    it_start = std::ranges::find_if_not(it_end, str.end(), [](auto c) { return std::isspace(c); });
    it_end = std::ranges::find(it_start, str.end(), ' ');
  }
  return result;
}

}  // namespace

REPLManager::REPLManager(DataManager* data_manager)
    : data_manager_(data_manager) {}

void REPLManager::REPLLoop() {
  std::string command;
  while (continue_loop_) {
    std::cout << lightning::formatting::Format("{@BBLUE}neversql{@RESET}> ");
    std::cin >> command;

    // Parse command, determine whether this is a multi-line command.
    while (!parseCommand(command)) {
      std::cout << ">>";
      std::cin >> command;
    }

    processCommand();
  }

  std::cout << "Exited neversql. Have a pleasant day!" << std::endl;
}

bool REPLManager::parseCommand(const std::string& command) {
  command_ << command << " ";
  return true;
}

void REPLManager::processCommand() {
  LOG_SEV(Info) << "Command is '" << command_.str() << "'.";

  auto segments = split(command_.str());

  if (segments.size() == 1 && segments.at(0) == "exit") {
    continue_loop_ = false;
  }
  else if (segments.size() == 3) {
    if (segments.at(0) == "create" && segments.at(1) == "collection") {
      auto&& collection_name = segments.at(2);
      data_manager_->AddCollection(collection_name, DataTypeEnum::UInt64);
      std::cout << lightning::formatting::Format(">> Created collection named \"{@BYELLOW}{}{@RESET}.\"",
                                                 collection_name)
                << std::endl;
      // Reset the command buffer.
      command_.str("");
    }
  }
  else if (segments.size() == 2) {
    if (segments.at(0) == "count") {
      auto&& collection_name = segments.at(1);
      auto it = data_manager_->Begin(collection_name);
      auto end = data_manager_->End(collection_name);
      std::size_t num_elements = 0;
      for (;it != end; ++num_elements, ++it);

      std::cout << lightning::formatting::Format(
          ">> Collection \"{@BYELLOW}{}{@RESET}\" has {:L} elements.", collection_name, num_elements)
                << std::endl;
      // Reset the command buffer.
      command_.str("");
    }
    else if (segments.at(0) == "list" && segments.at(1) == "collections") {
      auto collection_names = data_manager_->GetCollectionNames();
      if (collection_names.empty()) {
        std::cout << "There are no collections in the database." << std::endl;
      }
      else {
        std::cout << "All collections:" << std::endl;
        for (const auto& name : collection_names) {
          std::cout << lightning::formatting::Format(">> Collection: \"{@BYELLOW}{}{@RESET}\".", name)
                    << std::endl;
        }
      }
      // Reset the command buffer.
      command_.str("");
    }
  }
  else {
    // std::cout << "Unrecognized command: " << lightning::formatting::Format("{:?}", command_.str())
    //           << std::endl;
  }
}

}  // namespace neversql::frontend