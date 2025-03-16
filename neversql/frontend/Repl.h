//
// Created by Nathaniel Rupprecht on 3/19/24.
//

#pragma once

#include "neversql/database/DataManager.h"

namespace neversql::frontend {
class REPLManager {
public:
  explicit REPLManager(DataManager* data_manager);

  void REPLLoop();

private:
  bool parseCommand(const std::string& command);

  void processCommand();

  DataManager* data_manager_{};

  std::stringstream command_;

  bool continue_loop_ = true;
};

}  // namespace neversql::frontend