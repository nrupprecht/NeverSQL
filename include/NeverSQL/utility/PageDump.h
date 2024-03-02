//
// Created by Nathaniel Rupprecht on 2/28/24.
//

#pragma once

#include "NeverSQL/data/btree/BTreeNodeMap.h"

namespace neversql::utility {

//! \brief Class that can inspect a BTree node page and dump a report about the node to a stream.
class PageInspector {
public:
  static void NodePageDump(const BTreeNodeMap& node, std::ostream& out);
};

}  // namespace neversql::utility
