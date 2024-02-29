//
// Created by Nathaniel Rupprecht on 2/28/24.
//

#pragma once

#include "NeverSQL/data/btree/BTreeNodeMap.h"

namespace neversql::utility {

class PageInspector {
public:
  static void NodePageDump(const BTreeNodeMap& node, std::ostream& out);
};

}