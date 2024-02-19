//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#pragma once

#include <memory>
#include <vector>

#include "NeverSQL/utility/Defines.h"

namespace neversql {

template<typename T>
class BTreeNode {
public:
  NO_DISCARD bool IsLeaf() const { return children_.empty(); }

private:
  //! \brief The child nodes.
  std::vector<std::unique_ptr<BTreeNode>> children_;
  std::vector<T> items_;
};

// TODO: Implement.
template<typename T>
class BTree {
public:
private:
};

}  // namespace neversql