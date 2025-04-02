//
// Created by Nathaniel Rupprecht on 12/19/24.
//

#pragma once

namespace neversql {

class Transaction {
public:
  void Commit();

  //! \brief Add a value with a specified key to the BTree.
  void AddValue(GeneralKey key, internal::EntryCreator& entry_creator);

  //! \brief Add a value with an auto-incrementing key to the B-tree.
  //!
  //! Only works if the B-tree is configured to generate auto-incrementing keys.
  //!
  //! \param entry_creator The entry creator that knows how to create an entry in the btree.
  void AddValue(internal::EntryCreator& entry_creator);

private:
  Transaction(uint64_t transaction_number)
      : transaction_number_(transaction_number) {}

  uint64_t transaction_number_;
};

}  // namespace neversql