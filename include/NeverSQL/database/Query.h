//
// Created by Nathaniel Rupprecht on 3/20/24.
//

#pragma once

#include <utility>

#include "NeverSQL/data/Document.h"
#include "NeverSQL/data/btree/BTree.h"

namespace neversql::query {

class Condition : public lightning::ImplBase {
  friend class lightning::ImplBase;

protected:
  class Impl : public lightning::ImplBase::Impl {
  public:
    virtual bool Test(const Document& reader) const = 0;
    virtual std::shared_ptr<Impl> Copy() const = 0;
  };

  explicit Condition(const std::shared_ptr<Impl>& impl)
      : lightning::ImplBase(impl) {}

public:
  bool operator()(const Document& reader) const { return impl<Condition>()->Test(reader); }
  Condition Copy() const { return Condition(impl<Condition>()->Copy()); }
};

template<typename Data_t, typename Predicate_t>
class Comparison : public Condition {
  friend class lightning::ImplBase;

protected:
  class Impl : public Condition::Impl {
  public:
    Impl(std::string field_name, Data_t value)
        : field_name_(std::move(field_name))
        , value_(value) {}

    bool Test(const Document& reader) const override {
      if (auto field_value = reader.TryGetAs<Data_t>(field_name_)) {
        return Predicate_t {}(*field_value, value_);
      }
      return false;
    }

    std::shared_ptr<Condition::Impl> Copy() const override {
      return std::make_shared<Impl>(field_name_, value_);
    }

  private:
    std::string field_name_;
    Data_t value_;
  };

  explicit Comparison(const std::shared_ptr<Impl>& impl)
      : Condition(impl) {}

public:
  Comparison(const std::string& field_name, Data_t value)
      : Condition(std::make_shared<Impl>(field_name, value)) {}
};

template<typename Data_t>
using LessThan = Comparison<Data_t, std::less<Data_t>>;

template<typename Data_t>
using LessEqual = Comparison<Data_t, std::less_equal<Data_t>>;

template<typename Data_t>
using GreaterThan = Comparison<Data_t, std::greater<Data_t>>;

template<typename Data_t>
using GreaterEqual = Comparison<Data_t, std::greater_equal<Data_t>>;

//! \brief A query iterator. This wraps an ordinary BTreeManager::Iterator and filters the results based on a
//!        condition. This allows us to iterate though a collection, only counting documents that meet a
//!        certain condition.
class BTreeQueryIterator {
public:
  using difference_type = std::ptrdiff_t;
  using value_type = std::span<const std::byte>;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::forward_iterator_tag;

  BTreeQueryIterator(BTreeManager::Iterator iterator, Condition condition)
      : iterator_(std::move(iterator))
      , condition_(std::move(condition)) {
    advance();
  }

  std::span<const std::byte> operator*() const { return *iterator_; }

  BTreeQueryIterator& operator++() {
    ++iterator_;
    advance();
    return *this;
  }

  bool operator==(const BTreeQueryIterator& other) const noexcept { return iterator_ == other.iterator_; }

  bool operator!=(const BTreeQueryIterator& other) const noexcept { return iterator_ != other.iterator_; }

  bool IsEnd() const noexcept { return iterator_.IsEnd(); }

private:
  void advance() {
    // Find the next valid iterator.
    for (; !iterator_.IsEnd(); ++iterator_) {
      auto document = ReadDocumentFromBuffer(*iterator_);
      if (condition_(*document)) {
        return;
      }
    }
  }

  BTreeManager::Iterator iterator_;
  Condition condition_;
};

}  // namespace neversql::query