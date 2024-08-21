//
// Created by Nathaniel Rupprecht on 3/20/24.
//

#pragma once

#include <utility>

#include "NeverSQL/data/Document.h"
#include "NeverSQL/data/btree/BTree.h"

namespace neversql::query {

class Condition : public lightning::ImplBase {
  friend class ImplBase;

protected:
  class Impl : public ImplBase::Impl {
  public:
    virtual bool Test(const Document& reader) const = 0;
    virtual std::shared_ptr<Impl> Copy() const = 0;
  };

  explicit Condition(const std::shared_ptr<Impl>& impl)
      : ImplBase(impl) {}

public:
  bool operator()(const Document& reader) const { return impl<Condition>()->Test(reader); }
  Condition Copy() const { return Condition(impl<Condition>()->Copy()); }
};

//! \brief A condition that always evaluates to true, used as a placeholder.
class AlwaysTrue : public Condition {
  friend class ImplBase;

protected:
  class Impl final : public Condition::Impl {
    bool Test([[maybe_unused]] const Document& reader) const override { return true; }

    std::shared_ptr<Condition::Impl> Copy() const override { return std::make_shared<Impl>(); }
  };

public:
  AlwaysTrue()
      : Condition(std::make_shared<Impl>()) {}
};

//! \brief Base condition for binary comparisons.
template<typename Data_t, typename Predicate_t>
class Comparison : public Condition {
  friend class ImplBase;

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
using Equal = Comparison<Data_t, std::equal_to<Data_t>>;

template<typename Data_t>
using NotEqual = Comparison<Data_t, std::not_equal_to<Data_t>>;

template<typename Data_t>
using LessThan = Comparison<Data_t, std::less<Data_t>>;

template<typename Data_t>
using LessEqual = Comparison<Data_t, std::less_equal<Data_t>>;

template<typename Data_t>
using GreaterThan = Comparison<Data_t, std::greater<Data_t>>;

template<typename Data_t>
using GreaterEqual = Comparison<Data_t, std::greater_equal<Data_t>>;

//! \brief A condition that a document has a field of a certain name. Optionally, the type of the field can be
//!        checked as well.
class HasField : public Condition {
  friend class ImplBase;

protected:
  class Impl final : public Condition::Impl {
  public:
    explicit Impl(std::string field_name, std::optional<DataTypeEnum> type = {})
        : field_name_(std::move(field_name))
        , type_(type) {}

    bool Test(const Document& document) const override {
      if (auto field = document.GetElement(field_name_)) {
        if (type_) {
          return field->get().GetDataType() == *type_;
        }
        return true;
      }
      return false;
    }

    std::shared_ptr<Condition::Impl> Copy() const override {
      return std::make_shared<Impl>(field_name_, type_);
    }

  private:
    std::string field_name_;
    std::optional<DataTypeEnum> type_;
  };

  explicit HasField(const std::shared_ptr<Impl>& impl)
      : Condition(impl) {}

public:
  explicit HasField(const std::string& field_name, std::optional<DataTypeEnum> type = {})
      : Condition(std::make_shared<Impl>(field_name, type)) {}
};

//! \brief A query iterator. This wraps an ordinary BTreeManager::Iterator and filters the results based on a
//!        condition. This allows us to iterate though a collection, only counting documents that meet a
//!        certain condition.
class BTreeQueryIterator {
public:
  using difference_type = std::ptrdiff_t;
  using value_type = std::unique_ptr<internal::DatabaseEntry>;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::forward_iterator_tag;

  BTreeQueryIterator()
      : condition_(AlwaysTrue {}) {}

  BTreeQueryIterator(const BTreeQueryIterator& other)
      : iterator_(other.iterator_)
      , condition_(other.condition_.Copy()) {}

  BTreeQueryIterator(BTreeQueryIterator&& other) noexcept
      : iterator_(std::move(other.iterator_))
      , condition_(std::move(other.condition_)) {}

  BTreeQueryIterator(BTreeManager::Iterator iterator, Condition condition)
      : iterator_(std::move(iterator))
      , condition_(std::move(condition)) {
    advance();
  }

  BTreeQueryIterator& operator=(const BTreeQueryIterator& other) {
    iterator_ = other.iterator_;
    condition_ = other.condition_.Copy();
    return *this;
  }

  BTreeQueryIterator& operator=(BTreeQueryIterator&& other) {
    iterator_ = std::move(other.iterator_);
    condition_ = std::move(other.condition_);
    return *this;
  }

  std::unique_ptr<internal::DatabaseEntry> operator*() const { return *iterator_; }

  //! \brief Pre-incrementation operator.
  BTreeQueryIterator& operator++() {
    ++iterator_;
    advance();
    return *this;
  }

  //! \brief Post-incrementation operator.
  BTreeQueryIterator operator++(int) {
    BTreeQueryIterator it = *this;
    ++(*this);
    return it;
  }

  bool operator==(const BTreeQueryIterator& other) const noexcept { return iterator_ == other.iterator_; }

  bool operator!=(const BTreeQueryIterator& other) const noexcept { return iterator_ != other.iterator_; }

  bool IsEnd() const noexcept { return iterator_.IsEnd(); }

private:
  void advance() {
    // Find the next valid iterator.
    for (; !iterator_.IsEnd(); ++iterator_) {
      auto entry = *iterator_;
      const auto document = EntryToDocument(*entry);
      if (condition_(*document)) {
        return;
      }
    }
  }

  BTreeManager::Iterator iterator_;
  Condition condition_;
};

}  // namespace neversql::query