//
// Created by Nathaniel Rupprecht on 3/2/24.
//

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "neversql/utility/Defines.h"

namespace neversql::utility {

//! \brief A class that can be used to construct, format, and display a table of data.
class DisplayTable {
public:
  //! \brief Base class for columns. Contains no type info, but contains column and column formatting
  //! information.
  struct Column {
    virtual ~Column() = default;

    //! \brief The column name.
    std::string column_name;

    //! \brief The minimum formatted width of the column.
    std::optional<std::size_t> min_width {};
    //! \brief The maximum formatted width of the column.
    std::optional<std::size_t> max_allowed_width {};

    //! \brief The color of the column name, in the table header. Should use the lightning Format special formatting syntax.
    std::string name_color = "DEFAULT";
    //! \brief The color to format the data in the column. Should use the lightning Format special formatting syntax.
    std::string data_color = "DEFAULT";
    //! \brief When calculating the width the column should be formatted into, the hint from the column name will be
    //! the size of the column name in characters, plus the buffer.
    std::size_t column_name_buffer = 4;
    //! \brief When calculating the width the column should be formatted into, the hint from the data will be the
    //! maximum size of any formatted data entry in characters, plus the buffer.
    std::size_t data_width_buffer = 4;

    //! \brief The alignment of the column name in the table header. Should be one of '<', '>', or '^'.
    char column_name_alignment = '^';
    //! \brief The alignment of the data in its row space. Should be one of '<', '>', or '^'.
    char data_alignment = '^';

    //! \brief Get the number of rows in the column.
    virtual std::size_t GetNumRows() const noexcept = 0;
    //! \brief Get the maximum width of any (data) entry in the column.
    virtual std::size_t GetMaxEntryWidth() const noexcept = 0;
    //! \brief Get the string representation of the data at the given row.
    virtual std::string GetAsString(std::size_t row) const = 0;

    //! \brief Get the width of the column, in characters. This is calculated based off the min and max allowed widths
    //! (if any), the column name, and the data (inclusive of buffers).
    std::size_t GetColumnWidth() const noexcept;
  };

  //! \brief A concrete (type aware) column that holds the actual data.
  template<typename T>
  struct ConcreteColumn : Column {
    //! \brief Concrete storage for the data in the column.
    std::vector<T> values;
    //! \brief Store a serialization function for the data.
    std::function<std::string(const T&)> formatter;

    //! \brief Implementation to get the number of rows in the column.
    NO_DISCARD std::size_t GetNumRows() const noexcept override { return values.size(); }

    //! \brief Implementation to get the maximum width of any (data) entry in the column.
    NO_DISCARD std::size_t GetMaxEntryWidth() const noexcept override {
      std::size_t max_width = 0;
      for (auto& value : values) {
        max_width = std::max(formatter(value).size(), max_width);
      }
      return max_width;
    }

    //! \brief Implementation to get the string representation of the data at the given row.
    NO_DISCARD std::string GetAsString(std::size_t row) const override { return formatter(values[row]); }
  };

  //! \brief Get the string representation of the table at the given row and column.
  //!
  //! Does not include padding, alignment, coloring, etc.
  NO_DISCARD std::string GetAsString(std::size_t row, std::size_t col) const;

  //! \brief Get the width of each column, and the total width of the table.
  NO_DISCARD std::pair<std::size_t, std::vector<std::size_t>> GetWidthInfo() const;

  //! \brief Get the total width of the table, in characters.
  NO_DISCARD std::size_t GetTotalWidth() const;

  //! \brief Streaming operator formats and serializes the table to a stream.
  friend std::ostream& operator<<(std::ostream& out, const DisplayTable& table);

  //! \brief Add a column to the table.
  template<typename T, typename Func_t>
  std::shared_ptr<ConcreteColumn<T>> AddColumn(const std::string& name,
                                               std::vector<T> data,
                                               Func_t&& formatter,
                                               const std::string& data_color = "DEFAULT",
                                               const std::string& name_color = "DEFAULT") {
    auto column = std::make_shared<ConcreteColumn<T>>();
    column->column_name = name;
    column->values = std::move(data);
    column->formatter = std::forward<Func_t>(formatter);
    column->data_color = data_color;
    column->name_color = name_color;
    columns_.push_back(column);
    return column;
  }

private:
  //! \brief The collection of columns in the table.
  std::vector<std::shared_ptr<Column>> columns_;
};

}  // namespace neversql::utility