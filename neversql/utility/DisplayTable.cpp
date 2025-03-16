//
// Created by Nathaniel Rupprecht on 3/2/24.
//

#include "neversql/utility/DisplayTable.h"
// Other files.
#include <numeric>
#include <ranges>
#include <iterator>

#include <Lightning/Lightning.h>

namespace neversql::utility {

std::size_t DisplayTable::Column::GetColumnWidth() const noexcept {
  auto max_entry = GetMaxEntryWidth() + data_width_buffer;
  return std::min(
      std::max(min_width.value_or(0), std::max(max_entry, column_name.size() + column_name_buffer)),
      max_allowed_width.value_or(1000UL));
}

std::string DisplayTable::GetAsString(std::size_t row, std::size_t col) const {
  NOSQL_REQUIRE(col < columns_.size(), "column index out of bounds");
  return columns_[col]->GetAsString(row);
}

std::pair<std::size_t, std::vector<std::size_t>> DisplayTable::GetWidthInfo() const {
  std::vector<std::size_t> column_widths;
  column_widths.reserve(columns_.size());
  for (const auto& col : columns_) {
    column_widths.push_back(col->GetColumnWidth());
  }
  return {std::accumulate(column_widths.begin(), column_widths.end(), 0UL) + column_widths.size() + 1UL,
          column_widths};
}

std::size_t DisplayTable::GetTotalWidth() const {
  return GetWidthInfo().first;
}

std::ostream& operator<<(std::ostream& out, const DisplayTable& table) {
  using lightning::formatting::Format;

  if (table.columns_.empty()) {
    return out;
  }
  // Ensure that all columns have the same number of rows.
  auto check_size = table.columns_.front()->GetNumRows();
  std::ranges::for_each(table.columns_, [&check_size](const auto& col) {
    NOSQL_ASSERT(col->GetNumRows() == check_size, "column sizes do not match");
  });

  // Calculate the width of each column, and the total width of the table.
  auto [total_width, column_widths] = table.GetWidthInfo();

  std::fill_n(std::ostream_iterator<char>(out), total_width, '=');
  out << std::endl << "|";
  for (std::size_t col = 0; col < table.columns_.size(); ++col) {
    auto& column = *table.columns_[col];
    out << Format("{@" + column.name_color + "}{:" + column.column_name_alignment
                      + std::to_string(column_widths[col]) + "}{@RESET}",
                  table.columns_[col]->column_name);
    out << "|";
  }
  out << std::endl;

  std::fill_n(std::ostream_iterator<char>(out), total_width, '=');
  out << std::endl;

  // Write each row, one by one.
  for (std::size_t row = 0; row < check_size; ++row) {
    out << "|";
    for (std::size_t j = 0; j < table.columns_.size(); ++j) {
      auto str = table.GetAsString(row, j);
      auto& column = *table.columns_[j];
      auto formatted_string = Format("{@" + column.data_color + "}{:" + column.data_alignment
                                         + std::to_string(column_widths[j]) + "}{@RESET}",
                                     str);
      out << formatted_string;
      out << "|";
    }
    out << std::endl;
  }
  // Write fotter.
  std::fill_n(std::ostream_iterator<char>(out), total_width, '=');
  out << std::endl;

  return out;
}

}  // namespace neversql::utility