#include <string>

#include "NeverSQL/NeverSQL.hpp"

exported_class::exported_class()
    : m_name {"NeverSQL"}
{}

auto exported_class::name() const -> char const* {
  return m_name.c_str();
}
