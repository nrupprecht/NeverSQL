#pragma once

#include "neversql/utility/Defines.h"

namespace neversql {

template<typename Struct_t, typename Member_t>
constexpr page_size_t offset(Member_t Struct_t::* member) {
  auto diff = reinterpret_cast<std::size_t>(&(((Struct_t*)0)->*member));
  return static_cast<page_size_t>(diff);
}

//! \brief Macro useful for defining layout used for offset calculations.
#define STRUCT_DATA(type, name) std::byte name[sizeof(type)]

}  // namespace neversql