cmake_minimum_required(VERSION 3.14)

include(cmake/prelude.cmake)

project(
    NeverSQL
    VERSION 0.1.0
    DESCRIPTION "A small implementation of a no-sql database."
    HOMEPAGE_URL "https://example.com/"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

include(cmake/project-is-top-level.cmake)
include(cmake/variables.cmake)

# ---- Options ----

option(BUILD_NEVERSQL_APPLICATIONS ON)

# ---- Declare library ----

include (FetchContent)

FetchContent_Declare(
        Lightning
        GIT_REPOSITORY https://github.com/nrupprecht/Lightning.git
        GIT_TAG        6e47a5a
)
FetchContent_MakeAvailable(Lightning)
include_directories(${Lightning_SOURCE_DIR}/include)

add_library(
    NeverSQL_NeverSQL
    # Source files.
    source/data/DataAccessLayer.cpp
    source/data/FreeList.cpp
    source/data/btree/BTree.cpp
    source/data/btree/BTreeNodeMap.cpp
    source/utility/HexDump.cpp
)
add_library(NeverSQL::NeverSQL ALIAS NeverSQL_NeverSQL)

include(GenerateExportHeader)
generate_export_header(
    NeverSQL_NeverSQL
    BASE_NAME NeverSQL
    EXPORT_FILE_NAME export/NeverSQL/NeverSQL_export.hpp
    CUSTOM_CONTENT_FROM_VARIABLE pragma_suppress_c4251
)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(NeverSQL_NeverSQL PUBLIC NEVERSQL_STATIC_DEFINE)
endif()

set_target_properties(
    NeverSQL_NeverSQL PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    VERSION "${PROJECT_VERSION}"
    SOVERSION "${PROJECT_VERSION_MAJOR}"
    EXPORT_NAME NeverSQL
    OUTPUT_NAME NeverSQL
)

target_include_directories(
    NeverSQL_NeverSQL ${warning_guard}
    PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
)

target_include_directories(
    NeverSQL_NeverSQL SYSTEM
    PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/export>"
)

target_compile_features(NeverSQL_NeverSQL PUBLIC cxx_std_20)

# target_link_libraries(NeverSQL_NeverSQL PRIVATE fmt::fmt)


#if (BUILD_NEVERSQL_APPLICATIONS)
  message("Building applications.")
  add_subdirectory("${PROJECT_SOURCE_DIR}/applications")
#else()
#  message("Not building applications.")
#endif()

# ---- Install rules ----

if(NOT CMAKE_SKIP_INSTALL_RULES)
  include(cmake/install-rules.cmake)
endif()

# ---- Developer mode ----

if(NOT NeverSQL_DEVELOPER_MODE)
  return()
elseif(NOT PROJECT_IS_TOP_LEVEL)
  message(
      AUTHOR_WARNING
      "Developer mode is intended for developers of NeverSQL"
  )
endif()

include(cmake/dev-mode.cmake)
