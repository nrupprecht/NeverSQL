if(PROJECT_IS_TOP_LEVEL)
  set(
      CMAKE_INSTALL_INCLUDEDIR "include/NeverSQL-${PROJECT_VERSION}"
      CACHE PATH ""
  )
endif()

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

# find_package(<package>) call for consumers to find this project
set(package NeverSQL)

install(
    DIRECTORY
    include/
    "${PROJECT_BINARY_DIR}/export/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    COMPONENT NeverSQL_Development
)

install(
    TARGETS NeverSQL_NeverSQL
    EXPORT NeverSQLTargets
    RUNTIME #
    COMPONENT NeverSQL_Runtime
    LIBRARY #
    COMPONENT NeverSQL_Runtime
    NAMELINK_COMPONENT NeverSQL_Development
    ARCHIVE #
    COMPONENT NeverSQL_Development
    INCLUDES #
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

write_basic_package_version_file(
    "${package}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
)

# Allow package maintainers to freely override the path for the configs
set(
    NeverSQL_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/${package}"
    CACHE PATH "CMake package config location relative to the install prefix"
)
mark_as_advanced(NeverSQL_INSTALL_CMAKEDIR)

install(
    FILES cmake/install-config.cmake
    DESTINATION "${NeverSQL_INSTALL_CMAKEDIR}"
    RENAME "${package}Config.cmake"
    COMPONENT NeverSQL_Development
)

install(
    FILES "${PROJECT_BINARY_DIR}/${package}ConfigVersion.cmake"
    DESTINATION "${NeverSQL_INSTALL_CMAKEDIR}"
    COMPONENT NeverSQL_Development
)

install(
    EXPORT NeverSQLTargets
    NAMESPACE NeverSQL::
    DESTINATION "${NeverSQL_INSTALL_CMAKEDIR}"
    COMPONENT NeverSQL_Development
)

if(PROJECT_IS_TOP_LEVEL)
  include(CPack)
endif()
