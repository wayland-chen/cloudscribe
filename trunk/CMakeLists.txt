project(CloudScribe)

# CloudScribe version components
set(VERSION_MAJOR "1")
set(VERSION_MINOR "0")
set(VERSION_MICRO "0")
set(VERSION_PATCH "0")

# cmake behavior compatibility
cmake_minimum_required(VERSION 2.4.6)
mark_as_advanced(CMAKE_BACKWARDS_COMPATIBILITY)

# saner control structure syntax
set(CMAKE_ALLOW_LOOSE_LOOP_CONSTRUCTS true)

if (COMMAND cmake_policy)
  # we prefer the more strict behavior, to find out more:
  # cmake --help-policy CMP0003
  cmake_policy(SET CMP0003 NEW)
endif ()

# Something nice for GUI like ccmake
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING
      "Options: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif ()

# install directory prefix
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/scribed"
      CACHE PATH "installation directory prefix" FORCE)
endif ()

# Enable testing
enable_testing()

# A simple test helper
set(TEST_DIFF ${CloudScribe_SOURCE_DIR}/bin/src-utils/testdiff)

# gcc warning settings
if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_definitions(-Wall -pedantic -Wno-long-long)
endif ()

add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT)

# Uncomment the following to build dynamic libraries
# set (BUILD_SHARED_LIBS ON)

# Locate required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# A more informative hypertable version string
include(VersionHelper)
# Install directory
set(INSTALL_DIR ${CMAKE_INSTALL_PREFIX}/${VERSION})

find_package(Threads REQUIRED)
message(STATUS "Use thread library: ${CMAKE_THREAD_LIBS_INIT}")
find_package(Boost REQUIRED)
#find_package(EXPAT REQUIRED)
find_package(Doxygen)
find_package(Tcmalloc)
find_package(GoogleHash)

find_package(LibEvent)
find_package(Thrift REQUIRED)
find_package(Zookeeper REQUIRED)


# set malloc library except in Debug mode, as tcmalloc makes program startup
# extremely slow, which slows down debug cycles.
if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(MALLOC_LIBRARY ${Tcmalloc_LIBRARIES})
endif ()

# use GoogleHash if found
if (GoogleHash_FOUND)
  include_directories(${GoogleHash_INCLUDE_DIR})
  add_definitions(-DCloudScribe_WITH_GOOGLE_HASH)
endif (GoogleHash_FOUND)



# include directories
include_directories(src/cc ${CloudScribe_BINARY_DIR}/src/cc
    ${Boost_INCLUDE_DIRS})


include_directories(${LibEvent_INCLUDE_DIR}
	${Thrift_INCLUDE_DIR} ${Thrift_INCLUDE_DIR}/thrift
	${Zookeeper_INCLUDE_DIR}
)
set(CloudxBase_IDL_DIR ${CloudScribe_SOURCE_DIR}/src/cc/cloudxbase/if)
set(Scribe_IDL_DIR ${CloudScribe_SOURCE_DIR}/src/cc/scribe/if)



if (BOOST_VERSION MATCHES "1_34")
  message(STATUS "Got boost 1.34.x, prepend fix directory")
  include_directories(BEFORE src/cc/boost-1_34-fix)
endif ()

if (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.")
  add_definitions(-Wno-variadic-macros)
  # turn off deprecated feature warnings for gcc 4.3+
  if (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.([3-9]|[1-9][0-9]+)")
    add_definitions(-Wno-deprecated)
  endif ()
elseif (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.")
  message(STATUS "Ancient gcc detected, hope it finishes despite warnings...")
endif ()

if (DocHelper_FOUND)
	include(DocHelper)
endif ()

# sub-project directories
add_subdirectory(src)
add_subdirectory(tests/integration)
