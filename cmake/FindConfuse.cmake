# - Find CONFUSEapi
# This module defines
#  CONFUSE_INCLUDE_DIR, where to find LibEvent headers
#  CONFUSE_LIBS, CONFUSE libraries
#  CONFUSE_FOUND, If false, do not try to use ant

find_path(CONFUSE_INCLUDE_DIR confuse.h PATHS
    /usr/local/include
)

set(CONFUSE_LIB_PATHS /usr/local/lib /opt/CONFUSE-dev/lib)
find_library(CONFUSE_LIB NAMES confuse PATHS ${CONFUSE_LIB_PATHS})

if (CONFUSE_LIB AND CONFUSE_INCLUDE_DIR)
  set(CONFUSE_FOUND TRUE)
  set(CONFUSE_LIBS ${CONFUSE_LIB})
else ()
  set(CONFUSE_FOUND FALSE)
endif ()

if (CONFUSE_FOUND)
  if (NOT CONFUSE_FIND_QUIETLY)
    message(STATUS "Found CONFUSE: ${CONFUSE_LIBS}")
  endif ()
else ()
  message(STATUS "CONFUSE NOT found.")
endif ()

mark_as_advanced(
    CONFUSE_LIB
    CONFUSE_INCLUDE_DIR
)
