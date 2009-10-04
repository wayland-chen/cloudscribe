# - Find snacc
# This module defines
#  SNACC_LIBS, SNACC libraries
#  SNACC_FOUND, If false, do not try to use ant

#find_path(TDB_INCLUDE_DIR tdb.h PATHS
#    /usr/local/include
#    /opt/tdb-dev/include
#  )

set(SNACC_LIB_PATHS /usr/local/lib /opt/tdb-dev/lib)
find_library(SNACC_LIB NAMES asn1c++ PATHS ${SNACC_LIB_PATHS})

if (SNACC_LIB)
  set(SNACC_FOUND TRUE)
  set(SNACC_LIBS ${SNACC_LIB})
else ()
  set(SNACC_FOUND FALSE)
endif ()

if (SNACC_FOUND)
  if (NOT SNACC_FIND_QUIETLY)
    message(STATUS "Found SNACC: ${SNACC_LIBS}")
  endif ()
else ()
  message(STATUS "SNACC NOT found.")
endif ()

mark_as_advanced(
    SNACC_LIB
)
