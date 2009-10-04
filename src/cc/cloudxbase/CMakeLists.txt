set(ThriftGen_SRCS
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CloudxService.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/cloudxbase_constants.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/cloudxbase_types.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/reflection_limited_constants.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/reflection_limited_types.cpp
)

add_library(CloudxBaseThrift ServiceTracker.cc CloudxBase.cc ${ThriftGen_SRCS})
target_link_libraries(CloudxBaseThrift ${Thrift_LIBS} ${LibEvent_LIBS})

add_custom_command(
  OUTPUT    ${ThriftGen_SRCS}
  COMMAND   thrift
  ARGS      -r --gen cpp -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/if/cloudxbase.thrift
  DEPENDS   ${CMAKE_CURRENT_SOURCE_DIR}/if/reflection_limited.thrift
  COMMENT   "thrift2cpp"
)

install(TARGETS CloudxBaseThrift
        LIBRARY DESTINATION ${VERSION}/cloudxbase/lib
        ARCHIVE DESTINATION ${VERSION}/cloudxbase/lib)
install(FILES if/reflection_limited.thrift if/cloudxbase.thrift DESTINATION
${VERSION}/cloudxbase/include)
