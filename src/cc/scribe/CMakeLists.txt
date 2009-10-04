set(ThriftGen_SRCS
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/reflection_limited_types.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CloudxService.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/forwarder.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/forwarder_constants.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/forwarder_types.cpp
)

add_library(ForwarderThrift ${ThriftGen_SRCS})
target_link_libraries(ForwarderThrift ${Thrift_LIBS} ${LibEvent_LIBS})

set(Forwarderd_SRCS
	inet_addr.c
	conf.cc
	conn_pool.cc
	file.cc
	forwarder_server.cc
	store.cc
	store_queue.cc
	group_service.cc
)

#forwarderd
add_executable(forwarderd ${Forwarderd_SRCS})
add_dependencies(forwarderd CloudxBaseThrift)
target_link_libraries(forwarderd
	CloudxBaseThrift ForwarderThrift ${Thrift_NB_LIB}
	${MALLOC_LIBRARY}
	${BOOST_SYSTEM_LIB}
	${BOOST_FILESYSTEM_LIB}
	${Zookeeper_LIB}
	rt pthread
)

#forwarder_cat
add_executable(forwarder_cat forwarder_cat.cc group_service.cc)
add_dependencies(forwarder_cat CloudxBaseThrift)
target_link_libraries(forwarder_cat CloudxBaseThrift ForwarderThrift
	${Thrift_LIB}
	${BOOST_SYSTEM_LIB}
	${Zookeeper_LIB}
	pthread
)



add_custom_command(
  OUTPUT    ${ThriftGen_SRCS}
  COMMAND   thrift
  ARGS      -r --gen cpp -o ${CMAKE_CURRENT_SOURCE_DIR} -I ../cloudxbase/if
			${CMAKE_CURRENT_SOURCE_DIR}/if/forwarder.thrift
  DEPENDS   ../cloudxbase/if/cloudxbase.thrift
			../cloudxbase/if/reflection_limited.thrift
  COMMENT   "thrift2cpp"
)



# Basic regression test
add_executable(client_test tests/client_test.cc)
target_link_libraries(client_test ForwarderThrift CloudxBaseThrift ${BOOST_SYSTEM_LIB})
add_test(ThriftClient-cpp client_test)

install(TARGETS ForwarderThrift forwarderd forwarder_cat
        RUNTIME DESTINATION ${VERSION}/forwarder/bin
        LIBRARY DESTINATION ${VERSION}/forwarder/lib
        ARCHIVE DESTINATION ${VERSION}/forwarder/lib)
install(FILES if/forwarder.thrift DESTINATION ${VERSION}/forwarder/include)
