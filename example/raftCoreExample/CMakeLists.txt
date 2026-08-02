set(SRC_LIST raftKvDB.cpp)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun skip_list_on_raft rpc_lib protobuf::libprotobuf muduo_net muduo_base pthread )

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2}  ${src_common})
target_link_libraries(callerMain skip_list_on_raft protobuf::libprotobuf boost_serialization )

set(QPS_BENCHMARK_SOURCES qpsBenchmark.cpp qpsBenchmarkLogic.cpp)
add_executable(qpsBenchmark ${QPS_BENCHMARK_SOURCES} ${src_raftClerk} ${src_common})
target_include_directories(qpsBenchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(qpsBenchmark skip_list_on_raft protobuf::libprotobuf boost_serialization)

add_executable(test_qps_benchmark_logic qpsBenchmarkLogic.cpp ../../test/qps_benchmark_logic.cpp)
target_include_directories(test_qps_benchmark_logic PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(test_clerk_timeout_contract ../../test/clerk_timeout_contract.cpp ${src_raftClerk} ${src_common})
target_link_libraries(test_clerk_timeout_contract skip_list_on_raft protobuf::libprotobuf boost_serialization)
