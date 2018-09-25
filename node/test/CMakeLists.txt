set(test_srcs node_test.cpp session_test.cpp)
set(test_libs node proto chaos options protobuf.a)

add_gmock_test(node)
