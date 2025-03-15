cmake_minimum_required(VERSION 3.10)
project(MapReduce)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找必要的包
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# 包含目录
include_directories(
  ${Protobuf_INCLUDE_DIRS}
  ${gRPC_INCLUDE_DIRS}
  /usr/local/include
)

# 收集protobuf生成文件
file(GLOB PROTO_SOURCES
    "MapReduce.grpc.pb.cc"
    "MapReduce.pb.cc"
)

# 添加主程序
set(MAIN_SOURCE MapReduce.cc)

# 创建可执行程序
foreach(_target
        Master Work)
    add_executable(${_target} "${_target}.cc" ${PROTO_SOURCES})

    # 链接库
    target_link_libraries(${_target}
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
    # utf8 protobuf v25.0+ 以后的版本需要连接
    utf8_validity
    absl_utf8_for_code_point
    utf8_range
    absl_base
    absl_strings
    pthread
    ${gRPC_CARES_LIBRARIES}
    ${gRPC_ADDRESS_SORTING_LIBRARIES}
    ${gRPC_ABSL_LIBRARIES}
    ) 

endforeach(_target)



# 添加库搜索路径
link_directories(/usr/local/lib)


