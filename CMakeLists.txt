cmake_minimum_required(VERSION 3.8)
project(pointcloud_bridge)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(nav_msgs REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED COMPONENTS system iostreams program_options thread)
find_package(Eigen3 REQUIRED)
find_package(PCL REQUIRED COMPONENTS common filters)
find_package(pcl_conversions REQUIRED)

include(ExternalProject)

# Build lazperf as external project
ExternalProject_Add(
  lazperf_external
  GIT_REPOSITORY https://github.com/hobu/laz-perf.git
  GIT_TAG master
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/lazperf_install
    -DCMAKE_BUILD_TYPE=Release
  BUILD_COMMAND ${CMAKE_COMMAND} --build . --config Release
  INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install
)

# Build libLAS as external project
ExternalProject_Add(
  liblas_external
  GIT_REPOSITORY https://github.com/libLAS/libLAS.git
  GIT_TAG master
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/liblas_install
    -DCMAKE_BUILD_TYPE=Release
    -DWITH_LASZIP=OFF
    -DWITH_GEOTIFF=ON
  BUILD_COMMAND ${CMAKE_COMMAND} --build . --config Release
  INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install
)

set(LAZPERF_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/lazperf_install/include)
set(LAZPERF_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/lazperf_install/lib/liblazperf.so)
set(LIBLAS_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/liblas_install/include)
set(LIBLAS_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/liblas_install/lib/liblas.so)

# Generate protobuf files
set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/pointcloud.proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Generate protobuf sources
add_custom_command(
  OUTPUT ${PROTO_GEN_DIR}/pointcloud.pb.cc ${PROTO_GEN_DIR}/pointcloud.pb.h
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    --cpp_out=${PROTO_GEN_DIR}
    -I${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf files"
)

# Bridge node executable
add_executable(bridge_node
  src/bridge_node.cpp
  ${PROTO_GEN_DIR}/pointcloud.pb.cc
)

target_include_directories(bridge_node PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${PROTO_GEN_DIR}
  ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(bridge_node
  ${Protobuf_LIBRARIES}
  Boost::system
  pthread
)

ament_target_dependencies(bridge_node
  rclcpp
  sensor_msgs
  geometry_msgs
  nav_msgs
)

# Recorder node executable
add_executable(recorder_node
  src/recorder_node.cpp
)

target_include_directories(recorder_node PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${EIGEN3_INCLUDE_DIRS}
  ${LIBLAS_INCLUDE_DIR}
  ${LAZPERF_INCLUDE_DIR}
)

target_link_libraries(recorder_node
  Eigen3::Eigen
  ${LIBLAS_LIBRARY}
  ${LAZPERF_LIBRARY}
  Boost::system
  Boost::iostreams
  Boost::program_options
  Boost::thread
)

# Make sure external projects are built before recorder_node
add_dependencies(recorder_node lazperf_external liblas_external)

ament_target_dependencies(recorder_node
  rclcpp
  sensor_msgs
  geometry_msgs
  nav_msgs
)

# Optimized recorder node (with PCL, async I/O)
add_executable(recorder_node_optimized
  src/recorder_node_optimized.cpp
)

target_include_directories(recorder_node_optimized PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${EIGEN3_INCLUDE_DIRS}
  ${LIBLAS_INCLUDE_DIR}
  ${LAZPERF_INCLUDE_DIR}
  ${PCL_INCLUDE_DIRS}
)

target_link_libraries(recorder_node_optimized
  Eigen3::Eigen
  ${LIBLAS_LIBRARY}
  ${LAZPERF_LIBRARY}
  ${PCL_LIBRARIES}
  Boost::system
  Boost::iostreams
  Boost::program_options
  Boost::thread
  pthread
)

add_dependencies(recorder_node_optimized lazperf_external liblas_external)

ament_target_dependencies(recorder_node_optimized
  rclcpp
  sensor_msgs
  geometry_msgs
  nav_msgs
  pcl_conversions
)

# Install targets
# Install external libraries (liblas, lazperf) to package lib directory
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/liblas_install/lib/
  DESTINATION lib
  FILES_MATCHING PATTERN "*.so*"
)
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lazperf_install/lib/
  DESTINATION lib
  FILES_MATCHING PATTERN "*.so*"
)

install(TARGETS bridge_node recorder_node recorder_node_optimized
  DESTINATION lib/${PROJECT_NAME}
)

# Install Python scripts
install(PROGRAMS
  scripts/perf_test.py
  DESTINATION lib/${PROJECT_NAME}
)

# Install launch files
install(DIRECTORY launch/
  DESTINATION share/${PROJECT_NAME}/launch
)

# Install config files
install(DIRECTORY config/
  DESTINATION share/${PROJECT_NAME}/config
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  # Add GTest
  find_package(ament_cmake_gtest REQUIRED)

  # Writer unit tests
  ament_add_gtest(test_writers
    test/test_writers.cpp
  )

  target_include_directories(test_writers PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${EIGEN3_INCLUDE_DIRS}
    ${LIBLAS_INCLUDE_DIR}
    ${LAZPERF_INCLUDE_DIR}
  )

  target_link_libraries(test_writers
    Eigen3::Eigen
    ${LIBLAS_LIBRARY}
    ${LAZPERF_LIBRARY}
    Boost::system
    Boost::iostreams
    Boost::program_options
    Boost::thread
  )

  add_dependencies(test_writers lazperf_external liblas_external)

  ament_target_dependencies(test_writers
    rclcpp
    sensor_msgs
    geometry_msgs
    nav_msgs
  )

  # Benchmark tests (with PCL)
  ament_add_gtest(benchmark_operations
    test/benchmark_operations.cpp
  )

  target_include_directories(benchmark_operations PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${EIGEN3_INCLUDE_DIRS}
    ${LIBLAS_INCLUDE_DIR}
    ${LAZPERF_INCLUDE_DIR}
    ${PCL_INCLUDE_DIRS}
  )

  target_link_libraries(benchmark_operations
    Eigen3::Eigen
    ${LIBLAS_LIBRARY}
    ${LAZPERF_LIBRARY}
    ${PCL_LIBRARIES}
    Boost::system
    Boost::iostreams
    Boost::program_options
    Boost::thread
    pthread
  )

  add_dependencies(benchmark_operations lazperf_external liblas_external)

  ament_target_dependencies(benchmark_operations
    rclcpp
    sensor_msgs
    geometry_msgs
    nav_msgs
    pcl_conversions
  )
endif()

ament_package()
