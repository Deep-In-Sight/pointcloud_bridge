#include <gtest/gtest.h>
#include <fstream>
#include <vector>
#include <filesystem>
#include <chrono>
#include <cmath>
#include <limits>

#include <pointcloud_bridge/writers_optimized.hpp>

namespace fs = std::filesystem;

class WriterTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = "/tmp/pointcloud_writer_tests";
    fs::create_directories(test_dir_);

    // Create test points (contiguous XYZ layout)
    test_points_ = {
      1.0f, 2.0f, 3.0f,
      4.0f, 5.0f, 6.0f,
      7.0f, 8.0f, 9.0f
    };
  }

  void TearDown() override {
    fs::remove_all(test_dir_);
  }

  std::string test_dir_;
  std::vector<float> test_points_;
};

// Test PLY Writer
TEST_F(WriterTest, PLYWriterBasic) {
  std::string filename = test_dir_ + "/test.ply";

  BufferedPLYWriter writer;
  ASSERT_TRUE(writer.open(filename));
  ASSERT_TRUE(writer.write_points(test_points_.data(), 3));
  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, PLYWriterPointCount) {
  std::string filename = test_dir_ + "/test_count.ply";

  BufferedPLYWriter writer;
  writer.open(filename);
  writer.write_points(test_points_.data(), 3);
  writer.close();

  std::ifstream file(filename);
  std::string line;
  bool found_count = false;
  while (std::getline(file, line)) {
    if (line.find("element vertex") == 0) {
      std::string count_str = line.substr(15);
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, 3);
      found_count = true;
      break;
    }
  }
  ASSERT_TRUE(found_count);
}

// Test PCD Writer
TEST_F(WriterTest, PCDWriterBasic) {
  std::string filename = test_dir_ + "/test.pcd";

  BufferedPCDWriter writer;
  ASSERT_TRUE(writer.open(filename));
  ASSERT_TRUE(writer.write_points(test_points_.data(), 3));
  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, PCDWriterPointCount) {
  std::string filename = test_dir_ + "/test_count.pcd";

  BufferedPCDWriter writer;
  writer.open(filename);
  writer.write_points(test_points_.data(), 3);
  writer.close();

  std::ifstream file(filename);
  std::string line;
  bool found_width = false;
  bool found_points = false;

  while (std::getline(file, line)) {
    if (line.find("WIDTH") == 0) {
      std::string count_str = line.substr(6);
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, 3);
      found_width = true;
    }
    if (line.find("POINTS") == 0) {
      std::string count_str = line.substr(7);
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, 3);
      found_points = true;
    }
  }
  ASSERT_TRUE(found_width && found_points);
}

// Test LAS Writer
TEST_F(WriterTest, LASWriterBasic) {
  std::string filename = test_dir_ + "/test.las";

  BufferedLASWriter writer;
  ASSERT_TRUE(writer.open(filename));
  ASSERT_TRUE(writer.write_points(test_points_.data(), 3));
  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));
  ASSERT_GT(fs::file_size(filename), 227);
}

TEST_F(WriterTest, LASWriterHeader) {
  std::string filename = test_dir_ + "/test_header.las";

  BufferedLASWriter writer;
  writer.open(filename);
  writer.write_points(test_points_.data(), 3);
  writer.close();

  std::ifstream file(filename, std::ios::binary);
  char signature[4];
  file.read(signature, 4);
  EXPECT_EQ(std::string(signature, 4), "LASF");
}

// Test LAZ Writer
TEST_F(WriterTest, LAZWriterBasic) {
  std::string filename = test_dir_ + "/test.laz";

  BufferedLAZWriter writer;
  ASSERT_TRUE(writer.open(filename));
  ASSERT_TRUE(writer.write_points(test_points_.data(), 3));
  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, LAZWriterHeader) {
  std::string filename = test_dir_ + "/test_header.laz";

  BufferedLAZWriter writer;
  writer.open(filename);
  writer.write_points(test_points_.data(), 3);
  writer.close();

  std::ifstream file(filename, std::ios::binary);
  char signature[4];
  file.read(signature, 4);
  EXPECT_EQ(std::string(signature, 4), "LASF");
}

// ============================================================
// NaN Handling Tests (AsyncPCLWriter is responsible for filtering)
// ============================================================

// Helper to count NaN values in binary PLY data
int count_nan_in_ply(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);
  std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
  file.close();

  size_t header_end = content.find("end_header\n");
  if (header_end == std::string::npos) return -1;
  size_t data_start = header_end + 11;

  size_t num_floats = (content.size() - data_start) / sizeof(float);
  const float* float_data = reinterpret_cast<const float*>(content.data() + data_start);

  int nan_count = 0;
  for (size_t i = 0; i < num_floats; ++i) {
    if (std::isnan(float_data[i])) {
      nan_count++;
    }
  }
  return nan_count;
}

// Helper to count infinity values in binary PLY data
int count_inf_in_ply(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);
  std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
  file.close();

  size_t header_end = content.find("end_header\n");
  if (header_end == std::string::npos) return -1;
  size_t data_start = header_end + 11;

  size_t num_floats = (content.size() - data_start) / sizeof(float);
  const float* float_data = reinterpret_cast<const float*>(content.data() + data_start);

  int inf_count = 0;
  for (size_t i = 0; i < num_floats; ++i) {
    if (std::isinf(float_data[i])) {
      inf_count++;
    }
  }
  return inf_count;
}

// Helper to get point count from PLY header
int get_ply_point_count(const std::string& filename) {
  std::ifstream file(filename);
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("element vertex") == 0) {
      std::string count_str = line.substr(15);
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      return std::stoi(count_str);
    }
  }
  return -1;
}

// Test AsyncPCLWriter filters NaN points - PLY format
TEST_F(WriterTest, AsyncPCLWriterNaNHandling_PLY) {
  std::string filename = test_dir_ + "/test_nan_async.ply";

  auto base_writer = std::make_unique<BufferedPLYWriter>();
  AsyncPCLWriter writer(std::move(base_writer));
  ASSERT_TRUE(writer.open(filename));

  // Create PCL cloud with mix of valid and NaN points
  auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
  cloud->push_back(pcl::PointXYZ(1.0f, 2.0f, 3.0f));                        // valid
  cloud->push_back(pcl::PointXYZ(std::nanf(""), 2.0f, 3.0f));               // NaN x
  cloud->push_back(pcl::PointXYZ(1.0f, std::nanf(""), 3.0f));               // NaN y
  cloud->push_back(pcl::PointXYZ(1.0f, 2.0f, std::nanf("")));               // NaN z
  cloud->push_back(pcl::PointXYZ(std::nanf(""), std::nanf(""), std::nanf(""))); // all NaN
  cloud->push_back(pcl::PointXYZ(4.0f, 5.0f, 6.0f));                        // valid

  writer.add_cloud(cloud);
  writer.close();

  int nan_count = count_nan_in_ply(filename);
  int point_count = get_ply_point_count(filename);

  std::cout << "AsyncPCLWriter PLY: " << point_count << " points written, "
            << nan_count << " NaN values\n";

  EXPECT_EQ(point_count, 2) << "Expected 2 valid points, got " << point_count;
  EXPECT_EQ(nan_count, 0) << "NaN values found in output!";
}

// Test AsyncPCLWriter filters NaN points - PCD format
TEST_F(WriterTest, AsyncPCLWriterNaNHandling_PCD) {
  std::string filename = test_dir_ + "/test_nan_async.pcd";

  auto base_writer = std::make_unique<BufferedPCDWriter>();
  AsyncPCLWriter writer(std::move(base_writer));
  ASSERT_TRUE(writer.open(filename));

  auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
  cloud->push_back(pcl::PointXYZ(1.0f, 2.0f, 3.0f));
  cloud->push_back(pcl::PointXYZ(std::nanf(""), 2.0f, 3.0f));
  cloud->push_back(pcl::PointXYZ(4.0f, 5.0f, 6.0f));

  writer.add_cloud(cloud);
  writer.close();

  // Read back and check
  std::ifstream file(filename, std::ios::binary);
  std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
  file.close();

  size_t data_start = content.find("DATA binary\n");
  ASSERT_NE(data_start, std::string::npos);
  data_start += 12;

  size_t num_floats = (content.size() - data_start) / sizeof(float);
  const float* float_data = reinterpret_cast<const float*>(content.data() + data_start);

  int nan_count = 0;
  for (size_t i = 0; i < num_floats; ++i) {
    if (std::isnan(float_data[i])) nan_count++;
  }

  std::cout << "AsyncPCLWriter PCD: " << (num_floats / 3) << " points, "
            << nan_count << " NaN values\n";

  EXPECT_EQ(num_floats / 3, 2) << "Expected 2 valid points";
  EXPECT_EQ(nan_count, 0) << "NaN values found in output!";
}

// Test AsyncPCLWriter with mixed batch (10% NaN like real lidar)
TEST_F(WriterTest, AsyncPCLWriterMixedBatch) {
  std::string filename = test_dir_ + "/test_mixed_async.ply";

  auto base_writer = std::make_unique<BufferedPLYWriter>();
  AsyncPCLWriter writer(std::move(base_writer));
  ASSERT_TRUE(writer.open(filename));

  int valid_count = 0;
  int nan_count = 0;

  auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
  cloud->reserve(1000);

  for (int i = 0; i < 1000; ++i) {
    if (i % 10 == 0) {
      cloud->push_back(pcl::PointXYZ(std::nanf(""), std::nanf(""), std::nanf("")));
      nan_count++;
    } else {
      cloud->push_back(pcl::PointXYZ(i * 0.1f, i * 0.2f, i * 0.3f));
      valid_count++;
    }
  }

  writer.add_cloud(cloud);
  writer.close();

  int written_count = get_ply_point_count(filename);
  int nan_in_file = count_nan_in_ply(filename);

  std::cout << "Mixed batch: " << valid_count << " valid, " << nan_count << " NaN submitted\n";
  std::cout << "Actually written: " << written_count << " points, "
            << nan_in_file << " NaN in file\n";

  EXPECT_EQ(written_count, valid_count)
    << "Expected " << valid_count << " points, got " << written_count;
  EXPECT_EQ(nan_in_file, 0) << "NaN values found in output!";
}

// Test AsyncPCLWriter with all NaN points
TEST_F(WriterTest, AsyncPCLWriterAllNaN) {
  std::string filename = test_dir_ + "/test_all_nan_async.ply";

  auto base_writer = std::make_unique<BufferedPLYWriter>();
  AsyncPCLWriter writer(std::move(base_writer));
  ASSERT_TRUE(writer.open(filename));

  auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
  for (int i = 0; i < 100; ++i) {
    cloud->push_back(pcl::PointXYZ(std::nanf(""), std::nanf(""), std::nanf("")));
  }

  writer.add_cloud(cloud);
  writer.close();

  int written_count = get_ply_point_count(filename);
  std::cout << "All-NaN batch: wrote " << written_count << " points (expected 0)\n";

  EXPECT_EQ(written_count, 0) << "NaN points were not filtered!";
}

// Test AsyncPCLWriter with transformation and NaN
TEST_F(WriterTest, AsyncPCLWriterNaNWithTransform) {
  std::string filename = test_dir_ + "/test_nan_transform.ply";

  auto base_writer = std::make_unique<BufferedPLYWriter>();
  AsyncPCLWriter writer(std::move(base_writer));
  ASSERT_TRUE(writer.open(filename));

  auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
  cloud->push_back(pcl::PointXYZ(1.0f, 0.0f, 0.0f));  // valid
  cloud->push_back(pcl::PointXYZ(std::nanf(""), 0.0f, 0.0f));  // NaN
  cloud->push_back(pcl::PointXYZ(0.0f, 1.0f, 0.0f));  // valid

  // Set a transformation (translate by 10, 20, 30)
  cloud->sensor_origin_ = Eigen::Vector4f(10.0f, 20.0f, 30.0f, 0.0f);
  cloud->sensor_orientation_ = Eigen::Quaternionf::Identity();

  writer.add_cloud(cloud);
  writer.close();

  int written_count = get_ply_point_count(filename);
  int nan_in_file = count_nan_in_ply(filename);

  std::cout << "NaN with transform: " << written_count << " points, "
            << nan_in_file << " NaN\n";

  EXPECT_EQ(written_count, 2) << "Expected 2 valid points after transform";
  EXPECT_EQ(nan_in_file, 0) << "NaN values found in output!";
}

// Test multiple points
TEST_F(WriterTest, LargePointCloud) {
  std::string filename = test_dir_ + "/test_large.ply";

  BufferedPLYWriter writer;
  writer.open(filename);

  // Write 1000 points
  std::vector<float> points;
  points.reserve(1000 * 3);
  for (int i = 0; i < 1000; ++i) {
    points.push_back(i * 0.1f);
    points.push_back(i * 0.2f);
    points.push_back(i * 0.3f);
  }

  writer.write_points(points.data(), 1000);
  writer.close();

  int count = get_ply_point_count(filename);
  EXPECT_EQ(count, 1000);
}

// Helper function to get current RSS memory in MB
double get_memory_usage_mb() {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.find("VmRSS:") == 0) {
      std::string mem_str = line.substr(6);
      mem_str.erase(0, mem_str.find_first_not_of(" \t"));
      mem_str.erase(mem_str.find_last_not_of(" \tkB\r\n") + 1);
      int64_t memory_kb = std::stoll(mem_str);
      return memory_kb / 1024.0;
    }
  }
  return 0.0;
}

// Performance test for streaming writes
TEST_F(WriterTest, StreamingPerformance) {
  const int num_points = 100000;

  std::cout << "\n=== Streaming Performance Test ===\n";
  std::cout << "Writing " << num_points << " points per format\n";
  std::cout << "Expected data size: " << (num_points * 12.0 / (1024.0 * 1024.0))
            << " MB (raw point data)\n\n";

  // Generate test data
  std::vector<float> points;
  points.reserve(num_points * 3);
  for (int i = 0; i < num_points; ++i) {
    points.push_back(i * 0.001f);
    points.push_back(i * 0.002f);
    points.push_back(i * 0.003f);
  }

  // Test PLY writer
  {
    std::string filename = test_dir_ + "/perf_test.ply";
    double mem_before = get_memory_usage_mb();

    BufferedPLYWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(points.data(), num_points);
    auto end = std::chrono::high_resolution_clock::now();

    writer.close();

    double mem_after = get_memory_usage_mb();
    double duration_sec = std::chrono::duration<double>(end - start).count();
    double throughput_points_per_sec = num_points / duration_sec;
    double throughput_mb_per_sec = (num_points * 12.0 / (1024.0 * 1024.0)) / duration_sec;
    double mem_increase = mem_after - mem_before;

    std::cout << "PLY Writer Performance:\n";
    std::cout << "  Duration: " << duration_sec << " seconds\n";
    std::cout << "  Throughput: " << throughput_points_per_sec << " points/sec\n";
    std::cout << "  Throughput: " << throughput_mb_per_sec << " MB/sec\n";
    std::cout << "  Memory increase: " << mem_increase << " MB\n";
    std::cout << "  File size: " << (fs::file_size(filename) / (1024.0 * 1024.0)) << " MB\n\n";

    EXPECT_GT(throughput_points_per_sec, 10000.0);
    EXPECT_LT(mem_increase, 50.0);
  }

  // Test PCD writer
  {
    std::string filename = test_dir_ + "/perf_test.pcd";
    double mem_before = get_memory_usage_mb();

    BufferedPCDWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(points.data(), num_points);
    auto end = std::chrono::high_resolution_clock::now();

    writer.close();

    double mem_after = get_memory_usage_mb();
    double duration_sec = std::chrono::duration<double>(end - start).count();
    double throughput_points_per_sec = num_points / duration_sec;
    double throughput_mb_per_sec = (num_points * 12.0 / (1024.0 * 1024.0)) / duration_sec;
    double mem_increase = mem_after - mem_before;

    std::cout << "PCD Writer Performance:\n";
    std::cout << "  Duration: " << duration_sec << " seconds\n";
    std::cout << "  Throughput: " << throughput_points_per_sec << " points/sec\n";
    std::cout << "  Throughput: " << throughput_mb_per_sec << " MB/sec\n";
    std::cout << "  Memory increase: " << mem_increase << " MB\n";
    std::cout << "  File size: " << (fs::file_size(filename) / (1024.0 * 1024.0)) << " MB\n\n";

    EXPECT_GT(throughput_points_per_sec, 10000.0);
    EXPECT_LT(mem_increase, 50.0);
  }

  // Test LAS writer
  {
    std::string filename = test_dir_ + "/perf_test.las";
    double mem_before = get_memory_usage_mb();

    BufferedLASWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(points.data(), num_points);
    auto end = std::chrono::high_resolution_clock::now();

    writer.close();

    double mem_after = get_memory_usage_mb();
    double duration_sec = std::chrono::duration<double>(end - start).count();
    double throughput_points_per_sec = num_points / duration_sec;
    double throughput_mb_per_sec = (num_points * 12.0 / (1024.0 * 1024.0)) / duration_sec;
    double mem_increase = mem_after - mem_before;

    std::cout << "LAS Writer Performance:\n";
    std::cout << "  Duration: " << duration_sec << " seconds\n";
    std::cout << "  Throughput: " << throughput_points_per_sec << " points/sec\n";
    std::cout << "  Throughput: " << throughput_mb_per_sec << " MB/sec\n";
    std::cout << "  Memory increase: " << mem_increase << " MB\n";
    std::cout << "  File size: " << (fs::file_size(filename) / (1024.0 * 1024.0)) << " MB\n\n";

    EXPECT_GT(throughput_points_per_sec, 10000.0);
    EXPECT_LT(mem_increase, 50.0);
  }

  // Test LAZ writer (compressed, may be slower)
  {
    std::string filename = test_dir_ + "/perf_test.laz";
    double mem_before = get_memory_usage_mb();

    BufferedLAZWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(points.data(), num_points);
    auto end = std::chrono::high_resolution_clock::now();

    writer.close();

    double mem_after = get_memory_usage_mb();
    double duration_sec = std::chrono::duration<double>(end - start).count();
    double throughput_points_per_sec = num_points / duration_sec;
    double throughput_mb_per_sec = (num_points * 12.0 / (1024.0 * 1024.0)) / duration_sec;
    double mem_increase = mem_after - mem_before;

    std::cout << "LAZ Writer Performance:\n";
    std::cout << "  Duration: " << duration_sec << " seconds\n";
    std::cout << "  Throughput: " << throughput_points_per_sec << " points/sec\n";
    std::cout << "  Throughput: " << throughput_mb_per_sec << " MB/sec\n";
    std::cout << "  Memory increase: " << mem_increase << " MB\n";
    std::cout << "  File size: " << (fs::file_size(filename) / (1024.0 * 1024.0)) << " MB\n";
    std::cout << "  Compression ratio: " << ((num_points * 12.0) / fs::file_size(filename))
              << "x\n\n";

    // LAZ may be slower due to compression
    EXPECT_GT(throughput_points_per_sec, 5000.0);
    EXPECT_LT(mem_increase, 50.0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
