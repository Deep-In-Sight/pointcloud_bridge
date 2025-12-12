#include <gtest/gtest.h>
#include <fstream>
#include <vector>
#include <filesystem>
#include <chrono>
#include <Eigen/Dense>

#include <pointcloud_bridge/writers.hpp>

namespace fs = std::filesystem;

class WriterTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = "/tmp/pointcloud_writer_tests";
    fs::create_directories(test_dir_);

    // Create test points
    test_points_ = {
      Eigen::Vector3f(1.0f, 2.0f, 3.0f),
      Eigen::Vector3f(4.0f, 5.0f, 6.0f),
      Eigen::Vector3f(7.0f, 8.0f, 9.0f)
    };
  }

  void TearDown() override {
    // Clean up test directory
    fs::remove_all(test_dir_);
  }

  std::string test_dir_;
  std::vector<Eigen::Vector3f> test_points_;
};

// Test PLY Writer
TEST_F(WriterTest, PLYWriterBasic) {
  std::string filename = test_dir_ + "/test.ply";

  StreamingPLYWriter writer;
  ASSERT_TRUE(writer.open(filename));

  for (const auto& point : test_points_) {
    ASSERT_TRUE(writer.write_point(point));
  }

  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));

  // Verify file is not empty
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, PLYWriterPointCount) {
  std::string filename = test_dir_ + "/test_count.ply";

  StreamingPLYWriter writer;
  writer.open(filename);

  for (const auto& point : test_points_) {
    writer.write_point(point);
  }

  writer.close();

  // Read file and verify point count in header
  std::ifstream file(filename);
  std::string line;
  bool found_count = false;
  while (std::getline(file, line)) {
    if (line.find("element vertex") == 0) {  // Line starts with "element vertex"
      // Extract number from line (after "element vertex ")
      std::string count_str = line.substr(15);  // Skip "element vertex "
      // Trim whitespace
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, test_points_.size());
      found_count = true;
      break;
    }
  }
  ASSERT_TRUE(found_count);
}

// Test PCD Writer
TEST_F(WriterTest, PCDWriterBasic) {
  std::string filename = test_dir_ + "/test.pcd";

  StreamingPCDWriter writer;
  ASSERT_TRUE(writer.open(filename));

  for (const auto& point : test_points_) {
    ASSERT_TRUE(writer.write_point(point));
  }

  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));

  // Verify file is not empty
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, PCDWriterPointCount) {
  std::string filename = test_dir_ + "/test_count.pcd";

  StreamingPCDWriter writer;
  writer.open(filename);

  for (const auto& point : test_points_) {
    writer.write_point(point);
  }

  writer.close();

  // Read file and verify point count in header
  std::ifstream file(filename);
  std::string line;
  bool found_width = false;
  bool found_points = false;

  while (std::getline(file, line)) {
    if (line.find("WIDTH") == 0) {  // Line starts with WIDTH
      std::string count_str = line.substr(6);  // Skip "WIDTH "
      // Trim whitespace
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, test_points_.size());
      found_width = true;
    }
    if (line.find("POINTS") == 0) {  // Line starts with POINTS
      std::string count_str = line.substr(7);  // Skip "POINTS "
      // Trim whitespace
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, test_points_.size());
      found_points = true;
    }
  }
  ASSERT_TRUE(found_width && found_points);
}

// Test LAS Writer
TEST_F(WriterTest, LASWriterBasic) {
  std::string filename = test_dir_ + "/test.las";

  StreamingLASWriter writer;
  ASSERT_TRUE(writer.open(filename));

  for (const auto& point : test_points_) {
    ASSERT_TRUE(writer.write_point(point));
  }

  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));

  // Verify file is not empty and has LAS header
  ASSERT_GT(fs::file_size(filename), 227);  // Minimum LAS header size
}

TEST_F(WriterTest, LASWriterHeader) {
  std::string filename = test_dir_ + "/test_header.las";

  StreamingLASWriter writer;
  writer.open(filename);

  for (const auto& point : test_points_) {
    writer.write_point(point);
  }

  writer.close();

  // Read and verify LAS signature
  std::ifstream file(filename, std::ios::binary);
  char signature[4];
  file.read(signature, 4);
  EXPECT_EQ(std::string(signature, 4), "LASF");
}

// Test LAZ Writer
TEST_F(WriterTest, LAZWriterBasic) {
  std::string filename = test_dir_ + "/test.laz";

  StreamingLAZWriter writer;
  ASSERT_TRUE(writer.open(filename));

  for (const auto& point : test_points_) {
    ASSERT_TRUE(writer.write_point(point));
  }

  ASSERT_TRUE(writer.close());
  ASSERT_TRUE(fs::exists(filename));

  // Verify file is not empty
  ASSERT_GT(fs::file_size(filename), 0);
}

TEST_F(WriterTest, LAZWriterHeader) {
  std::string filename = test_dir_ + "/test_header.laz";

  StreamingLAZWriter writer;
  writer.open(filename);

  for (const auto& point : test_points_) {
    writer.write_point(point);
  }

  writer.close();

  // Read and verify LAS signature (LAZ uses LAS format)
  std::ifstream file(filename, std::ios::binary);
  char signature[4];
  file.read(signature, 4);
  EXPECT_EQ(std::string(signature, 4), "LASF");
}

// Test multiple points
TEST_F(WriterTest, LargePointCloud) {
  std::string filename = test_dir_ + "/test_large.ply";

  StreamingPLYWriter writer;
  writer.open(filename);

  // Write 1000 points
  for (int i = 0; i < 1000; ++i) {
    Eigen::Vector3f point(i * 0.1f, i * 0.2f, i * 0.3f);
    writer.write_point(point);
  }

  writer.close();

  // Verify point count
  std::ifstream file(filename);
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("element vertex") == 0) {
      std::string count_str = line.substr(15);  // Skip "element vertex "
      // Trim whitespace
      count_str.erase(0, count_str.find_first_not_of(" \t"));
      count_str.erase(count_str.find_last_not_of(" \t\r\n") + 1);
      int count = std::stoi(count_str);
      EXPECT_EQ(count, 1000);
      break;
    }
  }
}

// Helper function to get current RSS memory in MB
double get_memory_usage_mb() {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.find("VmRSS:") == 0) {
      // Extract memory value in kB
      std::string mem_str = line.substr(6);  // Skip "VmRSS:"
      mem_str.erase(0, mem_str.find_first_not_of(" \t"));
      mem_str.erase(mem_str.find_last_not_of(" \tkB\r\n") + 1);
      long memory_kb = std::stol(mem_str);
      return memory_kb / 1024.0;  // Convert to MB
    }
  }
  return 0.0;
}

// Performance test for streaming writes
TEST_F(WriterTest, StreamingPerformance) {
  const int num_points = 100000;  // 100K points

  std::cout << "\n=== Streaming Performance Test ===\n";
  std::cout << "Writing " << num_points << " points per format\n";
  std::cout << "Expected data size: " << (num_points * 12.0 / (1024.0 * 1024.0)) << " MB (raw point data)\n\n";

  // Test PLY writer
  {
    std::string filename = test_dir_ + "/perf_test.ply";
    double mem_before = get_memory_usage_mb();

    StreamingPLYWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_points; ++i) {
      Eigen::Vector3f point(i * 0.001f, i * 0.002f, i * 0.003f);
      writer.write_point(point);
    }

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

    // Verify reasonable performance (at least 10K points/sec)
    EXPECT_GT(throughput_points_per_sec, 10000.0);
    // Verify memory usage is reasonable (less than 50MB increase for streaming)
    EXPECT_LT(mem_increase, 50.0);
  }

  // Test PCD writer
  {
    std::string filename = test_dir_ + "/perf_test.pcd";
    double mem_before = get_memory_usage_mb();

    StreamingPCDWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_points; ++i) {
      Eigen::Vector3f point(i * 0.001f, i * 0.002f, i * 0.003f);
      writer.write_point(point);
    }

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

    StreamingLASWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_points; ++i) {
      Eigen::Vector3f point(i * 0.001f, i * 0.002f, i * 0.003f);
      writer.write_point(point);
    }

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

    StreamingLAZWriter writer;
    writer.open(filename);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_points; ++i) {
      Eigen::Vector3f point(i * 0.001f, i * 0.002f, i * 0.003f);
      writer.write_point(point);
    }

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
    std::cout << "  Compression ratio: " << ((num_points * 12.0) / fs::file_size(filename)) << "x\n\n";

    // LAZ may be slower due to compression, so lower threshold
    EXPECT_GT(throughput_points_per_sec, 5000.0);
    EXPECT_LT(mem_increase, 50.0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
