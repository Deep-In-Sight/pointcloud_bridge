#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <vector>
#include <numeric>
#include <cmath>

#include <sensor_msgs/msg/point_cloud2.hpp>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl/common/transforms.h>
#include <pcl_conversions/pcl_conversions.h>

#include <pointcloud_bridge/writers_optimized.hpp>

// Benchmark configuration
constexpr size_t NUM_POINTS = 100000;
constexpr size_t NUM_ITERATIONS = 100;
constexpr size_t WARMUP_ITERATIONS = 5;

// Stats helper
struct BenchmarkStats {
  std::vector<double> times_us;

  void add(double time_us) { times_us.push_back(time_us); }

  double mean() const {
    return std::accumulate(times_us.begin(), times_us.end(), 0.0) / times_us.size();
  }

  double stddev() const {
    double m = mean();
    double sum = 0;
    for (double t : times_us) sum += (t - m) * (t - m);
    return std::sqrt(sum / times_us.size());
  }

  double min() const { return *std::min_element(times_us.begin(), times_us.end()); }
  double max() const { return *std::max_element(times_us.begin(), times_us.end()); }

  void print(const std::string& name) const {
    printf("%-30s: mean=%7.1f us  std=%6.1f us  min=%7.1f us  max=%7.1f us\n",
           name.c_str(), mean(), stddev(), min(), max());
  }
};

class BenchmarkTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dist(-100.0f, 100.0f);

    // Pre-generate NUM_ITERATIONS different ROS messages and PCL clouds
    ros_msgs_.resize(NUM_ITERATIONS);
    pcl_clouds_.resize(NUM_ITERATIONS);

    for (size_t n = 0; n < NUM_ITERATIONS; ++n) {
      pcl_clouds_[n] = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
      pcl_clouds_[n]->resize(NUM_POINTS);
      for (size_t i = 0; i < NUM_POINTS; ++i) {
        pcl_clouds_[n]->points[i].x = dist(gen);
        pcl_clouds_[n]->points[i].y = dist(gen);
        pcl_clouds_[n]->points[i].z = dist(gen);
      }
      pcl::toROSMsg(*pcl_clouds_[n], ros_msgs_[n]);
    }

    // Keep single references for tests that need them
    pcl_cloud_ = pcl_clouds_[0];
    ros_msg_ = ros_msgs_[0];

    // Create transform
    transform_ = Eigen::Translation3f(1.0f, 2.0f, 3.0f) *
                 Eigen::AngleAxisf(0.5f, Eigen::Vector3f::UnitZ());

    // Pre-allocate contiguous buffer
    contiguous_buffer_.resize(NUM_POINTS * 3);
  }

  std::vector<sensor_msgs::msg::PointCloud2> ros_msgs_;
  std::vector<pcl::PointCloud<pcl::PointXYZ>::Ptr> pcl_clouds_;
  pcl::PointCloud<pcl::PointXYZ>::Ptr pcl_cloud_;
  sensor_msgs::msg::PointCloud2 ros_msg_;
  Eigen::Affine3f transform_;
  std::vector<float> contiguous_buffer_;
};

// Benchmark: pcl::fromROSMsg
TEST_F(BenchmarkTest, FromROSMsg) {
  BenchmarkStats stats;
  pcl::PointCloud<pcl::PointXYZ> cloud;

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    pcl::fromROSMsg(ros_msg_, cloud);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    pcl::fromROSMsg(ros_msg_, cloud);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("pcl::fromROSMsg");
  EXPECT_GT(cloud.size(), 0u);
}

// Benchmark: pcl::transformPointCloud
TEST_F(BenchmarkTest, TransformPointCloud) {
  BenchmarkStats stats;
  pcl::PointCloud<pcl::PointXYZ> transformed;
  transformed.reserve(NUM_POINTS);

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    pcl::transformPointCloud(*pcl_cloud_, transformed, transform_);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    pcl::transformPointCloud(*pcl_cloud_, transformed, transform_);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("pcl::transformPointCloud");
  EXPECT_EQ(transformed.size(), NUM_POINTS);
}

// Benchmark: make_contiguous (strided -> packed)
TEST_F(BenchmarkTest, MakeContiguous) {
  BenchmarkStats stats;
  const float* point_data = reinterpret_cast<const float*>(pcl_cloud_->points.data());

  auto make_contiguous = [this](const float* data, size_t num_points, size_t stride_floats) {
    for (size_t i = 0; i < num_points; ++i) {
      contiguous_buffer_[i * 3 + 0] = data[i * stride_floats + 0];
      contiguous_buffer_[i * 3 + 1] = data[i * stride_floats + 1];
      contiguous_buffer_[i * 3 + 2] = data[i * stride_floats + 2];
    }
  };

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    make_contiguous(point_data, NUM_POINTS, 4);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    make_contiguous(point_data, NUM_POINTS, 4);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("make_contiguous (stride=4)");
  EXPECT_FLOAT_EQ(contiguous_buffer_[0], pcl_cloud_->points[0].x);
}

// Benchmark: BufferedPLYWriter::write_points
TEST_F(BenchmarkTest, WritePLY) {
  BenchmarkStats stats;

  // Prepare contiguous data
  const float* point_data = reinterpret_cast<const float*>(pcl_cloud_->points.data());
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    contiguous_buffer_[i * 3 + 0] = point_data[i * 4 + 0];
    contiguous_buffer_[i * 3 + 1] = point_data[i * 4 + 1];
    contiguous_buffer_[i * 3 + 2] = point_data[i * 4 + 2];
  }

  // Open file once, write many times (simulates streaming)
  BufferedPLYWriter writer;
  ASSERT_TRUE(writer.open("/tmp/bench_test.ply"));

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  writer.close();
  stats.print("BufferedPLYWriter::write_points");

  // Cleanup
  std::remove("/tmp/bench_test.ply");
}

// Benchmark: BufferedPCDWriter::write_points
TEST_F(BenchmarkTest, WritePCD) {
  BenchmarkStats stats;

  // Prepare contiguous data
  const float* point_data = reinterpret_cast<const float*>(pcl_cloud_->points.data());
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    contiguous_buffer_[i * 3 + 0] = point_data[i * 4 + 0];
    contiguous_buffer_[i * 3 + 1] = point_data[i * 4 + 1];
    contiguous_buffer_[i * 3 + 2] = point_data[i * 4 + 2];
  }

  BufferedPCDWriter writer;
  ASSERT_TRUE(writer.open("/tmp/bench_test.pcd"));

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  writer.close();
  stats.print("BufferedPCDWriter::write_points");

  std::remove("/tmp/bench_test.pcd");
}

// Benchmark: BufferedLASWriter::write_points
TEST_F(BenchmarkTest, WriteLAS) {
  BenchmarkStats stats;

  // Prepare contiguous data
  const float* point_data = reinterpret_cast<const float*>(pcl_cloud_->points.data());
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    contiguous_buffer_[i * 3 + 0] = point_data[i * 4 + 0];
    contiguous_buffer_[i * 3 + 1] = point_data[i * 4 + 1];
    contiguous_buffer_[i * 3 + 2] = point_data[i * 4 + 2];
  }

  BufferedLASWriter writer;
  ASSERT_TRUE(writer.open("/tmp/bench_test.las"));

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  writer.close();
  stats.print("BufferedLASWriter::write_points");

  std::remove("/tmp/bench_test.las");
}

// Benchmark: BufferedLAZWriter::write_points
TEST_F(BenchmarkTest, WriteLAZ) {
  BenchmarkStats stats;

  // Prepare contiguous data
  const float* point_data = reinterpret_cast<const float*>(pcl_cloud_->points.data());
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    contiguous_buffer_[i * 3 + 0] = point_data[i * 4 + 0];
    contiguous_buffer_[i * 3 + 1] = point_data[i * 4 + 1];
    contiguous_buffer_[i * 3 + 2] = point_data[i * 4 + 2];
  }

  BufferedLAZWriter writer;
  ASSERT_TRUE(writer.open("/tmp/bench_test.laz"));

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
  }

  // Benchmark
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    writer.write_points(contiguous_buffer_.data(), NUM_POINTS);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  writer.close();
  stats.print("BufferedLAZWriter::write_points");

  std::remove("/tmp/bench_test.laz");
}

// Benchmark: fromROSMsg with allocation (realistic - different msg each iteration)
TEST_F(BenchmarkTest, FromROSMsgWithAlloc) {
  BenchmarkStats stats;

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
    pcl::fromROSMsg(ros_msgs_[i], *cloud);
  }

  // Benchmark - allocate new cloud each time, different msg each iteration
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();

    auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
    pcl::fromROSMsg(ros_msgs_[i], *cloud);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("fromROSMsg (with alloc)");
}

// Benchmark: transformPointCloud with fresh output (realistic - different cloud each iteration)
TEST_F(BenchmarkTest, TransformWithAlloc) {
  BenchmarkStats stats;

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    pcl::PointCloud<pcl::PointXYZ> transformed;
    pcl::transformPointCloud(*pcl_clouds_[i], transformed, transform_);
  }

  // Benchmark - fresh output cloud each time, different input each iteration
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    auto start = std::chrono::high_resolution_clock::now();

    pcl::PointCloud<pcl::PointXYZ> transformed;
    pcl::transformPointCloud(*pcl_clouds_[i], transformed, transform_);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("transformPointCloud (alloc)");
}

// Benchmark: make_contiguous with allocation (different cloud each iteration)
TEST_F(BenchmarkTest, MakeContiguousWithAlloc) {
  BenchmarkStats stats;

  // Warmup
  for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
    const float* point_data = reinterpret_cast<const float*>(pcl_clouds_[i]->points.data());
    std::vector<float> buffer(NUM_POINTS * 3);
    for (size_t j = 0; j < NUM_POINTS; ++j) {
      buffer[j * 3 + 0] = point_data[j * 4 + 0];
      buffer[j * 3 + 1] = point_data[j * 4 + 1];
      buffer[j * 3 + 2] = point_data[j * 4 + 2];
    }
  }

  // Benchmark - allocate buffer each time, different cloud each iteration
  for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
    const float* point_data = reinterpret_cast<const float*>(pcl_clouds_[i]->points.data());

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<float> buffer(NUM_POINTS * 3);
    for (size_t j = 0; j < NUM_POINTS; ++j) {
      buffer[j * 3 + 0] = point_data[j * 4 + 0];
      buffer[j * 3 + 1] = point_data[j * 4 + 1];
      buffer[j * 3 + 2] = point_data[j * 4 + 2];
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.add(duration.count());
  }

  stats.print("make_contiguous (alloc)");
}

int main(int argc, char** argv) {
  printf("\n");
  printf("====================================================\n");
  printf("  Pointcloud Operations Benchmark\n");
  printf("  Points: %zu  |  Iterations: %zu\n", NUM_POINTS, NUM_ITERATIONS);
  printf("====================================================\n\n");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
