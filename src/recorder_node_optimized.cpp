#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>

#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl_conversions/pcl_conversions.h>

#include <atomic>

#include <pointcloud_bridge/writers_optimized.hpp>

// Optimized recorder node:
// - Fast callback: PCL conversion + queue, return immediately
// - Background thread: filter + write
// - Pointcloud is expected in world frame (no transformation)
class RecorderNodeOptimized : public rclcpp::Node {
public:
  RecorderNodeOptimized() : Node("pointcloud_recorder_optimized") {
    // Declare parameters
    this->declare_parameter("pointcloud_topic", "/cloud_registered");
    this->declare_parameter("artifact_dir", "/tmp");
    this->declare_parameter("file_format", "ply");

    std::string pointcloud_topic = this->get_parameter("pointcloud_topic").as_string();
    artifact_dir_ = this->get_parameter("artifact_dir").as_string();
    file_format_ = this->get_parameter("file_format").as_string();

    RCLCPP_INFO(this->get_logger(), "Optimized pointcloud recorder starting");
    RCLCPP_INFO(this->get_logger(), "Artifact dir: %s", artifact_dir_.c_str());
    RCLCPP_INFO(this->get_logger(), "File format: %s", file_format_.c_str());

    // Create writer
    std::unique_ptr<BufferedStreamingWriter> base_writer;
    if (file_format_ == "ply") {
      base_writer = std::make_unique<BufferedPLYWriter>();
    } else if (file_format_ == "pcd") {
      base_writer = std::make_unique<BufferedPCDWriter>();
    } else if (file_format_ == "las") {
      base_writer = std::make_unique<BufferedLASWriter>();
    } else if (file_format_ == "laz") {
      base_writer = std::make_unique<BufferedLAZWriter>();
    } else {
      RCLCPP_ERROR(this->get_logger(), "Unsupported file format: %s", file_format_.c_str());
      rclcpp::shutdown();
      return;
    }

    writer_ = std::make_unique<AsyncPCLWriter>(std::move(base_writer));
    output_file_ = artifact_dir_ + "/map_result." + file_format_;

    if (!writer_->open(output_file_)) {
      RCLCPP_ERROR(this->get_logger(), "Failed to open file: %s", output_file_.c_str());
      rclcpp::shutdown();
      return;
    }

    RCLCPP_INFO(this->get_logger(), "Streaming to: %s", output_file_.c_str());

    // Subscribe to pointcloud (expected in world frame)
    pointcloud_sub_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
      pointcloud_topic, 10,
      std::bind(&RecorderNodeOptimized::pointcloud_callback, this, std::placeholders::_1)
    );
    RCLCPP_INFO(this->get_logger(), "Subscribing to pointcloud: %s (world frame)", pointcloud_topic.c_str());

    // Stats timer
    stats_timer_ = this->create_wall_timer(
      std::chrono::seconds(2),
      std::bind(&RecorderNodeOptimized::report_stats, this)
    );
  }

  ~RecorderNodeOptimized() {
    RCLCPP_INFO(this->get_logger(), "Shutting down, finalizing file...");
    if (writer_) {
      writer_->close();
      RCLCPP_INFO(this->get_logger(), "Finalized %zu points to: %s",
                  writer_->get_total_points(), output_file_.c_str());
    }
  }

private:
  void pointcloud_callback(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Fast PCL conversion
    auto cloud = std::make_shared<pcl::PointCloud<pcl::PointXYZ>>();
    pcl::fromROSMsg(*msg, *cloud);

    // Queue and return fast
    writer_->add_cloud(cloud);

    // Stats
    total_points_.fetch_add(cloud->size(), std::memory_order_relaxed);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - start_time).count();
    callback_time_us_.fetch_add(duration_us, std::memory_order_relaxed);
    callback_count_.fetch_add(1, std::memory_order_relaxed);
  }

  void report_stats() {
    size_t total = total_points_.load(std::memory_order_relaxed);
    size_t callbacks = callback_count_.load(std::memory_order_relaxed);
    size_t time_us = callback_time_us_.load(std::memory_order_relaxed);

    if (callbacks > 0) {
      double avg_callback_ms = (time_us / callbacks) / 1000.0;
      size_t queue_size = writer_->get_queue_size();
      size_t written = writer_->get_total_points();

      // Get writer thread stats
      const auto& ws = writer_->get_stats();
      size_t clouds = ws.clouds_processed.load();
      double avg_write_ms = clouds > 0 ? (ws.write_time_us.load() / clouds) / 1000.0 : 0;

      RCLCPP_INFO(this->get_logger(),
        "CB: %.1fms | Queue: %zu | Writer: write=%.1fms | Points: %zu/%zu",
        avg_callback_ms, queue_size, avg_write_ms, written, total);
    }
  }

  // ROS subscribers
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr pointcloud_sub_;
  rclcpp::TimerBase::SharedPtr stats_timer_;

  // Output
  std::string artifact_dir_;
  std::string file_format_;
  std::string output_file_;
  std::unique_ptr<AsyncPCLWriter> writer_;

  // Stats
  std::atomic<size_t> total_points_{0};
  std::atomic<size_t> callback_count_{0};
  std::atomic<size_t> callback_time_us_{0};
};

int main(int argc, char** argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<RecorderNodeOptimized>();
  rclcpp::spin(node);
  if (rclcpp::ok()) {
    rclcpp::shutdown();
  }
  return 0;
}
