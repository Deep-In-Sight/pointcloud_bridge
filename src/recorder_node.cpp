#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>

#include <mutex>
#include <vector>
#include <Eigen/Dense>
#include <Eigen/Geometry>

#include <pointcloud_bridge/writers.hpp>

// Main recorder node
class RecorderNode : public rclcpp::Node {
public:
  RecorderNode() : Node("pointcloud_recorder") {
    // Declare parameters
    this->declare_parameter("pointcloud_topic", "/lidar");
    this->declare_parameter("pose_topic", "");
    this->declare_parameter("pose_type", 1);  // 0 = Odometry, 1 = PoseStamped
    this->declare_parameter("artifact_dir", "/tmp");
    this->declare_parameter("file_format", "ply");  // ply, pcd, las, laz

    std::string pointcloud_topic = this->get_parameter("pointcloud_topic").as_string();
    std::string pose_topic = this->get_parameter("pose_topic").as_string();
    int pose_type = this->get_parameter("pose_type").as_int();
    artifact_dir_ = this->get_parameter("artifact_dir").as_string();
    file_format_ = this->get_parameter("file_format").as_string();

    RCLCPP_INFO(this->get_logger(), "Pointcloud recorder starting");
    RCLCPP_INFO(this->get_logger(), "Artifact dir: %s", artifact_dir_.c_str());
    RCLCPP_INFO(this->get_logger(), "File format: %s", file_format_.c_str());

    // Validate file format and create appropriate writer
    if (file_format_ == "ply") {
      writer_ = std::make_unique<StreamingPLYWriter>();
    } else if (file_format_ == "pcd") {
      writer_ = std::make_unique<StreamingPCDWriter>();
    } else if (file_format_ == "las") {
      writer_ = std::make_unique<StreamingLASWriter>();
    } else if (file_format_ == "laz") {
      writer_ = std::make_unique<StreamingLAZWriter>();
    } else {
      RCLCPP_ERROR(this->get_logger(), "Unsupported file format: %s (use ply, pcd, las, or laz)",
                   file_format_.c_str());
      rclcpp::shutdown();
      return;
    }

    output_file_ = artifact_dir_ + "/map_result." + file_format_;

    // Open the writer
    if (!writer_->open(output_file_)) {
      RCLCPP_ERROR(this->get_logger(), "Failed to open file for writing: %s", output_file_.c_str());
      rclcpp::shutdown();
      return;
    }

    RCLCPP_INFO(this->get_logger(), "Streaming to: %s", output_file_.c_str());

    // Create subscribers
    pointcloud_sub_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
      pointcloud_topic, 10,
      std::bind(&RecorderNode::pointcloud_callback, this, std::placeholders::_1)
    );
    RCLCPP_INFO(this->get_logger(), "Subscribing to pointcloud: %s", pointcloud_topic.c_str());

    // Create pose subscriber if pose_topic is not empty
    if (!pose_topic.empty()) {
      enable_transform_ = true;
      if (pose_type == 0) {
        RCLCPP_INFO(this->get_logger(), "Subscribing to pose (Odometry): %s", pose_topic.c_str());
        odom_sub_ = this->create_subscription<nav_msgs::msg::Odometry>(
          pose_topic, 10,
          std::bind(&RecorderNode::odom_callback, this, std::placeholders::_1)
        );
      } else if (pose_type == 1) {
        RCLCPP_INFO(this->get_logger(), "Subscribing to pose (PoseStamped): %s", pose_topic.c_str());
        pose_stamped_sub_ = this->create_subscription<geometry_msgs::msg::PoseStamped>(
          pose_topic, 10,
          std::bind(&RecorderNode::pose_stamped_callback, this, std::placeholders::_1)
        );
      } else {
        RCLCPP_ERROR(this->get_logger(), "Invalid pose_type: %d (must be 0 or 1)", pose_type);
      }
    } else {
      RCLCPP_INFO(this->get_logger(), "Pose topic not configured, recording in sensor frame");
      enable_transform_ = false;
    }
  }

  ~RecorderNode() {
    RCLCPP_INFO(this->get_logger(), "Shutting down recorder, finalizing file...");

    if (writer_) {
      writer_->close();
      RCLCPP_INFO(this->get_logger(), "Successfully finalized: %s", output_file_.c_str());
    }
  }

private:
  void pointcloud_callback(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(mutex_);

    uint32_t num_points = msg->width * msg->height;
    sensor_msgs::PointCloud2ConstIterator<float> iter_x(*msg, "x");
    sensor_msgs::PointCloud2ConstIterator<float> iter_y(*msg, "y");
    sensor_msgs::PointCloud2ConstIterator<float> iter_z(*msg, "z");

    size_t points_written = 0;
    for (uint32_t i = 0; i < num_points; ++i, ++iter_x, ++iter_y, ++iter_z) {
      // Skip invalid points (NaN)
      if (std::isnan(*iter_x) || std::isnan(*iter_y) || std::isnan(*iter_z)) {
        continue;
      }

      Eigen::Vector3f point(*iter_x, *iter_y, *iter_z);

      // Transform to world frame if pose is available
      if (enable_transform_ && has_pose_) {
        point = transform_ * point;
      }

      // Write point immediately (true streaming!)
      writer_->write_point(point);
      points_written++;
    }

    total_points_ += points_written;

    // Periodic progress report
    if (total_points_ / 10000 > last_report_count_) {
      last_report_count_ = total_points_ / 10000;
      RCLCPP_INFO(this->get_logger(), "Streamed %zu points so far", total_points_);
    }
  }

  void pose_stamped_callback(const geometry_msgs::msg::PoseStamped::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    update_transform(
      msg->pose.position.x, msg->pose.position.y, msg->pose.position.z,
      msg->pose.orientation.x, msg->pose.orientation.y,
      msg->pose.orientation.z, msg->pose.orientation.w
    );
  }

  void odom_callback(const nav_msgs::msg::Odometry::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    update_transform(
      msg->pose.pose.position.x, msg->pose.pose.position.y, msg->pose.pose.position.z,
      msg->pose.pose.orientation.x, msg->pose.pose.orientation.y,
      msg->pose.pose.orientation.z, msg->pose.pose.orientation.w
    );
  }

  void update_transform(float px, float py, float pz, float qx, float qy, float qz, float qw) {
    // Build transformation matrix from pose
    Eigen::Quaternionf q(qw, qx, qy, qz);
    Eigen::Translation3f t(px, py, pz);
    transform_ = t * q;
    has_pose_ = true;
  }

  // ROS subscribers
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr pointcloud_sub_;
  rclcpp::Subscription<geometry_msgs::msg::PoseStamped>::SharedPtr pose_stamped_sub_;
  rclcpp::Subscription<nav_msgs::msg::Odometry>::SharedPtr odom_sub_;

  // Output configuration
  std::string artifact_dir_;
  std::string file_format_;
  std::string output_file_;
  std::unique_ptr<StreamingWriter> writer_;

  // Transform
  std::mutex mutex_;
  Eigen::Affine3f transform_ = Eigen::Affine3f::Identity();
  bool has_pose_ = false;
  bool enable_transform_ = false;

  // Statistics
  size_t total_points_ = 0;
  size_t last_report_count_ = 0;
};

int main(int argc, char** argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<RecorderNode>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
