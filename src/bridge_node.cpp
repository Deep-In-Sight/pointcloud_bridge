#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

#include "pointcloud.pb.h"

#include <mutex>
#include <thread>
#include <atomic>
#include <set>

using websocketpp::connection_hdl;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

// Protobuf schema as string (served via getSchema command)
static const char* PROTO_SCHEMA = R"(syntax = "proto3";

package pointcloud;

message SensorPose {
  float px = 1;
  float py = 2;
  float pz = 3;
  float qx = 4;
  float qy = 5;
  float qz = 6;
  float qw = 7;
}

message PointCloudData {
  uint32 num_points = 1;
  bytes point_data = 2;
  uint64 timestamp = 3;
  uint32 sequence = 4;
}

message PoseData {
  SensorPose pose = 1;
  uint64 timestamp = 2;
}

message StreamMessage {
  oneof payload {
    PointCloudData points = 1;
    PoseData pose = 2;
  }
}
)";

class BridgeNode : public rclcpp::Node {
public:
  using WsServer = websocketpp::server<websocketpp::config::asio>;

  BridgeNode() : Node("pointcloud_bridge") {
    // Declare parameters
    this->declare_parameter("websocket_port", 8765);
    this->declare_parameter("pointcloud_topic", "/cloud_registered");
    this->declare_parameter("pose_topic", "");
    this->declare_parameter("pose_type", 0);  // 0 = Odometry, 1 = PoseStamped

    int port = this->get_parameter("websocket_port").as_int();
    std::string pointcloud_topic = this->get_parameter("pointcloud_topic").as_string();
    std::string pose_topic = this->get_parameter("pose_topic").as_string();
    int pose_type = this->get_parameter("pose_type").as_int();

    RCLCPP_INFO(this->get_logger(), "Starting WebSocket bridge on port %d", port);
    RCLCPP_INFO(this->get_logger(), "Subscribing to pointcloud: %s (world frame)", pointcloud_topic.c_str());

    // Create pointcloud subscriber
    lidar_sub_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
      pointcloud_topic, 10,
      std::bind(&BridgeNode::lidar_callback, this, std::placeholders::_1)
    );

    // Create pose subscriber if pose_topic is not empty
    if (!pose_topic.empty()) {
      if (pose_type == 0) {
        // Subscribe to Odometry
        RCLCPP_INFO(this->get_logger(), "Subscribing to pose (Odometry): %s", pose_topic.c_str());
        odom_sub_ = this->create_subscription<nav_msgs::msg::Odometry>(
          pose_topic, 10,
          std::bind(&BridgeNode::odom_callback, this, std::placeholders::_1)
        );
      } else if (pose_type == 1) {
        // Subscribe to PoseStamped
        RCLCPP_INFO(this->get_logger(), "Subscribing to pose (PoseStamped): %s", pose_topic.c_str());
        pose_stamped_sub_ = this->create_subscription<geometry_msgs::msg::PoseStamped>(
          pose_topic, 10,
          std::bind(&BridgeNode::pose_stamped_callback, this, std::placeholders::_1)
        );
      } else {
        RCLCPP_ERROR(this->get_logger(), "Invalid pose_type: %d (must be 0 or 1)", pose_type);
      }
    } else {
      RCLCPP_INFO(this->get_logger(), "Pose topic not configured, getLatestPose will return empty");
    }

    // Initialize WebSocket server
    ws_server_.set_access_channels(websocketpp::log::alevel::none);
    ws_server_.set_error_channels(websocketpp::log::elevel::warn | websocketpp::log::elevel::rerror);

    ws_server_.init_asio();
    ws_server_.set_reuse_addr(true);

    ws_server_.set_open_handler(std::bind(&BridgeNode::on_open, this, _1));
    ws_server_.set_close_handler(std::bind(&BridgeNode::on_close, this, _1));
    ws_server_.set_message_handler(std::bind(&BridgeNode::on_message, this, _1, _2));

    ws_server_.listen(port);
    ws_server_.start_accept();

    // Run WebSocket server in separate thread
    ws_thread_ = std::thread([this]() {
      ws_server_.run();
    });

    RCLCPP_INFO(this->get_logger(), "WebSocket server started");
  }

  ~BridgeNode() {
    // Set shutdown flag to suppress expected errors
    shutting_down_ = true;

    // Suppress all logging during shutdown
    ws_server_.set_access_channels(websocketpp::log::alevel::none);
    ws_server_.set_error_channels(websocketpp::log::elevel::none);

    // Stop accepting new connections
    websocketpp::lib::error_code ec;
    ws_server_.stop_listening(ec);

    // Close all connections gracefully
    {
      std::lock_guard<std::mutex> lock(conn_mutex_);
      for (auto& hdl : connections_) {
        websocketpp::lib::error_code close_ec;
        ws_server_.close(hdl, websocketpp::close::status::going_away, "Server shutdown", close_ec);
      }
      connections_.clear();
    }

    // Stop the io_service
    ws_server_.stop();

    // Wait for the WebSocket thread to finish
    if (ws_thread_.joinable()) {
      ws_thread_.join();
    }
  }

private:
  void lidar_callback(const sensor_msgs::msg::PointCloud2::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(pointcloud_mutex_);
    latest_pointcloud_ = msg;
    has_new_pointcloud_ = true;
    pointcloud_sequence_++;
    RCLCPP_DEBUG(this->get_logger(), "Received pointcloud with %u points",
                 msg->width * msg->height);
  }

  void pose_stamped_callback(const geometry_msgs::msg::PoseStamped::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(pose_mutex_);
    latest_position_[0] = msg->pose.position.x;
    latest_position_[1] = msg->pose.position.y;
    latest_position_[2] = msg->pose.position.z;
    latest_orientation_[0] = msg->pose.orientation.x;
    latest_orientation_[1] = msg->pose.orientation.y;
    latest_orientation_[2] = msg->pose.orientation.z;
    latest_orientation_[3] = msg->pose.orientation.w;
    pose_timestamp_ = msg->header.stamp.sec * 1000000000ULL + msg->header.stamp.nanosec;
    has_pose_ = true;
    RCLCPP_DEBUG(this->get_logger(), "Received PoseStamped: [%.2f, %.2f, %.2f]",
                 msg->pose.position.x, msg->pose.position.y, msg->pose.position.z);
  }

  void odom_callback(const nav_msgs::msg::Odometry::SharedPtr msg) {
    std::lock_guard<std::mutex> lock(pose_mutex_);
    latest_position_[0] = msg->pose.pose.position.x;
    latest_position_[1] = msg->pose.pose.position.y;
    latest_position_[2] = msg->pose.pose.position.z;
    latest_orientation_[0] = msg->pose.pose.orientation.x;
    latest_orientation_[1] = msg->pose.pose.orientation.y;
    latest_orientation_[2] = msg->pose.pose.orientation.z;
    latest_orientation_[3] = msg->pose.pose.orientation.w;
    pose_timestamp_ = msg->header.stamp.sec * 1000000000ULL + msg->header.stamp.nanosec;
    has_pose_ = true;
    RCLCPP_DEBUG(this->get_logger(), "Received Odometry: [%.2f, %.2f, %.2f]",
                 msg->pose.pose.position.x, msg->pose.pose.position.y, msg->pose.pose.position.z);
  }

  void on_open(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(conn_mutex_);
    connections_.insert(hdl);
    RCLCPP_INFO(this->get_logger(), "Client connected. Total: %zu", connections_.size());
  }

  void on_close(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(conn_mutex_);
    connections_.erase(hdl);
    RCLCPP_INFO(this->get_logger(), "Client disconnected. Total: %zu", connections_.size());
  }

  void on_message(connection_hdl hdl, WsServer::message_ptr msg) {
    std::string payload = msg->get_payload();

    if (payload == "getSchema") {
      // Send protobuf schema as text
      try {
        ws_server_.send(hdl, PROTO_SCHEMA, websocketpp::frame::opcode::text);
        RCLCPP_DEBUG(this->get_logger(), "Sent schema to client");
      } catch (const std::exception& e) {
        RCLCPP_ERROR(this->get_logger(), "Failed to send schema: %s", e.what());
      }
    }
    else if (payload == "getLatestPoints") {
      // Send latest pointcloud wrapped in StreamMessage
      send_latest_points(hdl);
    }
    else if (payload == "getLatestPose") {
      // Send latest pose wrapped in StreamMessage
      send_latest_pose(hdl);
    }
    else {
      RCLCPP_WARN(this->get_logger(), "Unknown command: %s", payload.c_str());
    }
  }

  void send_latest_points(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(pointcloud_mutex_);

    if (!has_new_pointcloud_ || !latest_pointcloud_) {
      // Send empty response if no new data
      try {
        ws_server_.send(hdl, "", websocketpp::frame::opcode::binary);
      } catch (...) {}
      return;
    }

    // Build StreamMessage with PointCloudData
    pointcloud::StreamMessage stream_msg;
    auto* points_data = stream_msg.mutable_points();

    // Extract points from PointCloud2
    std::vector<float> point_data;
    uint32_t num_points = latest_pointcloud_->width * latest_pointcloud_->height;
    point_data.reserve(num_points * 3);  // x, y, z per point

    // Create iterators for x, y, z fields
    sensor_msgs::PointCloud2ConstIterator<float> iter_x(*latest_pointcloud_, "x");
    sensor_msgs::PointCloud2ConstIterator<float> iter_y(*latest_pointcloud_, "y");
    sensor_msgs::PointCloud2ConstIterator<float> iter_z(*latest_pointcloud_, "z");

    for (uint32_t i = 0; i < num_points; ++i, ++iter_x, ++iter_y, ++iter_z) {
      // Skip invalid points (NaN)
      if (std::isnan(*iter_x) || std::isnan(*iter_y) || std::isnan(*iter_z)) {
        continue;
      }
      point_data.push_back(*iter_x);
      point_data.push_back(*iter_y);
      point_data.push_back(*iter_z);
    }

    // Update actual point count after filtering NaN
    num_points = point_data.size() / 3;
    points_data->set_num_points(num_points);

    // Set point data as bytes
    points_data->set_point_data(point_data.data(), point_data.size() * sizeof(float));

    // Set timestamp and sequence
    points_data->set_timestamp(
      latest_pointcloud_->header.stamp.sec * 1000000000ULL +
      latest_pointcloud_->header.stamp.nanosec
    );
    points_data->set_sequence(pointcloud_sequence_);

    // Mark as consumed
    has_new_pointcloud_ = false;

    // Serialize and send
    std::string serialized;
    if (stream_msg.SerializeToString(&serialized)) {
      try {
        ws_server_.send(hdl, serialized, websocketpp::frame::opcode::binary);
        RCLCPP_DEBUG(this->get_logger(), "Sent points with %u points, seq=%u",
                     num_points, pointcloud_sequence_.load());
      } catch (const std::exception& e) {
        RCLCPP_ERROR(this->get_logger(), "Failed to send points: %s", e.what());
      }
    } else {
      RCLCPP_ERROR(this->get_logger(), "Failed to serialize StreamMessage");
    }
  }

  void send_latest_pose(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(pose_mutex_);

    if (!has_pose_) {
      // Send empty response if no pose data
      try {
        ws_server_.send(hdl, "", websocketpp::frame::opcode::binary);
      } catch (...) {}
      return;
    }

    // Build StreamMessage with PoseData
    pointcloud::StreamMessage stream_msg;
    auto* pose_data = stream_msg.mutable_pose();
    auto* pose = pose_data->mutable_pose();

    pose->set_px(latest_position_[0]);
    pose->set_py(latest_position_[1]);
    pose->set_pz(latest_position_[2]);
    pose->set_qx(latest_orientation_[0]);
    pose->set_qy(latest_orientation_[1]);
    pose->set_qz(latest_orientation_[2]);
    pose->set_qw(latest_orientation_[3]);
    pose_data->set_timestamp(pose_timestamp_);

    // Serialize and send
    std::string serialized;
    if (stream_msg.SerializeToString(&serialized)) {
      try {
        ws_server_.send(hdl, serialized, websocketpp::frame::opcode::binary);
        RCLCPP_DEBUG(this->get_logger(), "Sent pose: [%.2f, %.2f, %.2f]",
                     latest_position_[0], latest_position_[1], latest_position_[2]);
      } catch (const std::exception& e) {
        RCLCPP_ERROR(this->get_logger(), "Failed to send pose: %s", e.what());
      }
    } else {
      RCLCPP_ERROR(this->get_logger(), "Failed to serialize StreamMessage");
    }
  }

  // ROS subscribers
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_sub_;
  rclcpp::Subscription<geometry_msgs::msg::PoseStamped>::SharedPtr pose_stamped_sub_;
  rclcpp::Subscription<nav_msgs::msg::Odometry>::SharedPtr odom_sub_;

  // WebSocket server
  WsServer ws_server_;
  std::thread ws_thread_;
  std::mutex conn_mutex_;
  std::set<connection_hdl, std::owner_less<connection_hdl>> connections_;
  std::atomic<bool> shutting_down_{false};

  // Pointcloud data (protected by pointcloud_mutex_)
  std::mutex pointcloud_mutex_;
  sensor_msgs::msg::PointCloud2::SharedPtr latest_pointcloud_;
  std::atomic<uint32_t> pointcloud_sequence_{0};
  bool has_new_pointcloud_{false};

  // Pose data (protected by pose_mutex_)
  std::mutex pose_mutex_;
  float latest_position_[3] = {0.0f, 0.0f, 0.0f};
  float latest_orientation_[4] = {0.0f, 0.0f, 0.0f, 1.0f};  // Identity quaternion
  uint64_t pose_timestamp_{0};
  bool has_pose_{false};
};

int main(int argc, char** argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<BridgeNode>();
  rclcpp::spin(node);
  if (rclcpp::ok()) {
    rclcpp::shutdown();
  }
  return 0;
}
