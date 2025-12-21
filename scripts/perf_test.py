#!/usr/bin/env python3
"""
Performance test for pointcloud recorder node.
Publishes random point clouds and poses while measuring recorder's CPU/memory usage.

Usage:
  # Terminal 1: Start the recorder node
  ros2 run pointcloud_bridge recorder_node --ros-args -p pointcloud_topic:=/perf_test/pointcloud -p pose_topic:=/perf_test/pose

  # Terminal 2: Run this test
  python3 perf_test.py --points 100000 --rate 10 --duration 30

  # Or test optimized version:
  ros2 run pointcloud_bridge recorder_node_optimized --ros-args -p pointcloud_topic:=/perf_test/pointcloud -p pose_topic:=/perf_test/pose
"""

import argparse
import time
import csv
import os
import subprocess
import numpy as np
from dataclasses import dataclass
from typing import Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import PointCloud2, PointField
from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Header


@dataclass
class PerfStats:
    timestamp_sec: float
    cpu_percent: float
    memory_mb: float
    points_published: int
    clouds_published: int


def create_pointcloud_msg(num_points: int, frame_id: str = "base_link") -> PointCloud2:
    """Create a PointCloud2 message with random XYZ points."""
    msg = PointCloud2()
    msg.header = Header()
    msg.header.frame_id = frame_id
    msg.height = 1
    msg.width = num_points
    msg.is_dense = True
    msg.is_bigendian = False

    # Define XYZ fields (float32 each)
    msg.fields = [
        PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
    ]
    msg.point_step = 12  # 3 * 4 bytes
    msg.row_step = msg.point_step * num_points

    # Generate random points
    points = np.random.uniform(-50.0, 50.0, (num_points, 3)).astype(np.float32)
    msg.data = points.tobytes()

    return msg


def create_random_pose() -> PoseStamped:
    """Create a PoseStamped message with random position and orientation."""
    msg = PoseStamped()
    msg.header.frame_id = "world"

    # Random position
    msg.pose.position.x = np.random.uniform(-10.0, 10.0)
    msg.pose.position.y = np.random.uniform(-10.0, 10.0)
    msg.pose.position.z = np.random.uniform(-10.0, 10.0)

    # Random quaternion (normalized)
    q = np.random.uniform(-1.0, 1.0, 4)
    q = q / np.linalg.norm(q)
    msg.pose.orientation.x = q[0]
    msg.pose.orientation.y = q[1]
    msg.pose.orientation.z = q[2]
    msg.pose.orientation.w = q[3]

    return msg


def find_recorder_pid(node_name: str = "recorder_node") -> Optional[int]:
    """Find PID of the recorder node process (actual binary, not ros2 wrapper)."""
    try:
        # Search for the actual binary path, not the ros2 run wrapper
        # Binary is at: /ros2_ws/install/pointcloud_bridge/lib/pointcloud_bridge/recorder_node
        result = subprocess.run(
            ["pgrep", "-f", f"lib/pointcloud_bridge/{node_name}"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split('\n')
        if pids and pids[0]:
            return int(pids[0])
    except Exception:
        pass
    return None


def get_process_stats(pid: int) -> tuple[float, float]:
    """Get CPU and memory usage for a process."""
    try:
        # Read /proc/[pid]/stat for CPU
        with open(f"/proc/{pid}/stat", 'r') as f:
            stat = f.read().split()
            utime = int(stat[13])  # User time
            stime = int(stat[14])  # System time

        # Read /proc/[pid]/statm for memory
        with open(f"/proc/{pid}/statm", 'r') as f:
            statm = f.read().split()
            pages = int(statm[0])  # Total program size

        page_size = os.sysconf('SC_PAGE_SIZE')
        memory_mb = (pages * page_size) / (1024 * 1024)

        return (utime + stime, memory_mb)
    except Exception:
        return (0, 0)


class PerfTestNode(Node):
    def __init__(self, args):
        super().__init__('recorder_perf_test')

        self.points_per_cloud = args.points
        self.publish_rate = args.rate
        self.duration_sec = args.duration
        self.output_csv = args.output
        self.recorder_name = args.recorder

        self.get_logger().info("=== Recorder Performance Test ===")
        self.get_logger().info(f"Points per cloud: {self.points_per_cloud}")
        self.get_logger().info(f"Publish rate: {self.publish_rate} Hz")
        self.get_logger().info(f"Duration: {self.duration_sec} seconds")
        self.get_logger().info(f"Output CSV: {self.output_csv}")

        # Find recorder process
        self.recorder_pid = find_recorder_pid(self.recorder_name)
        if self.recorder_pid:
            self.get_logger().info(f"Found recorder PID: {self.recorder_pid}")
        else:
            self.get_logger().warn(f"Recorder process '{self.recorder_name}' not found - will retry")

        # Publishers - use RELIABLE QoS to match recorder's default
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE)
        self.cloud_pub = self.create_publisher(PointCloud2, '/perf_test/pointcloud', qos)
        self.pose_pub = self.create_publisher(PoseStamped, '/perf_test/pose', qos)

        # Pre-generate cloud template (reuse to avoid allocation overhead)
        self.get_logger().info("Generating point cloud template...")
        self.cloud_template = create_pointcloud_msg(self.points_per_cloud)
        cloud_size_mb = len(self.cloud_template.data) / (1024 * 1024)
        self.get_logger().info(f"Cloud template: {self.points_per_cloud} points ({cloud_size_mb:.2f} MB)")

        # Stats tracking
        self.stats: list[PerfStats] = []
        self.points_published = 0
        self.clouds_published = 0
        self.start_time = time.time()
        self.last_cpu_ticks = 0
        self.last_stat_time = time.time()

        # CSV file
        self.csv_file = open(self.output_csv, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(['timestamp_sec', 'cpu_percent', 'memory_mb',
                                   'points_published', 'clouds_published'])

        # Timers
        publish_period = 1.0 / self.publish_rate
        self.publish_timer = self.create_timer(publish_period, self.publish_callback)
        self.stats_timer = self.create_timer(0.5, self.collect_stats)

        self.get_logger().info("Starting test...")

    def publish_callback(self):
        elapsed = time.time() - self.start_time
        if elapsed >= self.duration_sec:
            self.get_logger().info("Test duration reached, shutting down...")
            self.shutdown()
            return

        # Update timestamp and publish cloud
        self.cloud_template.header.stamp = self.get_clock().now().to_msg()
        self.cloud_pub.publish(self.cloud_template)
        self.clouds_published += 1
        self.points_published += self.points_per_cloud

        # Publish random pose
        pose = create_random_pose()
        pose.header.stamp = self.get_clock().now().to_msg()
        self.pose_pub.publish(pose)

    def collect_stats(self):
        # Try to find recorder if not found yet
        if not self.recorder_pid:
            self.recorder_pid = find_recorder_pid(self.recorder_name)
            if self.recorder_pid:
                self.get_logger().info(f"Found recorder PID: {self.recorder_pid}")

        elapsed = time.time() - self.start_time
        now = time.time()

        cpu_percent = 0.0
        memory_mb = 0.0

        if self.recorder_pid:
            cpu_ticks, memory_mb = get_process_stats(self.recorder_pid)

            # Calculate CPU percentage
            time_diff = now - self.last_stat_time
            tick_diff = cpu_ticks - self.last_cpu_ticks
            # Convert ticks to seconds (typically 100 ticks/sec)
            ticks_per_sec = os.sysconf('SC_CLK_TCK')
            cpu_seconds = tick_diff / ticks_per_sec
            cpu_percent = 100.0 * cpu_seconds / time_diff if time_diff > 0 else 0

            self.last_cpu_ticks = cpu_ticks
            self.last_stat_time = now

        # Log
        self.get_logger().info(
            f"[{elapsed:.1f}s] CPU: {cpu_percent:.1f}% | Mem: {memory_mb:.1f} MB | "
            f"Points: {self.points_published} | Clouds: {self.clouds_published}"
        )

        # Save to CSV
        self.csv_writer.writerow([
            f"{elapsed:.2f}", f"{cpu_percent:.1f}", f"{memory_mb:.1f}",
            self.points_published, self.clouds_published
        ])
        self.csv_file.flush()

        # Track for summary
        self.stats.append(PerfStats(
            timestamp_sec=elapsed,
            cpu_percent=cpu_percent,
            memory_mb=memory_mb,
            points_published=self.points_published,
            clouds_published=self.clouds_published
        ))

    def shutdown(self):
        self.print_summary()
        self.csv_file.close()
        raise SystemExit(0)

    def print_summary(self):
        if not self.stats:
            return

        total_time = time.time() - self.start_time
        cpu_values = [s.cpu_percent for s in self.stats if s.cpu_percent > 0]
        mem_values = [s.memory_mb for s in self.stats if s.memory_mb > 0]

        cpu_avg = np.mean(cpu_values) if cpu_values else 0
        cpu_max = np.max(cpu_values) if cpu_values else 0
        mem_avg = np.mean(mem_values) if mem_values else 0
        mem_max = np.max(mem_values) if mem_values else 0

        throughput = self.points_published / total_time / 1_000_000

        self.get_logger().info("")
        self.get_logger().info("========== PERFORMANCE SUMMARY ==========")
        self.get_logger().info(f"Total time:        {total_time:.2f} seconds")
        self.get_logger().info(f"Points published:  {self.points_published:,}")
        self.get_logger().info(f"Clouds published:  {self.clouds_published}")
        self.get_logger().info(f"Throughput:        {throughput:.2f} M points/sec")
        self.get_logger().info("")
        self.get_logger().info("Recorder CPU Usage:")
        self.get_logger().info(f"  Average: {cpu_avg:.1f}%")
        self.get_logger().info(f"  Maximum: {cpu_max:.1f}%")
        self.get_logger().info("")
        self.get_logger().info("Recorder Memory Usage:")
        self.get_logger().info(f"  Average: {mem_avg:.1f} MB")
        self.get_logger().info(f"  Maximum: {mem_max:.1f} MB")
        self.get_logger().info("=========================================")
        self.get_logger().info(f"Results saved to: {self.output_csv}")


def main():
    parser = argparse.ArgumentParser(description='Performance test for pointcloud recorder')
    parser.add_argument('--points', type=int, default=100000,
                        help='Points per cloud (default: 100000)')
    parser.add_argument('--rate', type=float, default=10.0,
                        help='Publish rate in Hz (default: 10)')
    parser.add_argument('--duration', type=int, default=30,
                        help='Test duration in seconds (default: 30)')
    parser.add_argument('--output', type=str, default='/tmp/recorder_perf.csv',
                        help='Output CSV file (default: /tmp/recorder_perf.csv)')
    parser.add_argument('--recorder', type=str, default='recorder_node',
                        help='Recorder process name to monitor (default: recorder_node)')
    args = parser.parse_args()

    rclpy.init()
    node = PerfTestNode(args)

    try:
        rclpy.spin(node)
    except SystemExit:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
