from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory
import os


def generate_launch_description():
    # Get package share directory
    pkg_share = get_package_share_directory('pointcloud_bridge')
    config_file = os.path.join(pkg_share, 'config', 'recorder_config.yaml')

    return LaunchDescription([
        Node(
            package='pointcloud_bridge',
            executable='recorder_node',
            name='recorder_node',
            output='screen',
            parameters=[config_file],
        )
    ])
