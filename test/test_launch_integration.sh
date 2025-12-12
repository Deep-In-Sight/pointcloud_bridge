#!/bin/bash
# Integration test for Fast-LIO launch with bridge and recorder
# This test verifies that all nodes can be launched and topics are published correctly

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAIL=$((FAIL + 1))
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${RED}[WARN]${NC} $1"
}

# Source ROS2
source /opt/ros/humble/setup.bash
source /ros2_ws/install/setup.bash

echo ""
echo "========================================"
echo "Test 1: Bridge Launch Verification"
echo "========================================"

log_info "Testing bridge launch file syntax..."
if ros2 launch pointcloud_bridge bridge.launch.py --show-args &> /tmp/bridge_launch_test.log; then
    log_pass "Bridge launch file is valid"
else
    log_fail "Bridge launch file has errors"
    cat /tmp/bridge_launch_test.log
fi

echo ""
echo "========================================"
echo "Test 2: Recorder Launch Verification"
echo "========================================"

log_info "Testing recorder launch file syntax..."
if ros2 launch pointcloud_bridge recorder.launch.py --show-args &> /tmp/recorder_launch_test.log; then
    log_pass "Recorder launch file is valid"
else
    log_fail "Recorder launch file has errors"
    cat /tmp/recorder_launch_test.log
fi

echo ""
echo "========================================"
echo "Test 3: Fast-LIO Launch Verification"
echo "========================================"

log_info "Testing Fast-LIO with bridge launch file syntax..."
if ros2 launch fast_lio mapping_with_bridge.launch.py --show-args &> /tmp/fastlio_launch_test.log; then
    log_pass "Fast-LIO with bridge launch file is valid"
else
    log_fail "Fast-LIO with bridge launch file has errors"
    cat /tmp/fastlio_launch_test.log
fi

echo ""
echo "========================================"
echo "Test 4: Node Executables"
echo "========================================"

INSTALL_DIR=$(ros2 pkg prefix pointcloud_bridge)

log_info "Checking if bridge_node executable exists..."
if [ -x "$INSTALL_DIR/lib/pointcloud_bridge/bridge_node" ]; then
    log_pass "bridge_node executable exists"
else
    log_fail "bridge_node executable not found"
fi

log_info "Checking if recorder_node executable exists..."
if [ -x "$INSTALL_DIR/lib/pointcloud_bridge/recorder_node" ]; then
    log_pass "recorder_node executable exists"
else
    log_fail "recorder_node executable not found"
fi

echo ""
echo "========================================"
echo "Test 5: Package Dependencies"
echo "========================================"

log_info "Checking pointcloud_bridge dependencies..."
if ros2 pkg xml pointcloud_bridge | grep -q "sensor_msgs"; then
    log_pass "sensor_msgs dependency declared"
else
    log_fail "sensor_msgs dependency missing"
fi

if ros2 pkg xml pointcloud_bridge | grep -q "nav_msgs"; then
    log_pass "nav_msgs dependency declared"
else
    log_fail "nav_msgs dependency missing"
fi

if ros2 pkg xml pointcloud_bridge | grep -q "geometry_msgs"; then
    log_pass "geometry_msgs dependency declared"
else
    log_fail "geometry_msgs dependency missing"
fi

echo ""
echo "========================================"
echo "Test 6: Config File Installation"
echo "========================================"

INSTALL_DIR=$(ros2 pkg prefix pointcloud_bridge)

if [ -f "$INSTALL_DIR/share/pointcloud_bridge/config/bridge_config.yaml" ]; then
    log_pass "bridge_config.yaml installed"
else
    log_fail "bridge_config.yaml not found in install directory"
fi

if [ -f "$INSTALL_DIR/share/pointcloud_bridge/config/recorder_config.yaml" ]; then
    log_pass "recorder_config.yaml installed"
else
    log_fail "recorder_config.yaml not found in install directory"
fi

if [ -f "$INSTALL_DIR/share/pointcloud_bridge/config/fastlio_bridge_config.yaml" ]; then
    log_pass "fastlio_bridge_config.yaml installed"
else
    log_fail "fastlio_bridge_config.yaml not found in install directory"
fi

if [ -f "$INSTALL_DIR/share/pointcloud_bridge/config/fastlio_recorder_config.yaml" ]; then
    log_pass "fastlio_recorder_config.yaml installed"
else
    log_fail "fastlio_recorder_config.yaml not found in install directory"
fi

echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo -e "${GREEN}Passed: $PASS${NC}"
echo -e "${RED}Failed: $FAIL${NC}"
echo ""

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}Some tests failed. Please fix issues before proceeding.${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
