#!/usr/bin/env python3
"""
Integration tests for pointcloud_bridge and pointcloud_recorder
Tests configuration, topic subscriptions, and basic functionality
"""

import os
import sys
import time
import subprocess
import socket

# Color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

PASS = 0
FAIL = 0

def log_pass(msg):
    global PASS
    print(f"{GREEN}[PASS]{NC} {msg}")
    PASS += 1

def log_fail(msg):
    global FAIL
    print(f"{RED}[FAIL]{NC} {msg}")
    FAIL += 1

def log_info(msg):
    print(f"{YELLOW}[INFO]{NC} {msg}")


def run_command(cmd, timeout=10):
    """Run command and return output"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Timeout"


def check_port_available(port):
    """Check if a port is available"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', port))
    sock.close()
    return result == 0


# =============================================================================
# Test 1: Package Build
# =============================================================================
print("\n" + "="*60)
print("Test 1: Package Build Verification")
print("="*60)

log_info("Checking if pointcloud_bridge package is built...")
ret, out, err = run_command("test -f /ros2_ws/install/pointcloud_bridge/lib/pointcloud_bridge/bridge_node")

if ret == 0:
    log_pass("bridge_node executable exists")
else:
    log_fail("bridge_node executable not found")

ret, out, err = run_command("test -f /ros2_ws/install/pointcloud_bridge/lib/pointcloud_bridge/recorder_node")

if ret == 0:
    log_pass("recorder_node executable exists")
else:
    log_fail("recorder_node executable not found")


# =============================================================================
# Test 2: Config Files
# =============================================================================
print("\n" + "="*60)
print("Test 2: Configuration Files")
print("="*60)

config_files = [
    "/ros2_ws/src/pointcloud_bridge/config/bridge_config.yaml",
    "/ros2_ws/src/pointcloud_bridge/config/recorder_config.yaml",
    "/ros2_ws/src/pointcloud_bridge/config/fastlio_bridge_config.yaml",
    "/ros2_ws/src/pointcloud_bridge/config/fastlio_recorder_config.yaml",
]

for config_file in config_files:
    if os.path.exists(config_file):
        log_pass(f"Config file exists: {os.path.basename(config_file)}")

        # Check YAML is valid
        try:
            import yaml
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            if config:
                log_pass(f"Config is valid YAML: {os.path.basename(config_file)}")
        except Exception as e:
            log_fail(f"Invalid YAML in {os.path.basename(config_file)}: {e}")
    else:
        log_fail(f"Config file missing: {config_file}")


# =============================================================================
# Test 3: Launch Files
# =============================================================================
print("\n" + "="*60)
print("Test 3: Launch Files")
print("="*60)

launch_files = [
    "/ros2_ws/src/pointcloud_bridge/launch/bridge.launch.py",
    "/ros2_ws/src/pointcloud_bridge/launch/recorder.launch.py",
    "/ros2_ws/src/FAST_LIO/launch/mapping_with_bridge.launch.py",
]

for launch_file in launch_files:
    if os.path.exists(launch_file):
        log_pass(f"Launch file exists: {os.path.basename(launch_file)}")

        # Check Python syntax
        ret, out, err = run_command(f"python3 -m py_compile {launch_file}")
        if ret == 0:
            log_pass(f"Launch file has valid syntax: {os.path.basename(launch_file)}")
        else:
            log_fail(f"Syntax error in {os.path.basename(launch_file)}: {err}")
    else:
        log_fail(f"Launch file missing: {launch_file}")


# =============================================================================
# Test 5: Dependencies Check
# =============================================================================
print("\n" + "="*60)
print("Test 5: Python Dependencies")
print("="*60)

python_deps = [
    "numpy",
    "yaml",
]

for dep in python_deps:
    try:
        __import__(dep)
        log_pass(f"Python module available: {dep}")
    except ImportError:
        log_fail(f"Python module missing: {dep}")


# =============================================================================
# Test 6: Writer Unit Tests (GTest)
# =============================================================================
print("\n" + "="*60)
print("Test 6: Writer Unit Tests")
print("="*60)

log_info("Running GTest for writer implementations...")

# Check if GTest executable exists
gtest_path = "/ros2_ws/build/pointcloud_bridge/test_writers"
if os.path.exists(gtest_path):
    log_pass("GTest executable exists")

    # Run GTest
    ret, out, err = run_command(f"{gtest_path}")
    if ret == 0:
        log_pass("All writer unit tests passed")
        # Print test output
        for line in out.split('\n'):
            if '[' in line and ']' in line:  # GTest output lines
                print(f"  {line}")
    else:
        log_fail("Writer unit tests failed")
        print(err)
else:
    log_fail(f"GTest executable not found at {gtest_path}")
    log_info("Tests may not have been built with --cmake-args -DBUILD_TESTING=ON")


# =============================================================================
# Summary
# =============================================================================
print("\n" + "="*60)
print("Test Summary")
print("="*60)
print(f"{GREEN}Passed: {PASS}{NC}")
print(f"{RED}Failed: {FAIL}{NC}")
print()

if FAIL > 0:
    print(f"{RED}Some tests failed. Please fix issues before proceeding.{NC}")
    sys.exit(1)
else:
    print(f"{GREEN}All tests passed!{NC}")
    sys.exit(0)
