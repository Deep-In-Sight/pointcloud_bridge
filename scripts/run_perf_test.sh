#!/bin/bash
# Performance test script for pointcloud recorder
# Usage: ./run_perf_test.sh [original|optimized] [duration]

set -e

RECORDER=${1:-optimized}  # original or optimized
DURATION=${2:-20}
POINTS=${3:-100000}
RATE=${4:-10}

EXEC_SCRIPT="/home/linh/ros2_ws/dimenvue_server/docker/exec.sh"

echo "=== Pointcloud Recorder Performance Test ==="
echo "Recorder: $RECORDER"
echo "Duration: ${DURATION}s"
echo "Points/cloud: $POINTS"
echo "Rate: ${RATE} Hz"
echo ""

# 1. Restart container
echo "[1/4] Restarting container..."
docker restart dimenvue_server > /dev/null
sleep 3
echo "      Done."

# 2. Rebuild
echo "[2/4] Rebuilding pointcloud_bridge..."
$EXEC_SCRIPT "cd /ros2_ws && colcon build --packages-select pointcloud_bridge --cmake-args -DCMAKE_BUILD_TYPE=Release 2>&1 | tail -3"
echo "      Done."

# 3. Start recorder
echo "[3/4] Starting recorder_node_${RECORDER}..."
mkdir -p /tmp/perf_test_output

if [ "$RECORDER" = "original" ]; then
  NODE_NAME="recorder_node"
else
  NODE_NAME="recorder_node_optimized"
fi

$EXEC_SCRIPT "mkdir -p /tmp/perf_output && ros2 run pointcloud_bridge $NODE_NAME --ros-args \
  -p pointcloud_topic:=/perf_test/pointcloud \
  -p pose_topic:=/perf_test/pose \
  -p pose_type:=1 \
  -p artifact_dir:=/tmp/perf_output \
  -p file_format:=ply" > /tmp/perf_test_output/recorder.log 2>&1 &

RECORDER_PID=$!
sleep 2

# Check if recorder started
if ! kill -0 $RECORDER_PID 2>/dev/null; then
  echo "      ERROR: Recorder failed to start. Check /tmp/perf_test_output/recorder.log"
  exit 1
fi
echo "      Done (PID: $RECORDER_PID)"

# 4. Run perf test
echo "[4/4] Running performance test..."
echo ""

$EXEC_SCRIPT "python3 /ros2_ws/src/pointcloud_bridge/scripts/perf_test.py \
  --points $POINTS \
  --rate $RATE \
  --duration $DURATION \
  --output /tmp/perf_output/perf_results.csv \
  --recorder $NODE_NAME"

echo ""
echo "=== Recorder Stats ==="
tail -20 /tmp/perf_test_output/recorder.log | grep -E "(CB:|Writer stats)"

echo ""
echo "=== Output File ==="
$EXEC_SCRIPT "ls -lh /tmp/perf_output/map_result.ply"

# Cleanup
kill $RECORDER_PID 2>/dev/null || true
wait $RECORDER_PID 2>/dev/null || true

echo ""
echo "Done. Results saved to /tmp/perf_output/"
