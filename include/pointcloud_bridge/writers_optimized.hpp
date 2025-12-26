#pragma once

#include <fstream>
#include <memory>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>

#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

// libLAS for LAS writing
#include <liblas/liblas.hpp>

// lazperf for LAZ writing
#include <lazperf/writers.hpp>

// Base buffered writer interface - works with transformed points
class BufferedStreamingWriter {
public:
  virtual ~BufferedStreamingWriter() = default;
  virtual bool open(const std::string& filename) = 0;
  virtual bool write_points(const float* xyz_data, size_t num_points) = 0;
  virtual bool close() = 0;
  virtual size_t get_total_points() const { return total_points_; }

  // Write points with stride (for PCL PointXYZ which has 16-byte stride)
  bool write_points_strided(const float* data, size_t num_points, size_t stride_floats) {
    const float* contiguous = make_contiguous(data, num_points, stride_floats);
    return write_points(contiguous, num_points);
  }

protected:
  size_t total_points_ = 0;

  // Convert strided data to contiguous XYZ layout
  const float* make_contiguous(const float* data, size_t num_points, size_t stride_floats) {
    if (stride_floats == 3) {
      return data;  // Already contiguous
    }
    contiguous_buffer_.resize(num_points * 3);
    for (size_t i = 0; i < num_points; ++i) {
      contiguous_buffer_[i * 3 + 0] = data[i * stride_floats + 0];
      contiguous_buffer_[i * 3 + 1] = data[i * stride_floats + 1];
      contiguous_buffer_[i * 3 + 2] = data[i * stride_floats + 2];
    }
    return contiguous_buffer_.data();
  }

  std::vector<float> contiguous_buffer_;
};

// Buffered PLY writer
class BufferedPLYWriter : public BufferedStreamingWriter {
public:
  bool open(const std::string& filename) override {
    file_.open(filename, std::ios::binary);
    if (!file_.is_open()) return false;

    file_.rdbuf()->pubsetbuf(stream_buffer_.data(), stream_buffer_.size());

    file_ << "ply\n";
    file_ << "format binary_little_endian 1.0\n";
    file_ << "element vertex 0000000000\n";
    file_ << "property float x\n";
    file_ << "property float y\n";
    file_ << "property float z\n";
    file_ << "end_header\n";

    return true;
  }

  bool write_points(const float* xyz_data, size_t num_points) override {
    if (!file_.is_open() || num_points == 0) return false;
    file_.write(reinterpret_cast<const char*>(xyz_data), num_points * 3 * sizeof(float));
    total_points_ += num_points;
    return true;
  }

  bool close() override {
    if (!file_.is_open()) return false;
    file_.flush();
    file_.seekp(51);
    char count_str[11];
    snprintf(count_str, sizeof(count_str), "%010zu", total_points_);
    file_.write(count_str, 10);
    file_.close();
    return true;
  }

private:
  std::ofstream file_;
  std::array<char, 1024 * 1024> stream_buffer_;
};

// Buffered PCD writer
class BufferedPCDWriter : public BufferedStreamingWriter {
public:
  bool open(const std::string& filename) override {
    file_.open(filename, std::ios::binary);
    if (!file_.is_open()) return false;

    file_.rdbuf()->pubsetbuf(stream_buffer_.data(), stream_buffer_.size());

    file_ << "# .PCD v0.7 - Point Cloud Data file format\n";
    file_ << "VERSION 0.7\n";
    file_ << "FIELDS x y z\n";
    file_ << "SIZE 4 4 4\n";
    file_ << "TYPE F F F\n";
    file_ << "COUNT 1 1 1\n";
    file_ << "WIDTH 0000000000\n";
    file_ << "HEIGHT 1\n";
    file_ << "VIEWPOINT 0 0 0 1 0 0 0\n";
    file_ << "POINTS 0000000000\n";
    file_ << "DATA binary\n";

    return true;
  }

  bool write_points(const float* xyz_data, size_t num_points) override {
    if (!file_.is_open() || num_points == 0) return false;
    file_.write(reinterpret_cast<const char*>(xyz_data), num_points * 3 * sizeof(float));
    total_points_ += num_points;
    return true;
  }

  bool close() override {
    if (!file_.is_open()) return false;
    file_.flush();
    char count_str[11];
    snprintf(count_str, sizeof(count_str), "%010zu", total_points_);
    file_.seekp(108);
    file_.write(count_str, 10);
    file_.seekp(159);
    file_.write(count_str, 10);
    file_.close();
    return true;
  }

private:
  std::ofstream file_;
  std::array<char, 1024 * 1024> stream_buffer_;
};

// Buffered LAS writer
class BufferedLASWriter : public BufferedStreamingWriter {
public:
  bool open(const std::string& filename) override {
    ofs_ = std::make_unique<std::ofstream>();
    ofs_->open(filename, std::ios::out | std::ios::binary);
    if (!ofs_->is_open()) return false;

    liblas::Header header;
    header.SetVersionMinor(2);
    header.SetDataFormatId(liblas::ePointFormat0);
    header.SetScale(0.001, 0.001, 0.001);
    header.SetOffset(0.0, 0.0, 0.0);
    writer_ = std::make_unique<liblas::Writer>(*ofs_, header);
    return true;
  }

  bool write_points(const float* xyz_data, size_t num_points) override {
    if (!writer_ || num_points == 0) return false;
    liblas::Point las_point(&writer_->GetHeader());
    for (size_t i = 0; i < num_points; ++i) {
      las_point.SetCoordinates(xyz_data[i*3], xyz_data[i*3+1], xyz_data[i*3+2]);
      writer_->WritePoint(las_point);
    }
    total_points_ += num_points;
    return true;
  }

  bool close() override {
    writer_.reset();
    ofs_.reset();
    return true;
  }

private:
  std::unique_ptr<std::ofstream> ofs_;
  std::unique_ptr<liblas::Writer> writer_;
};

// Buffered LAZ writer
class BufferedLAZWriter : public BufferedStreamingWriter {
public:
  bool open(const std::string& filename) override {
    lazperf::writer::named_file::config config;
    config.scale = lazperf::vector3(0.001, 0.001, 0.001);
    config.offset = lazperf::vector3(0.0, 0.0, 0.0);
    config.pdrf = 0;
    config.minor_version = 2;
    config.chunk_size = 50000;
    writer_ = std::make_unique<lazperf::writer::named_file>(filename, config);
    return true;
  }

  bool write_points(const float* xyz_data, size_t num_points) override {
    if (!writer_ || num_points == 0) return false;
    char point_data[20] = {0};
    for (size_t i = 0; i < num_points; ++i) {
      int32_t x = static_cast<int32_t>(xyz_data[i*3] / 0.001);
      int32_t y = static_cast<int32_t>(xyz_data[i*3+1] / 0.001);
      int32_t z = static_cast<int32_t>(xyz_data[i*3+2] / 0.001);
      std::memcpy(point_data + 0, &x, 4);
      std::memcpy(point_data + 4, &y, 4);
      std::memcpy(point_data + 8, &z, 4);
      writer_->writePoint(point_data);
    }
    total_points_ += num_points;
    return true;
  }

  bool close() override {
    if (writer_) {
      writer_->close();
      writer_.reset();
    }
    return true;
  }

private:
  std::unique_ptr<lazperf::writer::named_file> writer_;
};

// Profiling stats for writer thread
struct WriterStats {
  std::atomic<size_t> clouds_processed{0};
  std::atomic<size_t> write_time_us{0};
  std::atomic<size_t> total_time_us{0};

  void reset() {
    clouds_processed = 0;
    write_time_us = 0;
    total_time_us = 0;
  }

  void print_summary() const {
    size_t clouds = clouds_processed.load();
    if (clouds == 0) return;

    double avg_write_ms = (write_time_us.load() / clouds) / 1000.0;
    double avg_total_ms = (total_time_us.load() / clouds) / 1000.0;

    printf("Writer stats: %zu clouds | Avg write: %.2f ms | Avg total: %.2f ms\n",
           clouds, avg_write_ms, avg_total_ms);
  }
};

// Async writer that accepts PCL clouds and writes to file (no transformation)
class AsyncPCLWriter {
public:
  explicit AsyncPCLWriter(std::unique_ptr<BufferedStreamingWriter> writer)
    : writer_(std::move(writer)) {}

  ~AsyncPCLWriter() { stop(); }

  bool open(const std::string& filename) {
    if (!writer_->open(filename)) return false;
    stats_.reset();
    running_ = true;
    writer_thread_ = std::thread(&AsyncPCLWriter::writer_loop, this);
    return true;
  }

  // Fast: just queue the cloud and return
  void add_cloud(pcl::PointCloud<pcl::PointXYZ>::Ptr cloud) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      cloud_queue_.push(cloud);
    }
    queue_cv_.notify_one();
  }

  bool close() {
    stop();
    stats_.print_summary();
    return writer_->close();
  }

  size_t get_total_points() const { return writer_->get_total_points(); }
  size_t get_queue_size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return cloud_queue_.size();
  }

  const WriterStats& get_stats() const { return stats_; }

private:
  void writer_loop() {
    while (true) {
      pcl::PointCloud<pcl::PointXYZ>::Ptr cloud;

      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return !cloud_queue_.empty() || !running_; });

        if (!running_ && cloud_queue_.empty()) break;

        if (!cloud_queue_.empty()) {
          cloud = cloud_queue_.front();
          cloud_queue_.pop();
        }
      }

      if (cloud && !cloud->empty()) {
        auto total_start = std::chrono::high_resolution_clock::now();

        // Remove NaN/Inf points
        pcl::PointCloud<pcl::PointXYZ>::Ptr filtered_cloud(new pcl::PointCloud<pcl::PointXYZ>());
        filtered_cloud->reserve(cloud->size());
        for (const auto& pt : cloud->points) {
          if (std::isfinite(pt.x) && std::isfinite(pt.y) && std::isfinite(pt.z)) {
            filtered_cloud->push_back(pt);
          }
        }

        if (filtered_cloud->empty()) continue;

        size_t num_points = filtered_cloud->size();

        // Write points - PCL PointXYZ has 16-byte stride, need to extract XYZ
        auto write_start = std::chrono::high_resolution_clock::now();

        // Direct write - PointXYZ is [x,y,z,padding] so we can write the first 12 bytes of each
        const float* point_data = reinterpret_cast<const float*>(filtered_cloud->points.data());
        writer_->write_points_strided(point_data, num_points, 4);  // stride=4 floats (16 bytes)

        auto write_end = std::chrono::high_resolution_clock::now();

        // Update stats
        auto write_us = std::chrono::duration_cast<std::chrono::microseconds>(
          write_end - write_start).count();
        auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(
          write_end - total_start).count();

        stats_.clouds_processed.fetch_add(1, std::memory_order_relaxed);
        stats_.write_time_us.fetch_add(write_us, std::memory_order_relaxed);
        stats_.total_time_us.fetch_add(total_us, std::memory_order_relaxed);
      }
    }
  }

  void stop() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      running_ = false;
    }
    queue_cv_.notify_all();
    if (writer_thread_.joinable()) {
      writer_thread_.join();
    }
  }

  std::unique_ptr<BufferedStreamingWriter> writer_;
  std::queue<pcl::PointCloud<pcl::PointXYZ>::Ptr> cloud_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread writer_thread_;
  std::atomic<bool> running_{false};
  WriterStats stats_;
};
