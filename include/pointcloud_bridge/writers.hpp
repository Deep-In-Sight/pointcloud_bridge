#pragma once

#include <fstream>
#include <memory>
#include <Eigen/Dense>

// libLAS for LAS writing
#include <liblas/liblas.hpp>

// lazperf for LAZ writing
#include <lazperf/writers.hpp>

// Base streaming writer interface
class StreamingWriter {
public:
  virtual ~StreamingWriter() = default;
  virtual bool open(const std::string& filename) = 0;
  virtual bool write_point(const Eigen::Vector3f& point) = 0;
  virtual bool close() = 0;

protected:
  size_t total_points_ = 0;
};

// Streaming PLY writer
class StreamingPLYWriter : public StreamingWriter {
public:
  bool open(const std::string& filename) override {
    file_.open(filename, std::ios::binary);
    if (!file_.is_open()) return false;

    // Write header with placeholder for point count
    file_ << "ply\n";
    file_ << "format binary_little_endian 1.0\n";
    file_ << "element vertex 0000000000\n";  // 10-digit placeholder
    file_ << "property float x\n";
    file_ << "property float y\n";
    file_ << "property float z\n";
    file_ << "end_header\n";

    data_start_pos_ = file_.tellp();
    return true;
  }

  bool write_point(const Eigen::Vector3f& point) override {
    if (!file_.is_open()) return false;
    file_.write(reinterpret_cast<const char*>(point.data()), 3 * sizeof(float));
    total_points_++;
    return true;
  }

  bool close() override {
    if (!file_.is_open()) return false;

    file_.flush();

    // Calculate position of vertex count
    // "ply\n" = 4 bytes
    // "format binary_little_endian 1.0\n" = 33 bytes
    // "element vertex " = 15 bytes
    // Total = 52 bytes (position of count)
    file_.seekp(52);
    char count_str[11];
    snprintf(count_str, sizeof(count_str), "%010zu", total_points_);
    file_.write(count_str, 10);

    file_.close();
    return true;
  }

private:
  std::ofstream file_;
  std::streampos data_start_pos_;
};

// Streaming PCD writer
class StreamingPCDWriter : public StreamingWriter {
public:
  bool open(const std::string& filename) override {
    file_.open(filename, std::ios::binary);
    if (!file_.is_open()) return false;

    // Write header with placeholder
    file_ << "# .PCD v0.7 - Point Cloud Data file format\n";
    file_ << "VERSION 0.7\n";
    file_ << "FIELDS x y z\n";
    file_ << "SIZE 4 4 4\n";
    file_ << "TYPE F F F\n";
    file_ << "COUNT 1 1 1\n";
    file_ << "WIDTH 0000000000\n";  // 10-digit placeholder
    file_ << "HEIGHT 1\n";
    file_ << "VIEWPOINT 0 0 0 1 0 0 0\n";
    file_ << "POINTS 0000000000\n";  // 10-digit placeholder
    file_ << "DATA binary\n";

    data_start_pos_ = file_.tellp();
    return true;
  }

  bool write_point(const Eigen::Vector3f& point) override {
    if (!file_.is_open()) return false;
    file_.write(reinterpret_cast<const char*>(point.data()), 3 * sizeof(float));
    total_points_++;
    return true;
  }

  bool close() override {
    if (!file_.is_open()) return false;

    file_.flush();

    // Update point counts in header
    char count_str[11];
    snprintf(count_str, sizeof(count_str), "%010zu", total_points_);

    // Update WIDTH (position 108)
    file_.seekp(108);
    file_.write(count_str, 10);

    // Update POINTS (position 159)
    file_.seekp(159);
    file_.write(count_str, 10);

    file_.close();
    return true;
  }

private:
  std::ofstream file_;
  std::streampos data_start_pos_;
};

// Streaming LAS writer using libLAS
class StreamingLASWriter : public StreamingWriter {
public:
  bool open(const std::string& filename) override {
    filename_ = filename;

    // Create output stream
    ofs_ = std::make_unique<std::ofstream>();
    ofs_->open(filename, std::ios::out | std::ios::binary);
    if (!ofs_->is_open()) return false;

    // Set up LAS header
    liblas::Header header;
    header.SetVersionMinor(2);  // LAS 1.2
    header.SetDataFormatId(liblas::ePointFormat0);  // Simple XYZ format
    header.SetScale(0.001, 0.001, 0.001);  // 1mm precision
    header.SetOffset(0.0, 0.0, 0.0);

    // Create writer
    writer_ = std::make_unique<liblas::Writer>(*ofs_, header);

    return true;
  }

  bool write_point(const Eigen::Vector3f& point) override {
    if (!writer_) return false;

    liblas::Point las_point(&writer_->GetHeader());
    las_point.SetCoordinates(point.x(), point.y(), point.z());

    writer_->WritePoint(las_point);
    total_points_++;
    return true;
  }

  bool close() override {
    // libLAS writer updates header automatically on destruction
    writer_.reset();
    ofs_.reset();
    return true;
  }

private:
  std::string filename_;
  std::unique_ptr<std::ofstream> ofs_;
  std::unique_ptr<liblas::Writer> writer_;
};

// Streaming LAZ writer using lazperf
class StreamingLAZWriter : public StreamingWriter {
public:
  bool open(const std::string& filename) override {
    filename_ = filename;

    // Set up lazperf configuration
    lazperf::writer::named_file::config config;
    config.scale = lazperf::vector3(0.001, 0.001, 0.001);  // 1mm precision
    config.offset = lazperf::vector3(0.0, 0.0, 0.0);
    config.pdrf = 0;  // Point format 0 (XYZ only)
    config.minor_version = 2;  // LAS 1.2
    config.chunk_size = 50000;  // Chunk size for compression

    // Create writer
    writer_ = std::make_unique<lazperf::writer::named_file>(filename, config);

    return true;
  }

  bool write_point(const Eigen::Vector3f& point) override {
    if (!writer_) return false;

    // lazperf point format 0: X (i32), Y (i32), Z (i32), intensity (u16),
    // return info (u8), classification (u8), scan angle (i8),
    // user data (u8), point source ID (u16)
    // Total: 20 bytes for format 0

    // Scale point coordinates to integers
    int32_t x = static_cast<int32_t>(point.x() / 0.001);
    int32_t y = static_cast<int32_t>(point.y() / 0.001);
    int32_t z = static_cast<int32_t>(point.z() / 0.001);

    // Build point data (20 bytes for format 0)
    char point_data[20] = {0};
    std::memcpy(point_data + 0, &x, 4);
    std::memcpy(point_data + 4, &y, 4);
    std::memcpy(point_data + 8, &z, 4);
    // Rest is zeros (intensity, return info, classification, etc.)

    writer_->writePoint(point_data);
    total_points_++;
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
  std::string filename_;
  std::unique_ptr<lazperf::writer::named_file> writer_;
};
