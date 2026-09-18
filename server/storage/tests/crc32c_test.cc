/**
 * @file server/storage/tests/crc32c_test.cc
 * The checksum against published vectors, and the hardware and table
 * implementations against each other.
 */

#include "wal/crc32c.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "wal/crc32c_internal.h"

namespace {

using helios::storage::wal::Crc32c;
using helios::storage::wal::internal::HasSse42;
using helios::storage::wal::internal::UpdateWithTable;

constexpr uint32_t kInitialState = 0xffffffffu;
constexpr uint32_t kFinalXor = 0xffffffffu;

uint32_t Compute(const void *data, size_t size) {
  Crc32c crc;
  crc.Update(data, size);
  return crc.Finish();
}

uint32_t ComputeWithTable(const void *data, size_t size) {
  return UpdateWithTable(kInitialState, data, size) ^ kFinalXor;
}

std::vector<uint8_t> RandomBytes(std::mt19937 &rng, size_t size) {
  std::uniform_int_distribution<int> byte_dist(0, 255);
  std::vector<uint8_t> data(size);
  for (auto &b : data) b = static_cast<uint8_t>(byte_dist(rng));
  return data;
}

struct KnownAnswer {
  std::string input;
  uint32_t crc;
};

// The check value, the RFC 3720 appendix B.4 vectors, and one text. An IEEE
// CRC-32 (zlib) implementation fails every nonempty vector.
std::vector<KnownAnswer> KnownAnswers() {
  std::string ascending(32, '\0');
  std::string descending(32, '\0');
  for (int i = 0; i < 32; ++i) {
    ascending[i] = static_cast<char>(i);
    descending[i] = static_cast<char>(31 - i);
  }
  return {
      {"", 0x00000000u},
      {"123456789", 0xe3069283u},
      {"The quick brown fox jumps over the lazy dog", 0x22620404u},
      {std::string(32, '\0'), 0x8a9136aau},
      {std::string(32, '\xff'), 0x62a8ab43u},
      {ascending, 0x46dd794eu},
      {descending, 0x113fdb5cu},
  };
}

TEST(Crc32cTest, KnownAnswersThroughTheClass) {
  for (const auto &[input, crc] : KnownAnswers()) {
    EXPECT_EQ(crc, Compute(input.data(), input.size())) << input;
  }
}

TEST(Crc32cTest, KnownAnswersThroughTheTable) {
  for (const auto &[input, crc] : KnownAnswers()) {
    EXPECT_EQ(crc, ComputeWithTable(input.data(), input.size())) << input;
  }
}

TEST(Crc32cTest, NullDataWithZeroSize) {
  EXPECT_EQ(0x00000000u, Compute(nullptr, 0));
  EXPECT_EQ(0x00000000u, ComputeWithTable(nullptr, 0));
}

TEST(Crc32cTest, SplitUpdatesEqualOneShot) {
  const std::string data = "The quick brown fox jumps over the lazy dog";
  for (size_t split = 0; split <= data.size(); ++split) {
    Crc32c crc;
    crc.Update(data.data(), split);
    crc.Update(data.data() + split, data.size() - split);
    EXPECT_EQ(0x22620404u, crc.Finish()) << "split=" << split;
  }
}

TEST(Crc32cTest, ChunkedUpdateMatchesOneShot) {
  std::mt19937 rng(12345);
  const std::vector<uint8_t> data = RandomBytes(rng, 4096 * 3 + 17);

  // Chunk sizes crossing and misaligned with the hardware path's 8-byte
  // stride, so the seam between calls is exercised on both code paths.
  const size_t chunk_sizes[] = {1, 3, 8, 251, 4096};
  Crc32c chunked;
  size_t offset = 0;
  size_t chunk_index = 0;
  while (offset < data.size()) {
    const size_t chunk =
        std::min(chunk_sizes[chunk_index % std::size(chunk_sizes)],
                 data.size() - offset);
    chunked.Update(data.data() + offset, chunk);
    offset += chunk;
    ++chunk_index;
  }
  EXPECT_EQ(Compute(data.data(), data.size()), chunked.Finish());
}

#if defined(__x86_64__)
TEST(Crc32cTest, TableAndHardwarePathsAgree) {
  if (!HasSse42()) GTEST_SKIP() << "SSE4.2 not available on this CPU";
  using helios::storage::wal::internal::UpdateWithSse42;

  std::mt19937 rng(67890);
  // Every remainder mod the hardware path's 8-byte stride, up through a few
  // strides, plus larger sizes further from that boundary; each size at every
  // start alignment.
  std::vector<size_t> sizes;
  for (size_t size = 0; size <= 23; ++size) sizes.push_back(size);
  for (const size_t size : {63u, 64u, 65u, 4096u, 100003u}) {
    sizes.push_back(size);
  }
  for (const size_t size : sizes) {
    const std::vector<uint8_t> buffer = RandomBytes(rng, size + 8);
    for (size_t offset = 0; offset < 8; ++offset) {
      const uint8_t *data = buffer.data() + offset;
      const uint32_t table = UpdateWithTable(kInitialState, data, size);
      const uint32_t sse42 = UpdateWithSse42(kInitialState, data, size);
      EXPECT_EQ(table, sse42) << "size=" << size << " offset=" << offset;
      EXPECT_EQ(table ^ kFinalXor, Compute(data, size))
          << "size=" << size << " offset=" << offset;
    }
  }
}
#endif

}  // namespace
