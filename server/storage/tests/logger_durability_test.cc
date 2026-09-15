/**
 * @file server/storage/tests/logger_durability_test.cc
 * What the log reports as persisted, when a waiter wakes, and what a
 * synchronous acknowledgement waits for.
 */

#include <errno.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "wal/log_entry.h"
#include "wal/logger.h"
#include "wal/wal.h"

namespace {

using helios::storage::EpochNumber;
using helios::storage::wal::LogEntry;
using helios::storage::wal::Logger;
using helios::storage::wal::WalIo;
using helios::storage::wal::LogEntries;

constexpr auto kTestTimeout = std::chrono::seconds(5);

// Exercises the logger without constructing a Database: the WAL and the
// durable epoch are the units under test here, and a Database would
// drag in the epoch framework.
class LoggerDurabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_walger_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    config_.work_dir = root_ + "/logs";
    // Every fixture writes out its capacity before its first group; these
    // logs hold a handful of frames.
    config_.wal_initial_capacity_bytes = 1ull << 20;
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  static LogEntries MakePrimaryLogEntries(const std::string &key) {
    LogEntry entry(key, nullptr, 0, nullptr, "t", "");
    LogEntries log_entries;
    log_entries.emplace_back(std::move(entry));
    return log_entries;
  }

  // Secondary log entries with no delta persist nothing.
  static LogEntries MakeEmptySecondaryLogEntries(const std::string &key) {
    LogEntry entry(key, nullptr, 0, nullptr, "t", "idx");
    LogEntries log_entries;
    log_entries.emplace_back(std::move(entry));
    return log_entries;
  }

  std::string root_;
  helios::storage::Config config_;
};

TEST_F(LoggerDurabilityTest, EnqueueReportsOnlyWhatItPersists) {
  Logger logger(config_);
  EXPECT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 5));
  EXPECT_FALSE(logger.Enqueue(LogEntries{}, 5));
  EXPECT_FALSE(logger.Enqueue(MakeEmptySecondaryLogEntries("bob"), 5));
}

TEST_F(LoggerDurabilityTest, AlreadyDurableReturnsImmediately) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 5));
  logger.RequestFlush(5);

  EXPECT_EQ(logger.WaitUntilDurable(5, Logger::Deadline::max()),
            Logger::WaitResult::kDurable);
  EXPECT_EQ(logger.GetDurableEpoch(), 5u);
  // A second wait on an epoch already durable must not block at all.
  EXPECT_EQ(logger.WaitUntilDurable(5, std::chrono::steady_clock::now()),
            Logger::WaitResult::kDurable);
  logger.Stop();
}

TEST_F(LoggerDurabilityTest, WaitersWakeAtEpochGranularity) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 5));
  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("bob"), 7));

  auto spawn_waiter = [&logger](EpochNumber epoch) {
    return std::async(std::launch::async, [&logger, epoch] {
      return logger.WaitUntilDurable(epoch, Logger::Deadline::max());
    });
  };
  auto first = spawn_waiter(5);
  auto second = spawn_waiter(5);
  auto later = spawn_waiter(7);

  logger.RequestFlush(5);
  ASSERT_EQ(first.wait_for(kTestTimeout), std::future_status::ready);
  ASSERT_EQ(second.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(first.get(), Logger::WaitResult::kDurable);
  EXPECT_EQ(second.get(), Logger::WaitResult::kDurable);
  // Epoch 7 is not covered by a flush through 5.
  EXPECT_EQ(later.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);

  logger.RequestFlush(7);
  ASSERT_EQ(later.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(later.get(), Logger::WaitResult::kDurable);
  logger.Stop();
}

// The acknowledgement a Sync commit waits for cannot be given while the
// fdatasync that would earn it is still running. Holding the syscall makes the
// order observable rather than merely likely.
TEST_F(LoggerDurabilityTest, SyncAcknowledgementFollowsTheFdatasync) {
  std::mutex mutex;
  std::condition_variable held;
  bool inside_fdatasync = false;
  bool released = false;

  WalIo io = WalIo::Posix();
  auto posix_fdatasync = io.fdatasync;
  io.fdatasync = [&](int fd) {
    std::unique_lock<std::mutex> lock(mutex);
    inside_fdatasync = true;
    held.notify_all();
    held.wait(lock, [&] { return released; });
    lock.unlock();
    return posix_fdatasync(fd);
  };

  Logger logger(config_, io);
  std::atomic<bool> committer_started{false};
  std::future<void> committer;

  // Releases a held fdatasync and drains the logger worker on every exit:
  // an assertion failure would otherwise leave the committer waiting forever,
  // and the future's destructor would block before ~Logger could wake it.
  // Declared after the future so unwinding runs the guard first; the drain
  // wakes the committer with either the durable epoch or the stopped state.
  struct ReleaseOnExit {
    Logger &logger;
    std::mutex &mutex;
    std::condition_variable &held;
    bool &released;
    ~ReleaseOnExit() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
      }
      held.notify_all();
      logger.Stop();
    }
  } release_on_exit{logger, mutex, held, released};

  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 3));
  committer = std::async(std::launch::async, [&logger, &committer_started] {
    committer_started.store(true);
    logger.AwaitCommitDurability(3, true);
  });
  // The timeout probe below measures the wait, not thread startup: flush only
  // once the committer thread is provably running.
  while (!committer_started.load()) std::this_thread::yield();
  logger.RequestFlush(3);

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(
        held.wait_for(lock, kTestTimeout, [&] { return inside_fdatasync; }));
  }
  EXPECT_EQ(committer.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
  }
  held.notify_all();

  ASSERT_EQ(committer.wait_for(kTestTimeout), std::future_status::ready);
  committer.get();
  EXPECT_EQ(logger.GetDurableEpoch(), 3u);
  logger.Stop();
}

// A caller that decided not to wait returns at once, whether because the
// contract is Async or because the transaction left no record.
TEST_F(LoggerDurabilityTest, AsyncAndUnloggedCommitsDoNotWait) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
    logger.Start();

    // Async: the commit path decided not to wait, and an epoch that will
    // never be flushed still returns at once.
    logger.AwaitCommitDurability(99, false);
    EXPECT_EQ(logger.GetDurableEpoch(), 0u);
    logger.Stop();
  }

  // The log is held exclusively for as long as a logger owns it, so the
  // second logger gets its own scope rather than overlapping with the first.
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  // Sync, but nothing was enqueued: there is no record to wait for.
  logger.AwaitCommitDurability(99, false);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);
  logger.Stop();
}

// With the fail-stop armed, a write failure ends the process by abort: under
// Async nobody waits on the durable epoch, and a process that carried on would
// keep acknowledging commits that exist only in memory. The rest of this file
// constructs loggers without arming: there an I/O failure surfaces as
// WaitResult::kFailed instead of ending the process.
TEST_F(LoggerDurabilityTest, ArmedFailStopEndsTheProcessOnFdatasyncFailure) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  WalIo io = WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  // A worker started in the parent would not survive the death-test fork.
  // Create and arm the logger in the child, as Database does before Start.
  EXPECT_EXIT(
      {
        Logger logger(config_, io);
        // Leaving early ends the child with an exit status the death test
        // reports, rather than passing on a failure of the setup.
        if (logger.Recover().status != Logger::RecoveryStatus::kOk) return;
        logger.SetFailStop();
        logger.Start();
        if (!logger.Enqueue(MakePrimaryLogEntries("alice"), 3)) return;
        logger.RequestFlush(3);
        std::this_thread::sleep_for(kTestTimeout);
      },
      ::testing::KilledBySignal(SIGABRT), "");
}

TEST_F(LoggerDurabilityTest, RecordsAboveTheTargetAreCarriedForward) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
    logger.Start();
    ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 4));
    ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("bob"), 9));
    logger.RequestFlush(4);
    ASSERT_EQ(logger.WaitUntilDurable(4, Logger::Deadline::max()),
              Logger::WaitResult::kDurable);
    logger.RequestFlush(9);
    ASSERT_EQ(logger.WaitUntilDurable(9, Logger::Deadline::max()),
              Logger::WaitResult::kDurable);
    logger.Stop();
  }

  // Both epochs must be present, in order, after reopening.
  helios::storage::wal::Wal wal(config_.work_dir,
                                helios::storage::wal::WalIo::Posix(),
                                config_.wal_initial_capacity_bytes);
  const auto scan = wal.Scan();
  ASSERT_EQ(scan.status, helios::storage::wal::WalScanResult::Status::kOk);
  EXPECT_EQ(scan.last_epoch, 9u);
  ASSERT_EQ(scan.records.size(), 2u);
  EXPECT_EQ(scan.records[0].epoch, 4u);
  EXPECT_EQ(scan.records[1].epoch, 9u);
}

TEST_F(LoggerDurabilityTest, StopWakesEveryWaiter) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  auto first = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(11, Logger::Deadline::max());
  });
  auto second = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(12, Logger::Deadline::max());
  });
  // Both are inside the wait, rather than merely started, when the stop
  // arrives: neither epoch is durable, so neither may be ready.
  ASSERT_EQ(first.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  ASSERT_EQ(second.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);

  logger.Stop();
  ASSERT_EQ(first.wait_for(kTestTimeout), std::future_status::ready);
  ASSERT_EQ(second.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(first.get(), Logger::WaitResult::kStopped);
  EXPECT_EQ(second.get(), Logger::WaitResult::kStopped);
}

TEST_F(LoggerDurabilityTest,
       FdatasyncFailureHoldsTheDurableEpochAndFailsWaiters) {
  WalIo io = WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Logger logger(config_, io);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 3));
  auto waiting = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(3, Logger::Deadline::max());
  });
  ASSERT_EQ(waiting.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);

  logger.RequestFlush(3);
  ASSERT_EQ(waiting.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(waiting.get(), Logger::WaitResult::kFailed);
  // The durable epoch must not move: durability was never confirmed, whatever
  // bytes may have landed.
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  // A waiter arriving after the failure learns of it rather than blocking.
  EXPECT_EQ(logger.WaitUntilDurable(3, Logger::Deadline::max()),
            Logger::WaitResult::kFailed);
  logger.Stop();
}

TEST_F(LoggerDurabilityTest, WriteFailureFailsWaiters) {
  WalIo io = WalIo::Posix();
  io.pwrite = [](int, const void *, size_t, off_t) -> ssize_t {
    errno = EIO;
    return -1;
  };

  Logger logger(config_, io);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 3));
  logger.RequestFlush(3);
  EXPECT_EQ(logger.WaitUntilDurable(3, Logger::Deadline::max()),
            Logger::WaitResult::kFailed);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);
  logger.Stop();
}

TEST_F(LoggerDurabilityTest, TimeoutIsReportedWhenNothingIsScheduled) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
  logger.Start();

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  EXPECT_EQ(logger.WaitUntilDurable(42, deadline),
            Logger::WaitResult::kTimedOut);
  logger.Stop();
}

TEST_F(LoggerDurabilityTest, StopDrainsWhatWasAlreadyClosed) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
    logger.Start();
    ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 6));
    logger.RequestFlush(6);
    // Stop without waiting: the drain must still write epoch 6.
    logger.Stop();
    EXPECT_EQ(logger.GetDurableEpoch(), 6u);
  }

  helios::storage::wal::Wal wal(config_.work_dir,
                                helios::storage::wal::WalIo::Posix(),
                                config_.wal_initial_capacity_bytes);
  const auto scan = wal.Scan();
  ASSERT_EQ(scan.status, helios::storage::wal::WalScanResult::Status::kOk);
  EXPECT_EQ(scan.last_epoch, 6u);
}

TEST_F(LoggerDurabilityTest, RecoverReportsTheDurableEpochOfAnExistingLog) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::kOk);
    logger.Start();
    ASSERT_TRUE(logger.Enqueue(MakePrimaryLogEntries("alice"), 8));
    logger.RequestFlush(8);
    ASSERT_EQ(logger.WaitUntilDurable(8, Logger::Deadline::max()),
              Logger::WaitResult::kDurable);
    logger.Stop();
  }

  Logger reopened(config_);
  const auto recovered = reopened.Recover();
  ASSERT_EQ(recovered.status, Logger::RecoveryStatus::kOk);
  EXPECT_EQ(recovered.durable_epoch, 8u);
  EXPECT_EQ(reopened.GetDurableEpoch(), 8u);
  ASSERT_EQ(recovered.recovery_entries.size(), 1u);
  EXPECT_EQ(recovered.recovery_entries[0].key, "alice");
}

}  // namespace
