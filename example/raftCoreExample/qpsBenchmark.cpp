#include "clerk.h"
#include "qpsBenchmarkLogic.h"

#include <getopt.h>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {

struct BenchmarkConfig {
  std::string configFile;
  std::uint64_t totalOperations = 0;
  std::size_t clients = 1;
  std::size_t keySpace = 100;
  std::uint32_t getWeight = 1;
  std::uint32_t putWeight = 1;
  std::uint32_t appendWeight = 1;
  int timeoutMs = 1000;
  std::uint64_t seed = static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
};

struct WorkerStats {
  std::uint64_t total = 0;
  std::uint64_t success = 0;
  std::uint64_t notFound = 0;
  std::uint64_t failed = 0;
  std::uint64_t get = 0;
  std::uint64_t put = 0;
  std::uint64_t append = 0;
  std::chrono::nanoseconds operationTime{};
};

enum class ParseStatus { Valid, Invalid, Help };

void PrintUsage(const char* program) {
  std::cout << "用法：" << program << " -f 配置文件 -n 操作总数 [选项]\n"
            << "选项：\n"
            << "  -f <文件>  Raft 节点配置文件（必填）\n"
            << "  -n <数量>  所有客户端合计执行的操作数（必填）\n"
            << "  -c <数量>  并发客户端数，默认 1\n"
            << "  -k <数量>  Key 数量，默认 100\n"
            << "  -g <权重>  Get 权重，默认 1\n"
            << "  -p <权重>  Put 权重，默认 1\n"
            << "  -a <权重>  Append 权重，默认 1\n"
            << "  -t <毫秒>  单次操作超时，默认 1000\n"
            << "  -s <种子>  随机种子，默认使用当前时间\n"
            << "  -h         显示帮助\n";
}

template <typename T>
bool ParseUnsigned(const char* text, T* value) {
  if (text == nullptr || *text == '\0' || *text == '-') {
    return false;
  }
  const char* end = text + std::char_traits<char>::length(text);
  T parsed{};
  const auto result = std::from_chars(text, end, parsed);
  if (result.ec != std::errc{} || result.ptr != end) {
    return false;
  }
  *value = parsed;
  return true;
}

bool ParsePositiveInt(const char* text, int* value) {
  std::uint32_t parsed = 0;
  if (!ParseUnsigned(text, &parsed) || parsed == 0 ||
      parsed > static_cast<std::uint32_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  *value = static_cast<int>(parsed);
  return true;
}

ParseStatus ParseArguments(int argc, char** argv, BenchmarkConfig* config) {
  bool valid = true;
  int option = 0;
  while ((option = getopt(argc, argv, "f:n:c:k:g:p:a:t:s:h")) != -1) {
    switch (option) {
      case 'f':
        config->configFile = optarg == nullptr ? "" : optarg;
        break;
      case 'n':
        valid = ParseUnsigned(optarg, &config->totalOperations) && valid;
        break;
      case 'c':
        valid = ParseUnsigned(optarg, &config->clients) && valid;
        break;
      case 'k':
        valid = ParseUnsigned(optarg, &config->keySpace) && valid;
        break;
      case 'g':
        valid = ParseUnsigned(optarg, &config->getWeight) && valid;
        break;
      case 'p':
        valid = ParseUnsigned(optarg, &config->putWeight) && valid;
        break;
      case 'a':
        valid = ParseUnsigned(optarg, &config->appendWeight) && valid;
        break;
      case 't':
        valid = ParsePositiveInt(optarg, &config->timeoutMs) && valid;
        break;
      case 's':
        valid = ParseUnsigned(optarg, &config->seed) && valid;
        break;
      case 'h':
        PrintUsage(argv[0]);
        return ParseStatus::Help;
      default:
        return ParseStatus::Invalid;
    }
  }

  const auto totalWeight = static_cast<std::uint64_t>(config->getWeight) + config->putWeight + config->appendWeight;
  return valid && optind == argc && !config->configFile.empty() && config->totalOperations > 0 && config->clients > 0 &&
                 config->keySpace > 0 && config->timeoutMs > 0 && totalWeight > 0
             ? ParseStatus::Valid
             : ParseStatus::Invalid;
}

void RecordOperationType(qps::OperationType type, WorkerStats* stats) {
  switch (type) {
    case qps::OperationType::Get:
      ++stats->get;
      break;
    case qps::OperationType::Put:
      ++stats->put;
      break;
    case qps::OperationType::Append:
      ++stats->append;
      break;
  }
}

void RecordResult(const ClerkOperationResult& result, WorkerStats* stats) {
  switch (result.status) {
    case ClerkOperationStatus::Success:
      ++stats->success;
      break;
    case ClerkOperationStatus::NotFound:
      ++stats->notFound;
      break;
    case ClerkOperationStatus::Failed:
      ++stats->failed;
      break;
  }
}

WorkerStats RunWorker(const BenchmarkConfig& config, std::size_t workerId, std::atomic<std::uint64_t>* nextOperation) {
  WorkerStats stats;
  qps::WorkloadGenerator generator(config.seed, workerId, config.keySpace, config.getWeight, config.putWeight,
                                   config.appendWeight);

  try {
    Clerk clerk;
    clerk.Init(config.configFile);
    while (true) {
      const auto operationId = nextOperation->fetch_add(1, std::memory_order_relaxed);
      if (operationId >= config.totalOperations) {
        break;
      }

      const auto operation = generator.Next();
      RecordOperationType(operation.type, &stats);
      ++stats.total;
      const auto start = std::chrono::steady_clock::now();
      ClerkOperationResult result;
      try {
        switch (operation.type) {
          case qps::OperationType::Get:
            result = clerk.GetWithTimeout(operation.key, config.timeoutMs);
            break;
          case qps::OperationType::Put:
            result = clerk.PutWithTimeout(operation.key, operation.value, config.timeoutMs);
            break;
          case qps::OperationType::Append:
            result = clerk.AppendWithTimeout(operation.key, operation.value, config.timeoutMs);
            break;
        }
      } catch (...) {
        result = {};
      }
      stats.operationTime += std::chrono::steady_clock::now() - start;
      RecordResult(result, &stats);
    }
    return stats;
  } catch (const std::exception& error) {
    std::cerr << "工作线程初始化失败：" << error.what() << '\n';
  } catch (...) {
    std::cerr << "工作线程初始化失败：未知异常\n";
  }

  while (true) {
    const auto operationId = nextOperation->fetch_add(1, std::memory_order_relaxed);
    if (operationId >= config.totalOperations) {
      break;
    }
    const auto operation = generator.Next();
    RecordOperationType(operation.type, &stats);
    ++stats.total;
    ++stats.failed;
  }
  return stats;
}

void MergeStats(const WorkerStats& source, WorkerStats* target) {
  target->total += source.total;
  target->success += source.success;
  target->notFound += source.notFound;
  target->failed += source.failed;
  target->get += source.get;
  target->put += source.put;
  target->append += source.append;
  target->operationTime += source.operationTime;
}

void PrintReport(const BenchmarkConfig& config, const WorkerStats& stats,
                 std::chrono::steady_clock::duration wallDuration) {
  const double wallSeconds = std::chrono::duration<double>(wallDuration).count();
  const double averageMs = stats.total == 0 ? 0.0
                                            : std::chrono::duration<double, std::milli>(stats.operationTime).count() /
                                                  static_cast<double>(stats.total);
  const double totalQps = wallSeconds == 0.0 ? 0.0 : static_cast<double>(stats.total) / wallSeconds;
  const double successQps = wallSeconds == 0.0 ? 0.0 : static_cast<double>(stats.success) / wallSeconds;
  const double wallMs = std::chrono::duration<double, std::milli>(wallDuration).count();

  std::cout << std::fixed << std::setprecision(2) << "========== Raft KV 压测结果 ==========\n"
            << "配置文件：" << config.configFile << '\n'
            << "并发客户端：" << config.clients << '\n'
            << "操作总数：" << stats.total << '\n'
            << "成功操作数：" << stats.success << '\n'
            << "Get 未命中数：" << stats.notFound << '\n'
            << "失败操作数：" << stats.failed << "\n\n"
            << "总耗时：" << wallMs << " ms\n"
            << "平均请求耗时：" << averageMs << " ms/次\n"
            << "总吞吐：" << totalQps << " QPS\n"
            << "成功吞吐：" << successQps << " QPS\n\n"
            << "操作分布：\n"
            << "  Get：" << stats.get << '\n'
            << "  Put：" << stats.put << '\n'
            << "  Append：" << stats.append << '\n'
            << "=======================================\n";
}

}  // namespace

int main(int argc, char** argv) {
  BenchmarkConfig config;
  const auto parseStatus = ParseArguments(argc, argv, &config);
  if (parseStatus == ParseStatus::Help) {
    return EXIT_SUCCESS;
  }
  if (parseStatus == ParseStatus::Invalid) {
    std::cerr << "参数错误：请提供有效的配置文件、操作总数和正数参数。\n";
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  std::atomic<std::uint64_t> nextOperation{0};
  std::vector<WorkerStats> workerStats(config.clients);
  std::vector<std::thread> workers;
  workers.reserve(config.clients);

  const auto begin = std::chrono::steady_clock::now();
  for (std::size_t workerId = 0; workerId < config.clients; ++workerId) {
    workers.emplace_back([&, workerId] { workerStats[workerId] = RunWorker(config, workerId, &nextOperation); });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  const auto finish = std::chrono::steady_clock::now();

  WorkerStats totalStats;
  for (const auto& stats : workerStats) {
    MergeStats(stats, &totalStats);
  }
  if (totalStats.total != config.totalOperations ||
      totalStats.success + totalStats.notFound + totalStats.failed != totalStats.total ||
      totalStats.get + totalStats.put + totalStats.append != totalStats.total) {
    std::cerr << "统计错误：操作总数或结果分类不一致。\n";
    return EXIT_FAILURE;
  }

  PrintReport(config, totalStats, finish - begin);
  return EXIT_SUCCESS;
}
