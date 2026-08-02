# Raft KV QPS Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a C++20 `qpsBenchmark` executable that drives the existing Raft KV client with varied mixed read/write inputs, bounded per-operation retries, configurable concurrency, fixed total work, and a Chinese throughput report.

**Architecture:** Keep the existing blocking `Clerk` API unchanged for current examples. Add a result-returning, deadline-aware path through `raftServerRpcUtil` and `MprpcChannel`; then keep workload generation/statistics independent from RPC so they can be compiled and tested without a live cluster. The benchmark creates one Clerk and one workload generator per worker thread while an atomic counter enforces the global operation limit.

**Tech Stack:** C++20, CMake, POSIX sockets, Protobuf-generated KV RPC stubs, `std::thread`, `std::atomic`, `std::chrono`, and the repository's existing `Clerk`/RPC code.

---

## File Map

- Create: `example/raftCoreExample/include/qpsBenchmarkLogic.h` - testable operation types and deterministic varied-input generator.
- Create: `example/raftCoreExample/qpsBenchmarkLogic.cpp` - generator implementation with weighted operation selection.
- Create: `example/raftCoreExample/qpsBenchmark.cpp` - CLI, worker threads, Clerk calls, result aggregation, and Chinese report.
- Create: `test/qps_benchmark_logic.cpp` - standalone assertions for generator determinism, input variation, key-space bounds, and operation weights.
- Modify: `example/raftCoreExample/CMakeLists.txt` - add the `qpsBenchmark` target.
- Modify: `src/rpc/include/mprpcchannel.h` and `src/rpc/mprpcchannel.cpp` - optional per-call socket timeout and timeout-aware connection/send/receive failures.
- Modify: `src/raftClerk/include/raftServerRpcUtil.h` and `src/raftClerk/raftServerRpcUtil.cpp` - own the channel explicitly and pass the remaining deadline to each KV RPC.
- Modify: `src/raftClerk/include/clerk.h` and `src/raftClerk/clerk.cpp` - add result statuses and bounded retry methods while preserving existing methods.
- Do not modify: `src/raftRpcPro/*.proto`, generated `*.pb.h`, or generated `*.pb.cc` files.

## Task 1: Add a Testable Workload Generator

**Files:**

- Create: `test/qps_benchmark_logic.cpp`
- Create: `example/raftCoreExample/include/qpsBenchmarkLogic.h`
- Create: `example/raftCoreExample/qpsBenchmarkLogic.cpp`

- [ ] **Step 1: Write the failing generator test**

Create a standalone test that intentionally includes the not-yet-created generator API and checks the agreed behavior:

```cpp
#include "qpsBenchmarkLogic.h"

#include <cassert>
#include <set>
#include <string>
#include <vector>

int main() {
  qps::WorkloadGenerator first(42, 0, 16, 1, 1, 1);
  qps::WorkloadGenerator second(42, 0, 16, 1, 1, 1);
  std::set<std::string> keys;
  std::set<std::string> values;
  std::vector<qps::GeneratedOperation> firstRun;

  for (int i = 0; i < 64; ++i) {
    auto left = first.Next();
    auto right = second.Next();
    assert(left.type == right.type);
    assert(left.key == right.key);
    assert(left.value == right.value);
    assert(left.key.rfind("key_", 0) == 0);
    assert(left.key != "key_16");
    keys.insert(left.key);
    values.insert(left.value);
    firstRun.push_back(left);
  }

  assert(keys.size() > 1);
  assert(values.size() > 1);

  bool sawGet = false;
  bool sawPut = false;
  bool sawAppend = false;
  for (const auto& operation : firstRun) {
    sawGet = sawGet || operation.type == qps::OperationType::Get;
    sawPut = sawPut || operation.type == qps::OperationType::Put;
    sawAppend = sawAppend || operation.type == qps::OperationType::Append;
  }
  assert(sawGet && sawPut && sawAppend);
  return 0;
}
```

- [ ] **Step 2: Run the test and verify it fails for the intended reason**

Run:

```bash
g++ -std=c++20 \
  -Iexample/raftCoreExample/include \
  example/raftCoreExample/qpsBenchmarkLogic.cpp \
  test/qps_benchmark_logic.cpp \
  -o /tmp/qps_benchmark_logic
```

Expected: compilation fails because `qpsBenchmarkLogic.h` and the generator implementation do not exist yet.

- [ ] **Step 3: Define the small generator interface**

Create `example/raftCoreExample/include/qpsBenchmarkLogic.h` with no RPC dependency:

```cpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>

namespace qps {

enum class OperationType { Get, Put, Append };

struct GeneratedOperation {
  OperationType type;
  std::string key;
  std::string value;
};

class WorkloadGenerator {
 public:
  WorkloadGenerator(std::uint64_t seed, std::size_t workerId, std::size_t keySpace, std::uint32_t getWeight,
                    std::uint32_t putWeight, std::uint32_t appendWeight);

  GeneratedOperation Next();

 private:
  std::mt19937_64 m_random;
  std::size_t m_workerId;
  std::size_t m_sequence;
  std::discrete_distribution<int> m_operation;
  std::uniform_int_distribution<std::size_t> m_key;
};

}  // namespace qps
```

- [ ] **Step 4: Implement deterministic weighted generation with varied data**

Create `example/raftCoreExample/qpsBenchmarkLogic.cpp`. Derive each worker's seed from the global seed and worker ID, select operations with `std::discrete_distribution`, keep keys inside `[0, keySpace - 1]`, and include worker/sequence/random data in every value:

```cpp
#include "qpsBenchmarkLogic.h"

#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace qps {

WorkloadGenerator::WorkloadGenerator(std::uint64_t seed, std::size_t workerId, std::size_t keySpace,
                                     std::uint32_t getWeight, std::uint32_t putWeight,
                                     std::uint32_t appendWeight)
    : m_random(seed ^ (0x9e3779b97f4a7c15ULL + workerId + (seed << 6) + (seed >> 2))),
      m_workerId(workerId),
      m_sequence(0),
      m_operation({static_cast<double>(getWeight), static_cast<double>(putWeight),
                   static_cast<double>(appendWeight)}),
      m_key(0, keySpace == 0 ? 0 : keySpace - 1) {
  if (keySpace == 0 || getWeight + putWeight + appendWeight == 0) {
    throw std::invalid_argument("key space and operation weights must be positive");
  }
}

GeneratedOperation WorkloadGenerator::Next() {
  const auto selected = m_operation(m_random);
  const auto keyIndex = m_key(m_random);
  const auto sequence = m_sequence++;
  const auto randomPart = m_random();

  const auto type = static_cast<OperationType>(selected);

  std::ostringstream value;
  value << "value_t" << m_workerId << '_' << sequence << '_' << std::hex << randomPart;
  return {type, "key_" + std::to_string(keyIndex), value.str()};
}

}  // namespace qps
```

The stored `std::discrete_distribution<int>` uses `{getWeight, putWeight, appendWeight}` directly, so `selected == 0`, `1`, and `2` map to `Get`, `Put`, and `Append`. Each `Next()` call consumes only this worker's random engine.

- [ ] **Step 5: Run the focused test and commit the generator slice**

Run:

```bash
g++ -std=c++20 \
  -Iexample/raftCoreExample/include \
  example/raftCoreExample/qpsBenchmarkLogic.cpp \
  test/qps_benchmark_logic.cpp \
  -o /tmp/qps_benchmark_logic
/tmp/qps_benchmark_logic
```

Expected: the process exits with status 0 and produces no assertion failure. Commit the three generator/test files with a focused message such as `添加 QPS 压测负载生成器`.

## Task 2: Add Optional Deadline-Aware Socket Calls

**Files:**

- Modify: `src/rpc/include/mprpcchannel.h`
- Modify: `src/rpc/mprpcchannel.cpp`

- [ ] **Step 1: Add a backward-compatible timeout API**

Extend the existing channel declaration without changing existing constructor calls:

```cpp
class MprpcChannel : public google::protobuf::RpcChannel {
 public:
  void CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request, google::protobuf::Message* response,
                  google::protobuf::Closure* done) override;
  MprpcChannel(string ip, short port, bool connectNow, int timeoutMs = 0);

  bool SetTimeoutMs(int timeoutMs, std::string* errMsg);

 private:
  bool ApplySocketTimeout(int fd, int timeoutMs, string* errMsg);
  bool SendAll(const std::string& payload, string* errMsg);
  bool newConnect(const char* ip, uint16_t port, string* errMsg);

  int m_clientFd;
  int m_timeoutMs;
  const std::string m_ip;
  const uint16_t m_port;
};
```

`timeoutMs == 0` means no socket timeout and preserves current Raft RPC and example behavior. Reject negative timeout values through `controller->SetFailed` rather than passing them to POSIX APIs.

- [ ] **Step 2: Apply the remaining per-operation timeout to the socket**

Implement `SetTimeoutMs` and `ApplySocketTimeout` with `SO_SNDTIMEO` and `SO_RCVTIMEO`. Include `<cstring>`, `<fcntl.h>`, `<poll.h>`, and `<sys/time.h>` in `mprpcchannel.cpp`. The helper must convert milliseconds to `timeval`, clear the timeout when passed zero, and return a controller-readable error when `setsockopt` fails:

```cpp
bool MprpcChannel::SetTimeoutMs(int timeoutMs, std::string* errMsg) {
  if (timeoutMs < 0) {
    *errMsg = "timeout must not be negative";
    return false;
  }
  m_timeoutMs = timeoutMs;
  if (m_clientFd != -1) {
    if (!ApplySocketTimeout(m_clientFd, timeoutMs, errMsg)) {
      close(m_clientFd);
      m_clientFd = -1;
      return false;
    }
  }
  return true;
}

bool MprpcChannel::ApplySocketTimeout(int fd, int timeoutMs, std::string* errMsg) {
  if (timeoutMs < 0) {
    *errMsg = "timeout must not be negative";
    return false;
  }
  timeval timeout{};
  timeout.tv_sec = timeoutMs / 1000;
  timeout.tv_usec = (timeoutMs % 1000) * 1000;
  if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) {
    *errMsg = std::strerror(errno);
    return false;
  }
  return true;
}
```

Call `ApplySocketTimeout` after every successful connection and before using an already-open connection so the `remainingMs` value for a retry is honored.

When `m_timeoutMs > 0`, make `connect` deadline-aware as well: set the new socket to nonblocking, call `connect`, wait with `poll(POLLOUT, m_timeoutMs)`, inspect `SO_ERROR`, and restore the original file flags before storing the descriptor. Return `ETIMEDOUT` through `errMsg` when `poll` expires. Keep the existing blocking `connect` path when `m_timeoutMs == 0` so Raft's current behavior is unchanged.

- [ ] **Step 3: Make send and receive timeout failures explicit**

Keep the current wire format, but close the connection and call `controller->SetFailed` when `send` or `recv` returns `EAGAIN`, `EWOULDBLOCK`, `ETIMEDOUT`, or another error. Use a `sendAll` loop so a short TCP write cannot be counted as a completed RPC:

```cpp
bool MprpcChannel::SendAll(const std::string& payload, std::string* errMsg) {
  std::size_t sent = 0;
  while (sent < payload.size()) {
    const auto count = send(m_clientFd, payload.data() + sent, payload.size() - sent, 0);
    if (count > 0) {
      sent += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    *errMsg = std::strerror(errno);
    return false;
  }
  return true;
}
```

Add the declaration for `SendAll` to the private section. On failure, close `m_clientFd`, set it to `-1`, and return before parsing a response. Do not retry inside `MprpcChannel`; retry belongs to the logical Clerk operation so the same request ID is preserved.

- [ ] **Step 4: Compile the RPC-dependent targets before adding Clerk changes**

Run:

```bash
cmake -S . -B build
cmake --build build --target callerMain -j
```

Expected: `callerMain` still builds, proving old `MprpcChannel(ip, port, bool)` calls remain source-compatible. Commit the channel timeout slice with a focused message such as `增加 RPC 客户端超时支持`.

## Task 3: Add Result-Returning Clerk Operations

**Files:**

- Modify: `src/raftClerk/include/raftServerRpcUtil.h`
- Modify: `src/raftClerk/raftServerRpcUtil.cpp`
- Modify: `src/raftClerk/include/clerk.h`
- Modify: `src/raftClerk/clerk.cpp`

- [ ] **Step 1: Add explicit channel ownership and timeout-aware RPC methods**

Change `raftServerRpcUtil` to own the `MprpcChannel` and stub independently. Add `<memory>` to the header. The generated stub must remain non-owning because it is constructed with the default ownership mode:

```cpp
class raftServerRpcUtil {
 private:
  std::unique_ptr<MprpcChannel> m_channel;
  std::unique_ptr<raftKVRpcProctoc::kvServerRpc_Stub> m_stub;

 public:
  bool Get(raftKVRpcProctoc::GetArgs* args, raftKVRpcProctoc::GetReply* reply, int timeoutMs = 0);
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply,
                int timeoutMs = 0);

  raftServerRpcUtil(std::string ip, short port);
  ~raftServerRpcUtil() = default;
};
```

The constructor creates `m_channel` first and passes `m_channel.get()` to the stub. Each method calls `m_channel->SetTimeoutMs(timeoutMs, &errorText)` before invoking the stub; if it returns false, set the controller failure and return false. Otherwise return `!controller.Failed()`. Keep the old two-argument calls source-compatible through the default parameter.

- [ ] **Step 2: Define Clerk result types and bounded methods**

Add the following public types and methods in `clerk.h`, retaining the existing methods unchanged:

```cpp
enum class ClerkOperationStatus { Success, NotFound, Failed };

struct ClerkOperationResult {
  ClerkOperationStatus status = ClerkOperationStatus::Failed;
  std::string value;
};

class Clerk {
 public:
  void Init(std::string configFileName);
  std::string Get(std::string key);
  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

  ClerkOperationResult GetWithTimeout(const std::string& key, int timeoutMs);
  ClerkOperationResult PutWithTimeout(const std::string& key, const std::string& value, int timeoutMs);
  ClerkOperationResult AppendWithTimeout(const std::string& key, const std::string& value, int timeoutMs);

 private:
  ClerkOperationResult PutAppendWithTimeout(const std::string& key, const std::string& value,
                                            const std::string& op, int timeoutMs);
};
```

- [ ] **Step 3: Implement one deadline loop and reuse it for Get/Put/Append**

For each new method, reject an empty server list or non-positive timeout as `Failed`, increment `m_requestId` once, create a `steady_clock` deadline, and keep the same serialized request while rotating `server` on RPC failure or `ErrWrongLeader`:

```cpp
const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
const auto requestId = ++m_requestId;
auto server = m_recentLeaderId % m_servers.size();

while (true) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now()).count();
  if (remaining <= 0) {
    return {ClerkOperationStatus::Failed, {}};
  }

  raftKVRpcProctoc::PutAppendReply reply;
  const bool ok = m_servers[server]->PutAppend(&args, &reply, static_cast<int>(remaining));
  if (ok && reply.err() == OK) {
    m_recentLeaderId = server;
    return {ClerkOperationStatus::Success, {}};
  }
  if (ok && reply.err() != ErrWrongLeader) {
    return {ClerkOperationStatus::Failed, {}};
  }
  server = (server + 1) % m_servers.size();
}
```

Use the analogous `Get` request and classify `ErrNoKey` as `NotFound`, `OK` as `Success`, and transport/unknown errors as `Failed`. Do not emit the existing per-retry `DPrintf` messages from the benchmark path; the result interface must return status instead.

- [ ] **Step 4: Preserve the existing client behavior and compile it**

Keep `Get`, `Put`, `Append`, and the existing `Init` signature behavior unchanged. Run:

```bash
cmake --build build --target callerMain -j
```

Expected: `callerMain` compiles with the new result types present, and no generated Protobuf source changes appear in `git status`. Commit the Clerk/RPC utility slice with a focused message such as `为 Clerk 增加有界重试结果`.

## Task 4: Implement the Benchmark Executable

**Files:**

- Create: `example/raftCoreExample/qpsBenchmark.cpp`
- Modify: `example/raftCoreExample/CMakeLists.txt`

- [ ] **Step 1: Add strict CLI parsing and defaults**

Use `getopt` with `f:n:c:k:g:p:a:t:s:h`. Require `-f` and `-n`; default `clients=1`, `keySpace=100`, weights `1/1/1`, and `timeoutMs=1000`. Validate every numeric value before starting threads:

```cpp
struct BenchmarkConfig {
  std::string configFile;
  std::uint64_t totalOperations = 0;
  std::size_t clients = 1;
  std::size_t keySpace = 100;
  std::uint32_t getWeight = 1;
  std::uint32_t putWeight = 1;
  std::uint32_t appendWeight = 1;
  int timeoutMs = 1000;
  std::uint64_t seed = std::chrono::steady_clock::now().time_since_epoch().count();
};
```

Print Chinese usage text and return `EXIT_FAILURE` for missing `-f`/`-n`, zero operations, zero clients, zero key space, non-positive timeout, or all-zero operation weights. `-s` overrides the default seed.

- [ ] **Step 2: Add per-worker statistics and global operation claiming**

Use local worker statistics to avoid a lock on every operation, and use one atomic counter to enforce the global operation count:

```cpp
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

std::atomic<std::uint64_t> nextOperation{0};
while (true) {
  const auto operationId = nextOperation.fetch_add(1, std::memory_order_relaxed);
  if (operationId >= config.totalOperations) {
    break;
  }
  // Generate one operation, execute it, and update only this worker's stats.
}
```

Each thread creates one `Clerk`, calls `Init(config.configFile)`, and creates one `WorkloadGenerator(config.seed, workerId, ...)`. Measure each logical operation with `steady_clock`, call the matching `GetWithTimeout`, `PutWithTimeout`, or `AppendWithTimeout`, and classify the returned status. Merge `WorkerStats` only after joining all threads.

- [ ] **Step 3: Implement Chinese reporting and QPS formulas**

Measure wall-clock time around worker execution. Print the configured file, concurrency, total operations, success/not-found/failure counts, total time, average per-operation time, total QPS, successful QPS, and operation distribution with `std::fixed << std::setprecision(2)`:

```cpp
const double wallSeconds =
    std::chrono::duration<double>(finish - begin).count();
const double totalQps = wallSeconds == 0.0 ? 0.0 : stats.total / wallSeconds;
const double successQps = wallSeconds == 0.0 ? 0.0 : stats.success / wallSeconds;
const double averageMs = stats.total == 0
                             ? 0.0
                             : std::chrono::duration<double, std::milli>(stats.operationTime).count() /
                                   static_cast<double>(stats.total);

std::cout << "========== Raft KV 压测结果 ==========\n"
          << "配置文件：" << config.configFile << '\n'
          << "并发客户端：" << config.clients << '\n'
          << "操作总数：" << stats.total << '\n'
          << "成功操作数：" << stats.success << '\n'
          << "Get 未命中数：" << stats.notFound << '\n'
          << "失败操作数：" << stats.failed << "\n\n"
          << "总耗时：" << std::chrono::duration<double, std::milli>(finish - begin).count() << " ms\n"
          << "平均请求耗时：" << averageMs << " ms/次\n"
          << "总吞吐：" << totalQps << " QPS\n"
          << "成功吞吐：" << successQps << " QPS\n"
          << "=======================================\n";
```

Check before printing that `success + notFound + failed == total`; if the invariant fails, print a Chinese error and return failure. Do not print individual keys, values, retries, or RPC errors from the benchmark executable.

- [ ] **Step 4: Register the target in CMake**

Append a separate target to `example/raftCoreExample/CMakeLists.txt`:

```cmake
set(QPS_BENCHMARK_SOURCES qpsBenchmark.cpp qpsBenchmarkLogic.cpp)
add_executable(qpsBenchmark ${QPS_BENCHMARK_SOURCES} ${src_raftClerk} ${src_common})
target_link_libraries(qpsBenchmark skip_list_on_raft protobuf::libprotobuf boost_serialization)
```

The target must link the same project libraries used by `callerMain`; do not add a second Protobuf generation path or edit generated files.

- [ ] **Step 5: Build the benchmark target and commit the executable slice**

Run:

```bash
cmake -S . -B build
cmake --build build --target qpsBenchmark -j
```

Expected: `bin/qpsBenchmark` is produced and the build does not regenerate or modify any `*.pb.h`/`*.pb.cc` file. Commit the executable and CMake changes with a focused message such as `添加 Raft KV QPS 压测程序`.

## Task 5: Verify CLI Behavior and Live-Cluster Measurement

**Files:**

- Test: `test/qps_benchmark_logic.cpp`
- Runtime: `bin/qpsBenchmark` against a user-started normal cluster

- [ ] **Step 1: Run the standalone generator test**

Run:

```bash
g++ -std=c++20 \
  -Iexample/raftCoreExample/include \
  example/raftCoreExample/qpsBenchmarkLogic.cpp \
  test/qps_benchmark_logic.cpp \
  -o /tmp/qps_benchmark_logic
/tmp/qps_benchmark_logic
```

Expected: exit status 0; the fixed seed produces matching per-worker sequences, more than one key and value are generated, and all three operation types appear.

- [ ] **Step 2: Verify invalid CLI input fails before RPC work**

Run:

```bash
./bin/qpsBenchmark -n 10
./bin/qpsBenchmark -f bin/checkChange.conf -n 0
./bin/qpsBenchmark -f bin/checkChange.conf -n 10 -c 0
./bin/qpsBenchmark -f bin/checkChange.conf -n 10 -g 0 -p 0 -a 0
```

Expected: every command prints Chinese usage/error text and exits nonzero without creating worker threads.

- [ ] **Step 3: Run a fixed-count live-cluster benchmark**

After the user has started and verified a normal Raft cluster, run:

```bash
./bin/qpsBenchmark \
  -f bin/checkChange.conf \
  -n 1000 \
  -c 4 \
  -k 100 \
  -t 1000 \
  -s 42
```

Expected: the Chinese summary reports `操作总数：1000`, and `成功操作数 + Get 未命中数 + 失败操作数` equals 1000. The report includes varied operation counts and nonzero total/average timing fields.

- [ ] **Step 4: Compare serial and concurrent modes**

Run the same fixed seed and request count with one and four clients:

```bash
./bin/qpsBenchmark -f bin/checkChange.conf -n 1000 -c 1 -s 42
./bin/qpsBenchmark -f bin/checkChange.conf -n 1000 -c 4 -s 42
```

Expected: both runs execute exactly 1000 operations; the operation distribution remains within normal random variation, and the four-client run exercises multiple independent Clerk instances.

- [ ] **Step 5: Verify bounded failure behavior**

Run the benchmark with a configuration containing unreachable local ports or against a stopped test cluster:

```bash
printf '%s\n' 'node0ip=127.0.0.1' 'node0port=1' > /tmp/raft-kv-unreachable.conf
./bin/qpsBenchmark -f /tmp/raft-kv-unreachable.conf -n 8 -c 2 -t 20 -s 42
```

Expected: the process exits after the finite operation workload, reports failed operations, and does not retry forever. Do not use this unreachable configuration for the final throughput number.

- [ ] **Step 6: Run the repository build and formatting checks**

Run:

```bash
cmake --build build -j
cmake --build build --target format
git diff --check
```

Expected: all existing targets and `qpsBenchmark` build successfully, formatting completes, and `git diff --check` reports no whitespace errors. Before reporting final QPS results, record that the repository's `Debug` constant currently enables server-side logging, because those logs can materially reduce measured throughput.

## Self-Review Checklist

- Spec coverage: the plan covers existing-cluster-only execution, configurable clients, global fixed operation count, `1:1:1` weighted mixed operations, varied deterministic inputs, per-operation deadlines, Leader rotation, separate `Success`/`NotFound`/`Failed` results, Chinese output, no generated Protobuf edits, CMake integration, and live-cluster verification.
- Placeholder scan: no step depends on unresolved placeholder markers, an unspecified helper, or an unbounded handling instruction.
- Type consistency: `GeneratedOperation` and `OperationType` are declared in `qpsBenchmarkLogic.h`; Clerk result types are declared in `clerk.h`; all later tasks use those names and the same `GetWithTimeout`/`PutWithTimeout`/`AppendWithTimeout` signatures.
- Scope check: workload generation, transport deadline support, client retry semantics, and benchmark reporting are the four coupled pieces required by the approved design; no Raft core or Protobuf schema changes are included.
