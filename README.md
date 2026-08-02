# Raft KV

这是一个基于 C++20、Protobuf 和自定义 RPC 的 Raft 键值存储学习项目。项目包含 Raft 集群、KV 状态机、客户端 Clerk，以及用于测量吞吐量的 `qpsBenchmark` 压测程序。

## 写在前面
这是我的一个学习项目，压测只是为了测试指标的学习。对于压测的内容，后续可以有很多改进点，比如无锁队列、关闭debug日志输出、快速恢复、异步RPC，或者改进raft结构，允许弱一致性读。
同时在IO的处理上，可以参照 copy on write 的做法，分段追加日志，批量提交状态。
在KV上，我这里采用的是跳表，也可以改进成速度更快的分片KV或RocksDB这种快速DB。

## 环境要求

- CMake 3.22 或更高版本
- 支持 C++20 的 GCC 和 G++
- Protobuf
- Muduo `muduo_net`、`muduo_base`
- Boost Serialization
- pthread、dl

## 构建

建议使用 GCC 建立独立构建目录：

```bash
cmake -S . -B build-gcc \
  -DCMAKE_C_COMPILER=gcc \
  -DCMAKE_CXX_COMPILER=g++
cmake --build build-gcc -j
```

主要可执行文件位于 `bin/`：

- `raftCoreRun`：启动本地 Raft 集群
- `qpsBenchmark`：执行 KV 混合负载压测
- `callerMain`：原有的串行客户端示例

## 启动集群

压测程序不会启动或停止集群，必须先启动一个正常运行的 Raft 集群。在终端一执行：

```bash
./bin/raftCoreRun -n 3 -f /tmp/raft-kv.conf
```

参数含义：

- `-n 3`：启动 3 个 Raft 节点
- `-f /tmp/raft-kv.conf`：节点配置文件路径。程序会先清空该文件，再写入各节点 IP 和端口

等待终端出现各节点 RPC 服务启动、节点互联并完成选主后，再打开终端二执行压测。启动集群的终端会持续运行，保持该终端不要关闭。

## 执行压测

推荐先执行一个小规模验证：

```bash
./bin/qpsBenchmark \
  -f /tmp/raft-kv.conf \
  -n 100 \
  -c 4 \
  -k 32 \
  -t 1000 \
  -s 42
```

常用参数如下：

| 参数 | 含义 | 默认值 |
| --- | --- | --- |
| `-f` | Raft 节点配置文件，必填 | 无 |
| `-n` | 所有客户端合计执行的操作数，必填 | 无 |
| `-c` | 并发客户端线程数 | `1` |
| `-k` | Key 的数量，Key 从有限集合中随机选择 | `100` |
| `-g` | Get 操作权重 | `1` |
| `-p` | Put 操作权重 | `1` |
| `-a` | Append 操作权重 | `1` |
| `-t` | 单次逻辑操作的超时时间，单位毫秒 | `1000` |
| `-s` | 随机种子，用于复现实验输入 | 当前时间 |
| `-h` | 显示帮助 | 无 |

权重不是百分比。例如：

```bash
./bin/qpsBenchmark -f /tmp/raft-kv.conf -n 1000 -c 4 -g 2 -p 1 -a 1 -s 42
```

表示 Get、Put、Append 的目标比例为 `2:1:1`。每个工作线程使用独立的随机序列；Key 会变化，Value 会包含线程编号、序号和随机片段，不会所有请求都使用同一内容。

## 结果说明

程序结束时输出中文报告，例如：

```text
========== Raft KV 压测结果 ==========
配置文件：/tmp/raft-kv.conf
并发客户端：4
操作总数：100
成功操作数：91
Get 未命中数：9
失败操作数：0

总耗时：607.79 ms
平均请求耗时：24.29 ms/次
总吞吐：164.53 QPS
成功吞吐：149.72 QPS

操作分布：
  Get：26
  Put：38
  Append：36
=======================================
```

- `成功操作数`：Get 返回已有值，或 Put/Append 成功提交
- `Get 未命中数`：Get 正常完成但 Key 不存在，不计入成功数，也不计入失败数
- `失败操作数`：连接失败、RPC 超时、找不到 Leader 或其他异常
- `总吞吐`：总操作数除以整个压测墙钟耗时
- `成功吞吐`：成功操作数除以整个压测墙钟耗时
- 三类结果应满足：`成功操作数 + Get 未命中数 + 失败操作数 = 操作总数`

压测程序会在截止时间内轮换节点重试。同一个逻辑操作保持相同的 `clientId + requestId`，避免重试写入时改变请求语义。

## 测试

构建两个独立的测试目标：

```bash
cmake --build build-gcc --target test_qps_benchmark_logic test_clerk_timeout_contract -j
./bin/test_qps_benchmark_logic
./bin/test_clerk_timeout_contract
```

还可以使用不可达配置验证超时不会无限重试。配置文件至少包含以下内容：

```text
node0ip=127.0.0.1
node0port=1
```

然后执行：

```bash
./bin/qpsBenchmark -f /tmp/raft-kv-unreachable.conf -n 8 -c 2 -t 20 -s 42
```

该测试应在有限时间内结束，并将请求计入失败数。

## 停止集群

回到启动集群的终端，按 `Ctrl+C` 停止本次本地集群。确认没有残留进程：

```bash
pgrep -af 'raftCoreRun|raftServer' || true
```

## 注意事项

1. 当前项目默认在 `src/common/include/config.h` 中开启 `Debug` 日志。服务端会输出大量心跳和 Raft 调试信息，这会显著影响 QPS。不同实验必须保持 Debug 配置一致，正式性能对比前应关闭逐请求调试日志并重新构建。
2. `-n` 是所有并发客户端合计的操作数，不是每个客户端的操作数。例如 `-n 1000 -c 4` 总共仍然只执行 1000 个逻辑操作。
3. 该工具测量的是当前机器、当前 Debug 配置和当前 Raft 实现下的吞吐量，不包含分布式压测控制器，也不提供延迟百分位统计。
4. 如果运行环境禁止本机监听端口，集群可能在创建 Muduo socket 时报告 `Operation not permitted`。需要在允许本机网络监听的环境中运行集群。
