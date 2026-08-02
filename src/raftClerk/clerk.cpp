//
// Created by swx on 23-6-4.
//
#include "clerk.h"

#include "raftServerRpcUtil.h"

#include "util.h"

#include <chrono>
#include <string>
#include <vector>

namespace {

using SteadyClock = std::chrono::steady_clock;

int RemainingMilliseconds(SteadyClock::time_point deadline) {
  return static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - SteadyClock::now()).count());
}

}  // namespace

std::string Clerk::Get(std::string key) {
  m_requestId++;
  auto requestId = m_requestId;
  size_t server = static_cast<size_t>(m_recentLeaderId);
  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);
    if (!ok ||
        reply.err() ==
            ErrWrongLeader) {  // 会一直重试，因为requestId没有改变，因此可能会因为RPC的丢失或者其他情况导致重试，kvserver层来保证不重复执行（线性一致性）
      server = (server + 1) % m_servers.size();
      continue;
    }
    if (reply.err() == ErrNoKey) {
      return "";
    }
    if (reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      return reply.value();
    }
  }
  return "";
}

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  // You will have to modify this function.
  m_requestId++;
  auto requestId = m_requestId;
  size_t server = static_cast<size_t>(m_recentLeaderId);
  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);
    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);
    if (!ok || reply.err() == ErrWrongLeader) {
      DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}",
              static_cast<int>(server), static_cast<int>(server + 1), op.c_str());
      if (!ok) {
        DPrintf("重试原因 ，rpc失敗 ，");
      }
      if (reply.err() == ErrWrongLeader) {
        DPrintf("重試原因：非leader");
      }
      server = (server + 1) % m_servers.size();  // try the next server
      continue;
    }
    if (reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      return;
    }
  }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }

ClerkOperationResult Clerk::GetWithTimeout(const std::string& key, int timeoutMs) {
  if (timeoutMs <= 0 || m_servers.empty()) {
    return {};
  }

  const auto deadline = SteadyClock::now() + std::chrono::milliseconds(timeoutMs);
  const int requestId = ++m_requestId;
  const size_t serverCount = m_servers.size();
  size_t server = static_cast<size_t>(m_recentLeaderId) % serverCount;

  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    const int remainingMs = RemainingMilliseconds(deadline);
    if (remainingMs <= 0) {
      return {};
    }

    raftKVRpcProctoc::GetReply reply;
    const bool ok = m_servers[server]->Get(&args, &reply, remainingMs);
    if (!ok || reply.err() == ErrWrongLeader) {
      server = (server + 1) % serverCount;
      continue;
    }
    if (reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      return {ClerkOperationStatus::Success, reply.value()};
    }
    if (reply.err() == ErrNoKey) {
      return {ClerkOperationStatus::NotFound, {}};
    }
    return {};
  }
}

ClerkOperationResult Clerk::PutWithTimeout(const std::string& key, const std::string& value, int timeoutMs) {
  return PutAppendWithTimeout(key, value, "Put", timeoutMs);
}

ClerkOperationResult Clerk::AppendWithTimeout(const std::string& key, const std::string& value, int timeoutMs) {
  return PutAppendWithTimeout(key, value, "Append", timeoutMs);
}

ClerkOperationResult Clerk::PutAppendWithTimeout(const std::string& key, const std::string& value,
                                                 const std::string& op, int timeoutMs) {
  if (timeoutMs <= 0 || m_servers.empty()) {
    return {};
  }

  const auto deadline = SteadyClock::now() + std::chrono::milliseconds(timeoutMs);
  const int requestId = ++m_requestId;
  const size_t serverCount = m_servers.size();
  size_t server = static_cast<size_t>(m_recentLeaderId) % serverCount;

  raftKVRpcProctoc::PutAppendArgs args;
  args.set_key(key);
  args.set_value(value);
  args.set_op(op);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    const int remainingMs = RemainingMilliseconds(deadline);
    if (remainingMs <= 0) {
      return {};
    }

    raftKVRpcProctoc::PutAppendReply reply;
    const bool ok = m_servers[server]->PutAppend(&args, &reply, remainingMs);
    if (!ok || reply.err() == ErrWrongLeader) {
      server = (server + 1) % serverCount;
      continue;
    }
    if (reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      return {ClerkOperationStatus::Success, {}};
    }
    return {};
  }
}

// 初始化客户端
void Clerk::Init(std::string configFileName) {
  // 自定义的一种 config 类 用于解析raft初始化时得到的节点。
  MprpcConfig config;
  config.LoadConfigFile(configFileName.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    // 获取所有的IP与对应节点
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
  }
  // 进行连接
  for (const auto& item : ipPortVt) {
    std::string ip = item.first;
    short port = item.second;
    // 2024-01-04 todo：bug fix
    auto* rpc = new raftServerRpcUtil(ip, port);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
