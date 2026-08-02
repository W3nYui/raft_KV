//
// Created by swx on 24-1-4.
//
#include "raftServerRpcUtil.h"

#include <chrono>
#include <limits>

// kvserver不同于raft节点之间，kvserver的rpc是用于clerk向kvserver调用，不会被调用，因此只用写caller功能，不用写callee功能
// 先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port)
    : channel_(std::make_unique<MprpcChannel>(ip, port, false)), stub_(nullptr) {
  //*********************************************  */
  // 接收rpc设置
  //*********************************************  */
  // 发送rpc设置
  stub_ = std::make_unique<raftKVRpcProctoc::kvServerRpc_Stub>(channel_.get(),
                                                               google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
}

raftServerRpcUtil::~raftServerRpcUtil() = default;

namespace {

using SteadyClock = std::chrono::steady_clock;

int RemainingMilliseconds(SteadyClock::time_point deadline) {
  const auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - SteadyClock::now()).count();
  if (remainingMs <= 0) {
    return 0;
  }
  if (remainingMs > std::numeric_limits<int>::max()) {
    return std::numeric_limits<int>::max();
  }
  return static_cast<int>(remainingMs);
}

bool PrepareChannel(MprpcChannel *channel, int timeoutMs, std::string *errorText) {
  if (timeoutMs < 0) {
    *errorText = "timeout must be non-negative";
    return false;
  }
  if (timeoutMs == 0) {
    return channel->SetTimeoutMs(0, errorText) && channel->Connect(errorText);
  }

  const auto deadline = SteadyClock::now() + std::chrono::milliseconds(timeoutMs);
  const int beforeConnectMs = RemainingMilliseconds(deadline);
  if (beforeConnectMs <= 0 || !channel->SetTimeoutMs(beforeConnectMs, errorText) || !channel->Connect(errorText)) {
    return false;
  }

  const int beforeRpcMs = RemainingMilliseconds(deadline);
  if (beforeRpcMs <= 0) {
    *errorText = "RPC operation deadline expired while connecting";
    return false;
  }
  return channel->SetTimeoutMs(beforeRpcMs, errorText);
}

}  // namespace

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply, int timeoutMs) {
  std::string errorText;
  if (!PrepareChannel(channel_.get(), timeoutMs, &errorText)) {
    return false;
  }
  MprpcController controller;
  stub_->Get(&controller, GetArgs, reply, nullptr);
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply,
                                  int timeoutMs) {
  std::string errorText;
  if (!PrepareChannel(channel_.get(), timeoutMs, &errorText)) {
    return false;
  }
  MprpcController controller;
  stub_->PutAppend(&controller, args, reply, nullptr);
  return !controller.Failed();
}
