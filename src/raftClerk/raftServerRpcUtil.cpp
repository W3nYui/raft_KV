//
// Created by swx on 24-1-4.
//
#include "raftServerRpcUtil.h"

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

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply, int timeoutMs) {
  std::string errorText;
  if (!channel_->SetTimeoutMs(timeoutMs, &errorText)) {
    return false;
  }
  if (!channel_->Connect(&errorText)) {
    return false;
  }
  MprpcController controller;
  stub_->Get(&controller, GetArgs, reply, nullptr);
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply,
                                  int timeoutMs) {
  std::string errorText;
  if (!channel_->SetTimeoutMs(timeoutMs, &errorText)) {
    return false;
  }
  if (!channel_->Connect(&errorText)) {
    return false;
  }
  MprpcController controller;
  stub_->PutAppend(&controller, args, reply, nullptr);
  return !controller.Failed();
}
