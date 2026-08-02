//
// Created by swx on 24-1-4.
//
#include "raftServerRpcUtil.h"

// kvserver不同于raft节点之间，kvserver的rpc是用于clerk向kvserver调用，不会被调用，因此只用写caller功能，不用写callee功能
//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port)
    : channel_(std::make_unique<MprpcChannel>(ip, port, false)), stub(nullptr) {
  //*********************************************  */
  // 接收rpc设置
  //*********************************************  */
  //发送rpc设置
  stub = new raftKVRpcProctoc::kvServerRpc_Stub(channel_.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
}

raftServerRpcUtil::~raftServerRpcUtil() { delete stub; }

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply) {
  std::string errMsg;
  if (!channel_->Connect(&errMsg)) {
    return false;
  }
  MprpcController controller;
  stub->Get(&controller, GetArgs, reply, nullptr);
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  std::string errMsg;
  if (!channel_->Connect(&errMsg)) {
    return false;
  }
  MprpcController controller;
  stub->PutAppend(&controller, args, reply, nullptr);
  if (controller.Failed()) {
    std::cout << controller.ErrorText() << endl;
  }
  return !controller.Failed();
}
