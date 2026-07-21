//
// Created by swx on 23-12-21.
//
#include <mprpcchannel.h>
#include <iostream>
#include <string>
#include "rpcExample/friend.pb.h"

#include <vector>
#include "rpcprovider.h"

class FriendService : public fixbug::FiendServiceRpc {
 public:
  // std::vector<std::string> GetFriendsList(uint32_t userid) {
  //   std::cout << "local do GetFriendsList service! userid:" << userid << std::endl;
  //   std::vector<std::string> vec;
  //   vec.push_back("gao yang");
  //   vec.push_back("liu hong");
  //   vec.push_back("wang shuo");
  //   return vec;
  // }

  // 重写基类方法
  void GetFriendsList(::google::protobuf::RpcController *controller, const ::fixbug::GetFriendsListRequest *request,
                      ::fixbug::GetFriendsListResponse *response, ::google::protobuf::Closure *done) {
    // uint32_t userid = request->userid();
    // std::vector<std::string> friendsList = GetFriendsList(userid);
    std::cout << "local do GetFriendsList service! userid:" << request->userid() << std::endl;
    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");

    // for (std::string &name : friendsList) {
    //   std::string *p = response->add_friends();
    //   *p = name;
    // }
    for (std::string& friend_name : friendsList_) {
      response->add_friends(friend_name);
    }

    if (friendsList_.empty()) {
      response->add_friends("cur has no friend");
    }

    done->Run();
  }

  void AddFriend(google::protobuf::RpcController* controller,
                 const fixbug::AddFriendRequest* request,
                 fixbug::AddFriendResponse* response,
                 google::protobuf::Closure* done) override {
    std::cout << "AddFriend, userid: " << request->userid() << ", friend: " << request->friend_name() << std::endl;
    if (request->friend_name().empty()) {
      response->mutable_result()->set_errcode(1);
      response->mutable_result()->set_errmsg("friend name cannot be empty");
    } else {
      friendsList_.push_back(request->friend_name());
      response->mutable_result()->set_errcode(0);
      response->mutable_result()->set_errmsg("");
    }
    done->Run();
  }

  private:
  std::vector<std::string> friendsList_;
};

void ShowArgsHelp() { std::cout << "format: callee -f <configFileName>" << std::endl; }

int main(int argc, char **argv) {
  std::string configFileName;
  int option = 0;

  while ((option = getopt(argc, argv, "f:")) != -1) {
    if (option == 'f') {
      configFileName = optarg;
    } else {
      ShowArgsHelp();
      return EXIT_FAILURE;
    }
  }

  if (configFileName.empty()) {
    ShowArgsHelp();
    return EXIT_FAILURE;
  }

  // 这里的服务端并没有使用到stub的方法。
  // std::string ip = "127.0.0.1";
  // short port = 7788;
  // auto stub = new fixbug::FiendServiceRpc_Stub(new MprpcChannel(ip, port, false));
  // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
  RpcProvider provider;
  provider.NotifyService(new FriendService());

  // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
  provider.Run(1, configFileName, 7788);

  return 0;
}

