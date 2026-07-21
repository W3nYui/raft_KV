//
// Created by swx on 23-12-21.
//
#include <iostream>

// #include "mprpcapplication.h"
#include "rpcExample/friend.pb.h"

#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"

// 这里充当客户端
int main(int argc, char **argv) {
  // https://askubuntu.com/questions/754213/what-is-difference-between-localhost-address-127-0-0-1-and-127-0-1-1
  std::string ip = "127.0.1.1";
  short port = 7788;

  // 演示调用远程发布的rpc方法Login 定义rpc的IP 端口与方法
  fixbug::FiendServiceRpc_Stub stub(
      new MprpcChannel(ip, port, true));  //注册进自己写的channel类，channel类用于自定义发送格式和负责序列化等操作

  // 共 20 个候选好友。这里直接用初始化列表，比 vector<string>(20) 后逐项赋值更简洁。
  std::vector<std::string> friend_names = {
      "alice", "bob",   "cindy", "david", "emma",
      "frank", "grace", "helen", "ivan",  "jane",
      "kevin", "linda", "mike",  "nancy", "oscar",
      "peter", "queen", "rose",  "steve", "tom",
  };

  std::random_device random_device;
  std::mt19937 generator(random_device()); // 生成伪随机数
  // 随机打乱名称，使每次写入的是不同顺序的名字。
  std::shuffle(friend_names.begin(), friend_names.end(), generator);

  // 生成恰好 20 次操作：10 次写入、10 次读取，然后随机穿插。
  std::vector<bool> operations;
  for (int i = 0; i < 10; ++i) {
    operations.push_back(true);   // true: AddFriend
    operations.push_back(false);  // false: GetFriendsList
  }
  std::shuffle(operations.begin(), operations.end(), generator);

  //長連接測試 ，發送20次請求 且这20次是随机穿插的写入/读取
  int count = 20;
  size_t next_friend_index = 0;
  while (count--) {
    const int round = 20 - count;
    const bool should_add = operations[round - 1];

    std::cout << " 倒数" << count << "次发起RPC请求" << '\t';


    if (should_add) {
      const auto friend_name = friend_names[next_friend_index++];
      std::cout << "AddFriend: " << friend_name << '\t';
      // add方法的rpc参数
      fixbug::AddFriendRequest add_request;
      add_request.set_userid(1000);
      add_request.set_friend_name(friend_name);

      fixbug::AddFriendResponse add_response;
      MprpcController add_controller;

      stub.AddFriend(&add_controller, &add_request, &add_response, nullptr);
      if (add_controller.Failed()) {
      std::cout << "AddFriend RPC failed: " << add_controller.ErrorText() << std::endl;
      } else if (add_response.result().errcode() != 0) {
        std::cout << "AddFriend business failed: "
                  << add_response.result().errmsg() << std::endl;
      } else {
        std::cout << "AddFriend success" << friend_name << std::endl;
      }
    } else {
      std::cout << "GetFriendsList" << '\t';
      // rpc方法的请求参数
      fixbug::GetFriendsListRequest get_request; // 定义请求的protobuf 并设置参数user_id
      get_request.set_userid(1000);
      // rpc方法的响应
      fixbug::GetFriendsListResponse get_response; // 定义返回
      // 发起rpc方法的调用,消费者的stub最后都会调用到channel的 call_method方法  同步的rpc调用过程  MprpcChannel::callmethod
      MprpcController get_controller;
      stub.GetFriendsList(&get_controller, &get_request, &get_response, nullptr);
      // RpcChannel->RpcChannel::callMethod 集中来做所有rpc方法调用的参数序列化和网络发送

      // 一次rpc调用完成，读调用的结果
      // rpc调用是否失败由框架来决定（rpc调用失败 ！= 业务逻辑返回false）
      // rpc和业务本质上是隔离的
      if (get_controller.Failed()) {
        std::cout << get_controller.ErrorText() << std::endl;
      } else {
        if (0 == get_response.result().errcode()) {
          std::cout << "GetFriendsList success" <<'\t' << "当前好友数: " << get_response.friends_size() << std::endl;
          int size = get_response.friends_size();
          for (int i = 0; i < size; i++) {
            std::cout << "  [" << i + 1 << "] " << get_response.friends(i) << std::endl;
          }
        } else {
          //这里不是rpc失败，
          // 而是业务逻辑的返回值是失败
          // 两者要区分清楚
          std::cout << "GetFriendsList business failed: " << get_response.result().errmsg() << std::endl;
        }
      }
    }
    
    sleep(5);  // sleep 5 seconds
  }
  return 0;
}
