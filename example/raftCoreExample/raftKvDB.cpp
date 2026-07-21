//
// Created by swx on 23-12-28.
//
#include <iostream>
#include "raft.h"
// #include "kvServer.h"
#include <kvServer.h>
#include <unistd.h>
#include <iostream>
#include <random>

void ShowArgsHelp();

int main(int argc, char **argv) {
  //////////////////////////////////读取命令参数：节点数量、写入raft节点节点信息到哪个文件
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }
  int c = 0;
  int nodeNum = 0;
  std::string configFileName;
  // 利用random获取随机种子 采用mt19937伪随机 最后利用dis解出随机起始端口
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);

  while ((c = getopt(argc, argv, "n:f:")) != -1) {
    switch (c) {
      // optarg 是当前shell的参数
      case 'n':
        nodeNum = atoi(optarg); // string 转换成 integer
        break;
      case 'f':
        configFileName = optarg;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }
  // 打开/创建文件
  std::ofstream file(configFileName, std::ios::out | std::ios::app);
  file.close(); // 关闭后重新以trunc模式打开 清空旧的配置内容
  // 当然这里可以只写trunc 因为trunc也代表了打开/创建
  file = std::ofstream(configFileName, std::ios::out | std::ios::trunc);
  if (file.is_open()) {
    file.close();
    std::cout << configFileName << " 已清空" << std::endl;
  } else {
    std::cout << "无法打开 " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }

  // 构建n个独立进程 每个进程运行一个Raftkv节点，同时他们通过IP+端口互相发现
  for (int i = 0; i < nodeNum; i++) {
    short port = startPort + static_cast<short>(i); // 生成子进程的port 他是连续的 如果可以的话这里可以加入端口检查
    std::cout << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid() << std::endl;
    pid_t pid = fork();  // 从这里开始 创建新进程

    // 对于父进程 pid返回正数 而子进程返回0 fork失败子进程内为-1
    if (pid == 0) { // 如果是子进程
      // 子进程 创建一个KvServer 并将IP、随机化的端口写入conf 同时进入pause
      auto kvServer = new KvServer(i, 500, configFileName, port);
      // 子进入pause 监听
      pause();
    } else if (pid > 0) {
      // 如果是父进程
      // 休眠1s后 再创建新的子进程 作为缓冲
      sleep(1);
    } else {
      // 如果创建进程失败
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  pause();
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
