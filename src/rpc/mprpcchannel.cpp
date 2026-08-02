#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <chrono>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"

namespace {

using SteadyClock = std::chrono::steady_clock;

void SetSocketError(std::string* errMsg, const char* operation, int errorNumber) {
  if (errMsg == nullptr) {
    return;
  }
  *errMsg = std::string(operation) + " error! errno:" + std::to_string(errorNumber) + " (" +
            std::strerror(errorNumber) + ")";
}

void CloseSocket(int* fd) {
  if (*fd != -1) {
    close(*fd);
    *fd = -1;
  }
}

int RemainingTimeoutMs(SteadyClock::time_point deadline) {
  const auto now = SteadyClock::now();
  if (now >= deadline) {
    return 0;
  }
  const auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
  return remainingMs > 0 ? static_cast<int>(remainingMs) : 1;
}

bool ApplySocketOptionTimeout(int fd, int option, int timeoutMs, const char* operation, std::string* errMsg) {
  timeval timeout{};
  timeout.tv_sec = timeoutMs / 1000;
  timeout.tv_usec = (timeoutMs % 1000) * 1000;
  if (setsockopt(fd, SOL_SOCKET, option, &timeout, sizeof(timeout)) == -1) {
    SetSocketError(errMsg, operation, errno);
    return false;
  }
  return true;
}

}  // namespace

/*
header_size + service_name method_name args_size + args
*/
// 所有通过stub代理对象调用的rpc方法，都会走到这里了，
// 统一通过rpcChannel来调用方法
// 统一做rpc方法调用的数据数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
  (void)done;
  // 检查TCP连接                                
  if (m_clientFd == -1) {
    controller->SetFailed("RPC channel is not connected");
    return;
  }

  // 获取方案与路由信息
  const google::protobuf::ServiceDescriptor* sd = method->service();
  std::string service_name = sd->name();     // service_name  ->  kvServerRpc
  std::string method_name = method->name();  // method_name   ->  Get / PutAppend

  // 获取参数的序列化字符串长度 args_size 这里指的是 GetArgs 等 proto
  uint32_t args_size{};
  std::string args_str;
  if (request->SerializeToString(&args_str)) {
    args_size = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }
  // 补充 RPC header
  // service_name = "kvServerRpc"
  // method_name  = "Get"
  // args_size    = GetArgs 序列化后的字节数
  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name); 
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str)) {
    controller->SetFailed("serialize rpc header error!");
    return;
  }

  // 实际发送内容是：varint32(header_size) + protobuf(RpcHeader) + protobuf(GetArgs)
  // [头长度][服务名、方法名、参数长度][业务请求参数]


  // 使用protobuf的CodedOutputStream来构建发送的数据流
  std::string send_rpc_str;  // 用来存储最终发送的数据
  {
    // 创建一个StringOutputStream用于写入send_rpc_str
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    // 先写入header的长度（变长编码）
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    // 不需要手动写入header_size，因为上面的WriteVarint32已经包含了header的长度信息
    // 然后写入rpc_header本身
    coded_output.WriteString(rpc_header_str);
  }

  // 最后，将请求参数附加到send_rpc_str后面
  send_rpc_str += args_str;

  // 打印调试信息
  //    std::cout << "============================================" << std::endl;
  //    std::cout << "header_size: " << header_size << std::endl;
  //    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
  //    std::cout << "service_name: " << service_name << std::endl;
  //    std::cout << "method_name: " << method_name << std::endl;
  //    std::cout << "args_str: " << args_str << std::endl;
  //    std::cout << "============================================" << std::endl;

  // 发送rpc请求
  std::string errMsg;
  if (!ApplySocketTimeout(m_clientFd, m_timeoutMs, &errMsg)) {
    CloseSocket(&m_clientFd);
    controller->SetFailed(errMsg);
    return;
  }
  if (!SendAll(send_rpc_str, &errMsg)) {
    CloseSocket(&m_clientFd);
    controller->SetFailed(errMsg);
    return;
  }

  /*
  从时间节点来说，这里将请求发送过去之后rpc服务的提供者就会开始处理，返回的时候就代表着已经返回响应了
  */

  // 接收rpc请求的响应值
  char recv_buf[1024] = {0};
  ssize_t recv_size = 0;

  // 同步等待响应 如果等待响应失败返回fail
  recv_size = recv(m_clientFd, recv_buf, 1024, 0);
  if (recv_size <= 0) {
    const int errorNumber = recv_size == 0 ? ECONNRESET : errno;
    CloseSocket(&m_clientFd);
    SetSocketError(&errMsg, "recv", errorNumber);
    controller->SetFailed(errMsg);
    return;
  }

  // 反序列化rpc调用的响应数据 最终写回 response
  // std::string response_str(recv_buf, 0, recv_size);
  // 利用 ParseFromArray 去反序列化 并写入response
  if (!response->ParseFromArray(recv_buf, static_cast<int>(recv_size))) {
    CloseSocket(&m_clientFd);
    controller->SetFailed("parse error! response parse failed");
    return;
  }
}

bool MprpcChannel::ApplySocketTimeout(int fd, int timeoutMs, string* errMsg) {
  if (timeoutMs < 0) {
    if (errMsg != nullptr) {
      *errMsg = "timeout must be non-negative";
    }
    return false;
  }

  if (errMsg != nullptr) {
    errMsg->clear();
  }

  if (!ApplySocketOptionTimeout(fd, SO_SNDTIMEO, timeoutMs, "setsockopt SO_SNDTIMEO", errMsg)) {
    return false;
  }
  if (!ApplySocketOptionTimeout(fd, SO_RCVTIMEO, timeoutMs, "setsockopt SO_RCVTIMEO", errMsg)) {
    return false;
  }
  return true;
}

bool MprpcChannel::SetTimeoutMs(int timeoutMs, string* errMsg) {
  if (timeoutMs < 0) {
    if (errMsg != nullptr) {
      *errMsg = "timeout must be non-negative";
    }
    return false;
  }

  if (m_clientFd != -1) {
    if (!ApplySocketTimeout(m_clientFd, timeoutMs, errMsg)) {
      CloseSocket(&m_clientFd);
      return false;
    }
  }
  m_timeoutMs = timeoutMs;
  if (errMsg != nullptr) {
    errMsg->clear();
  }
  return true;
}

bool MprpcChannel::Connect(std::string* errMsg) {
  if (m_clientFd != -1) {
    if (errMsg != nullptr) {
      errMsg->clear();
    }
    return true;
  }
  return newConnect(m_ip.c_str(), m_port, errMsg);
}

bool MprpcChannel::newConnect(const char* ip, uint16_t port, string* errMsg) {
  if (m_timeoutMs < 0) {
    if (errMsg != nullptr) {
      *errMsg = "timeout must be non-negative";
    }
    CloseSocket(&m_clientFd);
    return false;
  }

  // 建立TCP socket
  CloseSocket(&m_clientFd);
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd) {
    SetSocketError(errMsg, "create socket", errno);
    return false;
  }

  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);

  if (m_timeoutMs > 0) {
    const int originalFlags = fcntl(clientfd, F_GETFL, 0);
    if (originalFlags == -1) {
      const int errorNumber = errno;
      CloseSocket(&clientfd);
      SetSocketError(errMsg, "fcntl F_GETFL", errorNumber);
      return false;
    }
    if (fcntl(clientfd, F_SETFL, originalFlags | O_NONBLOCK) == -1) {
      const int errorNumber = errno;
      CloseSocket(&clientfd);
      SetSocketError(errMsg, "fcntl F_SETFL", errorNumber);
      return false;
    }

    const auto deadline = SteadyClock::now() + std::chrono::milliseconds(m_timeoutMs);
    int connectError = 0;
    if (connect(clientfd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
      const int initialError = errno;
      if (initialError == EINPROGRESS || initialError == EINTR) {
        pollfd pollFd{};
        pollFd.fd = clientfd;
        pollFd.events = POLLOUT;
        int pollResult = -1;
        while (true) {
          const int remainingMs = RemainingTimeoutMs(deadline);
          if (remainingMs == 0) {
            connectError = ETIMEDOUT;
            break;
          }
          pollResult = poll(&pollFd, 1, remainingMs);
          if (pollResult == -1 && errno == EINTR) {
            continue;
          }
          break;
        }
        if (connectError == 0) {
          if (pollResult == 0) {
            connectError = ETIMEDOUT;
          } else if (pollResult == -1) {
            connectError = errno;
          } else {
            int socketError = 0;
            socklen_t socketErrorLength = sizeof(socketError);
            if (getsockopt(clientfd, SOL_SOCKET, SO_ERROR, &socketError, &socketErrorLength) == -1) {
              connectError = errno;
            } else if (socketError != 0) {
              connectError = socketError;
            }
          }
        }
      } else {
        connectError = initialError;
      }
    }

    if (fcntl(clientfd, F_SETFL, originalFlags) == -1) {
      const int errorNumber = errno;
      CloseSocket(&clientfd);
      SetSocketError(errMsg, "fcntl restore flags", errorNumber);
      return false;
    }
    if (connectError != 0) {
      CloseSocket(&clientfd);
      SetSocketError(errMsg, "connect", connectError);
      return false;
    }
  } else if (connect(clientfd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
    const int errorNumber = errno;
    CloseSocket(&clientfd);
    SetSocketError(errMsg, "connect", errorNumber);
    return false;
  }

  if (!ApplySocketTimeout(clientfd, m_timeoutMs, errMsg)) {
    CloseSocket(&clientfd);
    return false;
  }

  // 获取套接字
  m_clientFd = clientfd;
  return true;
}

bool MprpcChannel::SendAll(const std::string& payload, std::string* errMsg) {
  if (m_timeoutMs < 0) {
    if (errMsg != nullptr) {
      *errMsg = "timeout must be non-negative";
    }
    return false;
  }

  const bool hasDeadline = m_timeoutMs > 0;
  const auto deadline = hasDeadline ? SteadyClock::now() + std::chrono::milliseconds(m_timeoutMs)
                                    : SteadyClock::time_point{};
  size_t sent = 0;
  while (sent < payload.size()) {
    if (hasDeadline) {
      const int remainingMs = RemainingTimeoutMs(deadline);
      if (remainingMs == 0) {
        SetSocketError(errMsg, "send", ETIMEDOUT);
        return false;
      }
      if (!ApplySocketOptionTimeout(m_clientFd, SO_SNDTIMEO, remainingMs, "setsockopt SO_SNDTIMEO", errMsg)) {
        return false;
      }
    }

    const ssize_t bytesSent = send(m_clientFd, payload.data() + sent, payload.size() - sent, MSG_NOSIGNAL);
    if (bytesSent > 0) {
      sent += static_cast<size_t>(bytesSent);
      continue;
    }
    if (bytesSent == -1 && errno == EINTR) {
      continue;
    }

    if (bytesSent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT)) {
      SetSocketError(errMsg, "send", ETIMEDOUT);
      return false;
    }

    const int errorNumber = bytesSent == 0 ? EPIPE : errno;
    SetSocketError(errMsg, "send", errorNumber);
    return false;
  }
  return true;
}

MprpcChannel::MprpcChannel(string ip, short port, bool connectNow, int timeoutMs)
    : m_clientFd(-1), m_ip(ip), m_port(port), m_timeoutMs(timeoutMs) {
  if (timeoutMs < 0) {
    throw std::invalid_argument("timeout must be non-negative");
  }

  // 使用tcp编程，完成rpc方法的远程调用，使用的是长连接服用，因此每次都要重新连接上去，待改成长连接。
  // 没有连接时由 Connect 建立连接，断开后的逻辑重试由上层负责。
  // 读取配置文件rpcserver的信息
  // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
  // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
  // rpc调用方想调用service_name的method_name服务，需要查询zk上该服务所在的host信息
  //  /UserServiceRpc/Login
  if (!connectNow) {
    return;
  }  //可以允许延迟连接
  std::string errMsg;
  if (!Connect(&errMsg)) {
    std::cout << errMsg << std::endl;
  }
}

MprpcChannel::~MprpcChannel() { CloseSocket(&m_clientFd); }
