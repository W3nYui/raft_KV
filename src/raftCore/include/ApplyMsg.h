#ifndef APPLYMSG_H
#define APPLYMSG_H
#include <string>
class ApplyMsg {
 public:
  bool CommandValid;   // 是否是命令
  std::string Command; // 序列化的Op命令
  int CommandIndex;    // 该命令在全局 Raft 日志中的位置
  bool SnapshotValid;  // 是否是快照
  std::string Snapshot;// 快照内容
  int SnapshotTerm;    // 快照Term
  int SnapshotIndex;   // 快照等级

 public:
  //两个valid最开始要赋予false！！
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1){

        };
};

#endif  // APPLYMSG_H