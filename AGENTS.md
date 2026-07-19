# Repository Guidelines

## Project Structure & Module Organization

This is a C++20/CMake learning implementation of a Raft-backed key-value store. Source modules live in `src/`: `raftCore` contains Raft and the KV state machine, `raftClerk` contains the client, `rpc` and `raftRpcPro` provide RPC/protocol code, `fiber` provides the runtime, and `skipList` contains the storage exercise. Public headers belong in each module's `include/` directory. Runnable demonstrations are in `example/`; small exploratory checks are in `test/`; architectural notes and images are in `docs/`.

Treat `.proto` files as the source of truth. Do not hand-edit generated `*.pb.h` or `*.pb.cc` files.

## Build, Test, and Development Commands

Configure an out-of-tree build, then compile all examples:

```bash
cmake -S . -B build
cmake --build build -j
```

Executables are written to `bin/`, including `raftCoreRun`, `callerMain`, and fiber examples such as `test_scheduler`. Run a specific target with `cmake --build build --target test_scheduler`, then execute its binary from `bin/`. The project depends on CMake 3.22+, a C++20 compiler, Protobuf, Muduo, Boost serialization, and pthread/dl.

Format C/C++ sources before review with `cmake --build build --target format`. This invokes `format.sh` and excludes generated Protobuf files.

## Coding Style & Naming Conventions

Follow `.clang-format`: Google-derived C++ style, two-space indentation, attached braces, left-aligned pointers, and a 120-column limit. Keep existing naming conventions: implementation files use lowercase camel case (for example, `raftServerRpcUtil.cpp`), headers match their component, and CMake targets use descriptive lower snake case or existing target names. Prefer module-local headers and explicit dependencies over adding new global include paths.

## Testing Guidelines

There is no automated unit-test framework or coverage threshold. Add focused executable checks beside the relevant `example/` or `test/` code and name them `test_<behavior>.cpp` when appropriate. Validate distributed changes with multiple nodes, leadership changes, retries, restart recovery, and snapshot behavior. For standalone compiler checks, use C++20, e.g. `g++ -std=c++20 test/defer_run.cpp -o /tmp/defer_run`.

## Commit & Pull Request Guidelines

Existing commits use short Chinese imperative summaries, such as `添加项目知识图谱与学习导览`; keep commits concise and scoped. In pull requests, explain the affected Raft/RPC path, list build or runtime checks performed, and link related issues when available. Include logs or screenshots only when they clarify observable behavior. Avoid committing `bin/`, `lib/`, `build/`, or IDE build directories.
