//
// Created by swx on 23-6-4.
//
#include <iostream>
#include "clerk.h"
#include "util.h"

void ShowArgsHelp();

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

  Clerk client;
  client.Init(configFileName);

  auto start = now();
  int count = 500;

  while (count--) {
    client.Put("x", std::to_string(count));

    std::string get1 = client.Get("x");
    std::printf("get return :{%s}\r\n", get1.c_str());
  }
  return 0;
}

void ShowArgsHelp() { std::cout << "format: callerMain -f <configFileName>" << std::endl; }