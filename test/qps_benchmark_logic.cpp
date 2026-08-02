#include "qpsBenchmarkLogic.h"

#include <cassert>
#include <set>
#include <stdexcept>
#include <string>

namespace {

void AssertInvalidArgument(const std::string& expectedText, auto&& construct) {
  try {
    construct();
    assert(false && "expected std::invalid_argument");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find(expectedText) != std::string::npos);
  }
}

void TestDeterminismAndVariation() {
  qps::WorkloadGenerator first(42, 0, 16, 1, 1, 1);
  qps::WorkloadGenerator second(42, 0, 16, 1, 1, 1);
  std::set<std::string> keys;
  std::set<std::string> values;
  bool sawGet = false;
  bool sawPut = false;
  bool sawAppend = false;

  for (int i = 0; i < 64; ++i) {
    const auto left = first.Next();
    const auto right = second.Next();
    assert(left.type == right.type);
    assert(left.key == right.key);
    assert(left.value == right.value);
    assert(left.key.rfind("key_", 0) == 0);
    assert(left.key != "key_16");
    const auto keyIndex = std::stoull(left.key.substr(4));
    assert(keyIndex < 16);

    keys.insert(left.key);
    values.insert(left.value);
    sawGet = sawGet || left.type == qps::OperationType::Get;
    sawPut = sawPut || left.type == qps::OperationType::Put;
    sawAppend = sawAppend || left.type == qps::OperationType::Append;
  }

  assert(keys.size() > 1);
  assert(values.size() > 1);
  assert(sawGet && sawPut && sawAppend);
}

void TestValueContainsWorkerAndSequence() {
  qps::WorkloadGenerator generator(7, 2, 1, 1, 0, 0);
  const auto first = generator.Next();
  const auto second = generator.Next();

  assert(first.type == qps::OperationType::Get);
  assert(second.type == qps::OperationType::Get);
  assert(first.value.rfind("value_t2_0_", 0) == 0);
  assert(second.value.rfind("value_t2_1_", 0) == 0);
  assert(first.value.size() > std::string("value_t2_0_").size());
  assert(second.value.size() > std::string("value_t2_1_").size());
}

void TestInvalidArguments() {
  AssertInvalidArgument("keySpace", [] { qps::WorkloadGenerator(1, 0, 0, 1, 1, 1); });
  AssertInvalidArgument("weight", [] { qps::WorkloadGenerator(1, 0, 1, 0, 0, 0); });
}

}  // namespace

int main() {
  TestDeterminismAndVariation();
  TestValueContainsWorkerAndSequence();
  TestInvalidArguments();
  return 0;
}
