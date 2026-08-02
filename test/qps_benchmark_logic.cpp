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

void TestExclusiveOperationWeights() {
  qps::WorkloadGenerator putOnly(11, 0, 4, 0, 1, 0);
  qps::WorkloadGenerator appendOnly(11, 0, 4, 0, 0, 1);

  for (int i = 0; i < 32; ++i) {
    assert(putOnly.Next().type == qps::OperationType::Put);
    assert(appendOnly.Next().type == qps::OperationType::Append);
  }
}

void TestNonDefaultOperationWeights() {
  qps::WorkloadGenerator generator(123, 0, 8, 0, 3, 1);
  bool sawPut = false;
  bool sawAppend = false;

  for (int i = 0; i < 64; ++i) {
    const auto operation = generator.Next();
    assert(operation.type != qps::OperationType::Get);
    sawPut = sawPut || operation.type == qps::OperationType::Put;
    sawAppend = sawAppend || operation.type == qps::OperationType::Append;
  }

  assert(sawPut);
  assert(sawAppend);
}

void TestWorkerIdsHaveIndependentSequences() {
  qps::WorkloadGenerator firstWorker(99, 1, 16, 1, 1, 1);
  qps::WorkloadGenerator secondWorker(99, 2, 16, 1, 1, 1);
  bool randomSequenceDiffers = false;

  for (int i = 0; i < 16; ++i) {
    const auto first = firstWorker.Next();
    const auto second = secondWorker.Next();
    assert(first.value.rfind("value_t1_", 0) == 0);
    assert(second.value.rfind("value_t2_", 0) == 0);
    randomSequenceDiffers = randomSequenceDiffers || first.type != second.type || first.key != second.key;
  }

  assert(randomSequenceDiffers);
}

void TestInvalidArguments() {
  AssertInvalidArgument("keySpace", [] { qps::WorkloadGenerator(1, 0, 0, 1, 1, 1); });
  AssertInvalidArgument("weight", [] { qps::WorkloadGenerator(1, 0, 1, 0, 0, 0); });
}

}  // namespace

int main() {
  TestDeterminismAndVariation();
  TestValueContainsWorkerAndSequence();
  TestExclusiveOperationWeights();
  TestNonDefaultOperationWeights();
  TestWorkerIdsHaveIndependentSequences();
  TestInvalidArguments();
  return 0;
}
