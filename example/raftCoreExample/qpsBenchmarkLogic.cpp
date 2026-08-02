#include "qpsBenchmarkLogic.h"

#include <sstream>
#include <stdexcept>

namespace {

std::uint64_t DeriveWorkerSeed(std::uint64_t seed, std::size_t workerId) {
  auto mixed = seed + 0x9e3779b97f4a7c15ULL + static_cast<std::uint64_t>(workerId);
  mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
  mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
  return mixed ^ (mixed >> 31);
}

std::discrete_distribution<int> MakeOperationDistribution(std::size_t keySpace, std::uint32_t getWeight,
                                                          std::uint32_t putWeight, std::uint32_t appendWeight) {
  const auto totalWeight = static_cast<std::uint64_t>(getWeight) + static_cast<std::uint64_t>(putWeight) +
                           static_cast<std::uint64_t>(appendWeight);
  if (keySpace == 0) {
    throw std::invalid_argument("keySpace must be greater than zero");
  }
  if (totalWeight == 0) {
    throw std::invalid_argument("operation weights must not all be zero");
  }
  return {{static_cast<double>(getWeight), static_cast<double>(putWeight), static_cast<double>(appendWeight)}};
}

}  // namespace

namespace qps {

WorkloadGenerator::WorkloadGenerator(std::uint64_t seed, std::size_t workerId, std::size_t keySpace,
                                     std::uint32_t getWeight, std::uint32_t putWeight, std::uint32_t appendWeight)
    : m_random(DeriveWorkerSeed(seed, workerId)),
      m_workerId(workerId),
      m_sequence(0),
      m_operation(MakeOperationDistribution(keySpace, getWeight, putWeight, appendWeight)),
      m_key(0, keySpace == 0 ? 0 : keySpace - 1) {}

GeneratedOperation WorkloadGenerator::Next() {
  const auto selectedOperation = m_operation(m_random);
  const auto keyIndex = m_key(m_random);
  const auto sequence = m_sequence++;
  const auto randomPart = m_random();

  std::ostringstream value;
  value << "value_t" << m_workerId << '_' << sequence << '_' << std::hex << randomPart;

  return {static_cast<OperationType>(selectedOperation), "key_" + std::to_string(keyIndex), value.str()};
}

}  // namespace qps
