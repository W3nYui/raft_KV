#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>

namespace qps {

enum class OperationType { Get, Put, Append };

struct GeneratedOperation {
  OperationType type;
  std::string key;
  std::string value;
};

class WorkloadGenerator {
 public:
  WorkloadGenerator(std::uint64_t seed, std::size_t workerId, std::size_t keySpace, std::uint32_t getWeight,
                    std::uint32_t putWeight, std::uint32_t appendWeight);

  GeneratedOperation Next();

 private:
  std::mt19937_64 m_random;
  std::size_t m_workerId;
  std::size_t m_sequence;
  std::discrete_distribution<int> m_operation;
  std::uniform_int_distribution<std::size_t> m_key;
};

}  // namespace qps
