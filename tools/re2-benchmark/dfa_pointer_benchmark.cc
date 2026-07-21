#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr int kTextLength = 16 << 20;

std::vector<int> ParseCounts(const char* variable, const char* default_value) {
  const char* value = std::getenv(variable);
  std::stringstream input(value == nullptr ? default_value : value);
  std::vector<int> counts;
  std::string token;
  while (std::getline(input, token, ',')) {
    int count = std::stoi(token);
    if (count <= 0) {
      throw std::invalid_argument(std::string(variable) +
                                  " contains a non-positive value");
    }
    counts.push_back(count);
  }
  if (counts.empty()) {
    throw std::invalid_argument(std::string(variable) + " is empty");
  }
  return counts;
}

int NextStateIndex(int state_index, int byte_class, int state_count) {
  return ((state_index * 33) + byte_class + 1) % state_count;
}

uintptr_t LoadWithSeparateAddress(uintptr_t row_address, uint8_t byte_class) {
#if defined(__aarch64__)
  uintptr_t transition_address;
  uintptr_t byte_class_index = byte_class;
  asm volatile("add %0, %1, %2, lsl #3"
               : "=&r"(transition_address)
               : "r"(row_address), "r"(byte_class_index));
  return *reinterpret_cast<const uintptr_t*>(transition_address);
#else
  return reinterpret_cast<const uintptr_t*>(row_address)[byte_class];
#endif
}

template <bool separate_address>
void DfaAbsolutePointers(benchmark::State& state, int state_count,
                         int class_count) {
  std::vector<uint8_t> text(kTextLength);
  uint64_t random_state = 1;
  for (uint8_t& value : text) {
    random_state ^= random_state << 13;
    random_state ^= random_state >> 7;
    random_state ^= random_state << 17;
    value = static_cast<uint8_t>(random_state);
  }

  std::array<uint8_t, 256> byte_map{};
  for (int value = 0; value < static_cast<int>(byte_map.size()); value++) {
    byte_map[value] = static_cast<uint8_t>(value % class_count);
  }

  // Row zero is reserved so zero remains the abnormal-transition sentinel.
  std::vector<uintptr_t> transitions(
      static_cast<size_t>(state_count + 1) * class_count);
  for (int state_index = 0; state_index < state_count; state_index++) {
    int state_id = state_index + 1;
    for (int byte_class = 0; byte_class < class_count; byte_class++) {
      int next_state_id =
          NextStateIndex(state_index, byte_class, state_count) + 1;
      transitions[(static_cast<size_t>(state_id) * class_count) + byte_class] =
          reinterpret_cast<uintptr_t>(
              transitions.data() +
              (static_cast<size_t>(next_state_id) * class_count));
    }
  }

  uintptr_t initial_row_address = reinterpret_cast<uintptr_t>(
      transitions.data() + class_count);
  for (auto _ : state) {
    uintptr_t transition_row_address = initial_row_address;
    for (uint8_t value : text) {
      uintptr_t next_transition_row_address;
      if constexpr (separate_address) {
        next_transition_row_address =
            LoadWithSeparateAddress(transition_row_address, byte_map[value]);
      } else {
        auto* transition_row =
            reinterpret_cast<const uintptr_t*>(transition_row_address);
        next_transition_row_address = transition_row[byte_map[value]];
      }
      if (next_transition_row_address == 0) {
        break;
      }
      transition_row_address = next_transition_row_address;
    }
    benchmark::DoNotOptimize(transition_row_address);
  }
  state.SetBytesProcessed(state.iterations() * text.size());
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::vector<int> state_counts = ParseCounts(
        "DFA_LAYOUT_STATE_COUNTS",
        "32,64,96,128,160,192,224,256,320,384,448,512");
    std::vector<int> class_counts =
        ParseCounts("DFA_LAYOUT_CLASS_COUNTS", "29");
    for (int state_count : state_counts) {
      if (state_count >= 65535) {
        throw std::invalid_argument("DFA_LAYOUT_STATE_COUNTS exceeds 65534");
      }
      for (int class_count : class_counts) {
        if (class_count > 256) {
          throw std::invalid_argument("DFA_LAYOUT_CLASS_COUNTS exceeds 256");
        }
        std::string name = "DfaAbsolutePointers/" +
                           std::to_string(state_count) + "/" +
                           std::to_string(class_count);
        benchmark::RegisterBenchmark(
            name.c_str(),
            [=](benchmark::State& state) {
              DfaAbsolutePointers<false>(state, state_count, class_count);
            })
            ->Unit(benchmark::kNanosecond);
        name = "DfaAbsolutePointersSeparateAddress/" +
               std::to_string(state_count) + "/" +
               std::to_string(class_count);
        benchmark::RegisterBenchmark(
            name.c_str(),
            [=](benchmark::State& state) {
              DfaAbsolutePointers<true>(state, state_count, class_count);
            })
            ->Unit(benchmark::kNanosecond);
      }
    }
  } catch (const std::exception& exception) {
    std::fprintf(stderr, "Invalid DFA layout parameters: %s\n",
                 exception.what());
    return 1;
  }

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
