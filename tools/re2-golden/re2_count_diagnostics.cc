// Copyright 2026 Airlift authors
// Licensed under the Apache License, Version 2.0 (the "License");

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include "re2/re2.h"

namespace {

int cache_reset_count;
std::size_t total_states_at_reset;
std::size_t maximum_states_at_reset;
std::int64_t state_budget;

void record_cache_reset(const re2::hooks::DFAStateCacheReset& reset)
{
    cache_reset_count++;
    total_states_at_reset += reset.state_cache_size;
    maximum_states_at_reset = std::max(maximum_states_at_reset, reset.state_cache_size);
    state_budget = reset.state_budget;
}

int count_matches(re2::RE2& pattern, const std::string& haystack)
{
    int position = 0;
    int count = 0;
    while (position <= static_cast<int>(haystack.size())) {
        re2::StringPiece match;
        if (!pattern.Match(haystack, position, static_cast<int>(haystack.size()), re2::RE2::UNANCHORED, &match, 1)) {
            break;
        }
        if (match.empty()) {
            std::cerr << "count diagnostics requires non-empty matches\n";
            std::exit(1);
        }
        position = static_cast<int>(match.data() - haystack.data() + match.size());
        count++;
    }
    return count;
}

void reset_counters()
{
    cache_reset_count = 0;
    total_states_at_reset = 0;
    maximum_states_at_reset = 0;
    state_budget = 0;
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc != 5) {
        std::cerr << "Usage: re2_count_diagnostics <pattern> <haystack-file> <maximum-memory-bytes> <iterations>\n";
        return 1;
    }

    std::ifstream input(argv[2], std::ios::binary);
    if (!input) {
        std::cerr << "Unable to read haystack: " << argv[2] << '\n';
        return 1;
    }
    std::string haystack((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());

    re2::RE2::Options options;
    options.set_encoding(re2::RE2::Options::EncodingLatin1);
    options.set_log_errors(false);
    options.set_max_mem(std::stoll(argv[3]));
    re2::RE2 pattern(argv[1], options);
    if (!pattern.ok()) {
        std::cerr << "Unable to compile pattern\n";
        return 1;
    }

    re2::hooks::SetDFAStateCacheResetHook(record_cache_reset);
    int iterations = std::stoi(argv[4]);
    reset_counters();
    count_matches(pattern, haystack);
    std::cout << "iteration,duration_ns,count,cache_resets,total_states_at_reset,maximum_states_at_reset,state_budget_bytes\n";
    for (int iteration = 0; iteration < iterations; iteration++) {
        reset_counters();
        auto start = std::chrono::steady_clock::now();
        int count = count_matches(pattern, haystack);
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start);
        std::cout << iteration << ','
                  << duration.count() << ','
                  << count << ','
                  << cache_reset_count << ','
                  << total_states_at_reset << ','
                  << maximum_states_at_reset << ','
                  << state_budget << '\n';
    }
}
