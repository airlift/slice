// Copyright 2026 Airlift authors
// Licensed under the Apache License, Version 2.0 (the "License");

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "absl/strings/string_view.h"
#include "re2/prog.h"
#include "re2/re2.h"
#include "re2/regexp.h"

namespace {

struct Benchmark {
    std::string name;
    std::string model;
    std::string pattern;
    std::string haystack;
    bool case_sensitive = true;
    bool unicode = false;
    std::int64_t maximum_iterations = 0;
    std::int64_t maximum_warmup_iterations = 0;
    std::int64_t maximum_time_nanoseconds = 0;
    std::int64_t maximum_warmup_time_nanoseconds = 0;
};

struct RegexpDeleter {
    void operator()(re2::Regexp* regexp) const
    {
        if (regexp != nullptr) {
            regexp->Decref();
        }
    }
};

using RegexpPointer = std::unique_ptr<re2::Regexp, RegexpDeleter>;

struct CompiledBenchmark {
    Benchmark benchmark;
    re2::RE2 public_pattern;
    RegexpPointer regexp;
    std::unique_ptr<re2::Prog> program;
    std::vector<absl::string_view> lines;
    int group_count;

    explicit CompiledBenchmark(Benchmark benchmark)
        : benchmark(std::move(benchmark)),
          public_pattern(this->benchmark.pattern, options(this->benchmark)),
          regexp(parse(this->benchmark)),
          program(regexp->CompileToProg(public_pattern.options().max_mem() * 2 / 3)),
          lines(split_lines(this->benchmark.haystack)),
          group_count(regexp->NumCaptures() + 1)
    {
        if (!public_pattern.ok()) {
            std::cerr << "Unable to compile public pattern: " << public_pattern.error() << '\n';
            std::exit(1);
        }
        if (program == nullptr) {
            std::cerr << "Unable to compile diagnostic program\n";
            std::exit(1);
        }
        program->IsOnePass();
    }

    static re2::RE2::Options options(const Benchmark& benchmark)
    {
        re2::RE2::Options options;
        options.set_log_errors(false);
        options.set_case_sensitive(benchmark.case_sensitive);
        if (!benchmark.unicode) {
            options.set_encoding(re2::RE2::Options::EncodingLatin1);
        }
        return options;
    }

    static RegexpPointer parse(const Benchmark& benchmark)
    {
        re2::Regexp::ParseFlags flags = re2::Regexp::LikePerl;
        if (!benchmark.unicode) {
            flags = static_cast<re2::Regexp::ParseFlags>(flags | re2::Regexp::Latin1);
        }
        if (!benchmark.case_sensitive) {
            flags = static_cast<re2::Regexp::ParseFlags>(flags | re2::Regexp::FoldCase);
        }
        re2::RegexpStatus status;
        RegexpPointer regexp(re2::Regexp::Parse(benchmark.pattern, flags, &status));
        if (regexp == nullptr) {
            std::cerr << "Unable to parse diagnostic pattern: " << status.Text() << '\n';
            std::exit(1);
        }
        return regexp;
    }

    static std::vector<absl::string_view> split_lines(const std::string& haystack)
    {
        std::vector<absl::string_view> lines;
        std::size_t line_start = 0;
        while (line_start < haystack.size()) {
            std::size_t line_end = haystack.find('\n', line_start);
            if (line_end == std::string::npos) {
                line_end = haystack.size();
            }
            std::size_t content_end = line_end;
            if (content_end > line_start && haystack[content_end - 1] == '\r') {
                content_end--;
            }
            lines.emplace_back(haystack.data() + line_start, content_end - line_start);
            line_start = line_end + 1;
        }
        return lines;
    }
};

std::int64_t parse_number(std::string_view value)
{
    std::string text(value);
    std::size_t parsed = 0;
    std::int64_t result = std::stoll(text, &parsed);
    if (parsed != text.size()) {
        std::cerr << "Invalid numeric KLV value\n";
        std::exit(1);
    }
    return result;
}

bool parse_boolean(std::string_view value)
{
    if (value == "true") {
        return true;
    }
    if (value == "false") {
        return false;
    }
    std::cerr << "Invalid boolean KLV value\n";
    std::exit(1);
}

Benchmark read_benchmark()
{
    std::string input((std::istreambuf_iterator<char>(std::cin)), std::istreambuf_iterator<char>());
    Benchmark benchmark;
    std::size_t position = 0;
    while (position < input.size()) {
        std::size_t key_end = input.find(':', position);
        std::size_t length_end = key_end == std::string::npos ? key_end : input.find(':', key_end + 1);
        if (key_end == std::string::npos || length_end == std::string::npos) {
            std::cerr << "Invalid KLV header\n";
            std::exit(1);
        }
        std::string_view key(input.data() + position, key_end - position);
        std::size_t length = static_cast<std::size_t>(parse_number(
            std::string_view(input.data() + key_end + 1, length_end - key_end - 1)));
        std::size_t value_start = length_end + 1;
        std::size_t next_position = value_start + length;
        if (next_position >= input.size() || input[next_position] != '\n') {
            std::cerr << "Invalid KLV value length\n";
            std::exit(1);
        }
        std::string_view value(input.data() + value_start, length);
        if (key == "name") {
            benchmark.name.assign(value);
        }
        else if (key == "model") {
            benchmark.model.assign(value);
        }
        else if (key == "pattern") {
            benchmark.pattern.assign(value);
        }
        else if (key == "haystack") {
            benchmark.haystack.assign(value);
        }
        else if (key == "case-insensitive") {
            benchmark.case_sensitive = !parse_boolean(value);
        }
        else if (key == "unicode") {
            benchmark.unicode = parse_boolean(value);
        }
        else if (key == "max-iters") {
            benchmark.maximum_iterations = parse_number(value);
        }
        else if (key == "max-warmup-iters") {
            benchmark.maximum_warmup_iterations = parse_number(value);
        }
        else if (key == "max-time") {
            benchmark.maximum_time_nanoseconds = parse_number(value);
        }
        else if (key == "max-warmup-time") {
            benchmark.maximum_warmup_time_nanoseconds = parse_number(value);
        }
        else {
            std::cerr << "Unknown KLV key: " << key << '\n';
            std::exit(1);
        }
        position = next_position + 1;
    }
    if (benchmark.model != "grep-captures") {
        std::cerr << "Capture diagnostic requires grep-captures\n";
        std::exit(1);
    }
    return benchmark;
}

std::int64_t count_participating_groups(const std::vector<absl::string_view>& groups)
{
    std::int64_t count = 0;
    for (absl::string_view group : groups) {
        if (group.data() != nullptr) {
            count++;
        }
    }
    return count;
}

std::int64_t run_public(CompiledBenchmark& compiled)
{
    std::vector<absl::string_view> groups(compiled.group_count);
    std::int64_t count = 0;
    for (absl::string_view line : compiled.lines) {
        int search_start = 0;
        while (search_start <= line.size()) {
            bool matched = compiled.public_pattern.Match(
                line,
                search_start,
                static_cast<int>(line.size()),
                re2::RE2::UNANCHORED,
                groups.data(),
                compiled.group_count);
            if (!matched) {
                break;
            }
            count += count_participating_groups(groups);
            search_start = static_cast<int>(groups[0].data() - line.data() + groups[0].size());
        }
    }
    return count;
}

std::int64_t run_direct_bit_state(CompiledBenchmark& compiled)
{
    if (!compiled.program->CanBitState()) {
        std::cerr << "Program is not eligible for BitState\n";
        std::exit(1);
    }
    std::vector<absl::string_view> groups(compiled.group_count);
    std::int64_t count = 0;
    for (absl::string_view line : compiled.lines) {
        bool matched = compiled.program->SearchBitState(
            line,
            line,
            re2::Prog::kAnchored,
            re2::Prog::kFirstMatch,
            groups.data(),
            compiled.group_count);
        if (matched) {
            count += count_participating_groups(groups);
        }
    }
    return count;
}

using Operation = std::int64_t (*)(CompiledBenchmark&);

void run_stage(CompiledBenchmark& compiled, Operation operation)
{
    std::int64_t expected = operation(compiled);
    auto warmup_start = std::chrono::steady_clock::now();
    for (std::int64_t iteration = 0; iteration < compiled.benchmark.maximum_warmup_iterations; iteration++) {
        if (operation(compiled) != expected) {
            std::cerr << "Warmup result changed\n";
            std::exit(1);
        }
        if (std::chrono::steady_clock::now() - warmup_start >=
            std::chrono::nanoseconds(compiled.benchmark.maximum_warmup_time_nanoseconds)) {
            break;
        }
    }

    auto run_start = std::chrono::steady_clock::now();
    for (std::int64_t iteration = 0; iteration < compiled.benchmark.maximum_iterations; iteration++) {
        auto start = std::chrono::steady_clock::now();
        std::int64_t result = operation(compiled);
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start);
        if (result != expected) {
            std::cerr << "Measured result changed\n";
            std::exit(1);
        }
        std::cout << duration.count() << ',' << result << '\n';
        if (std::chrono::steady_clock::now() - run_start >=
            std::chrono::nanoseconds(compiled.benchmark.maximum_time_nanoseconds)) {
            break;
        }
    }
}

std::string capture_route(const CompiledBenchmark& compiled)
{
    bool one_pass_eligible = compiled.program->IsOnePass() &&
        compiled.group_count <= re2::Prog::kMaxOnePassCapture;
    std::set<std::string> routes;
    for (absl::string_view line : compiled.lines) {
        if (one_pass_eligible) {
            routes.emplace("ONE_PASS");
        }
        else if (compiled.program->CanBitState() &&
                 line.size() <= compiled.program->bit_state_text_max_size()) {
            routes.emplace("BIT_STATE");
        }
        else {
            routes.emplace("NFA");
        }
    }
    std::string result;
    for (const std::string& route : routes) {
        if (!result.empty()) {
            result += '+';
        }
        result += route;
    }
    return result;
}

void write_manifest(CompiledBenchmark& compiled)
{
    bool one_pass = compiled.program->IsOnePass();
    bool one_pass_capture_eligible = one_pass &&
        compiled.group_count <= re2::Prog::kMaxOnePassCapture;
    bool anchored_dfa_skipped = compiled.program->anchor_start() &&
        (one_pass_capture_eligible || compiled.program->CanBitState());
    std::int64_t capture_bytes = 0;
    for (absl::string_view line : compiled.lines) {
        capture_bytes += line.size();
    }

    std::cout << "name=" << compiled.benchmark.name << '\n';
    std::cout << "model=" << compiled.benchmark.model << '\n';
    std::cout << "groups=" << compiled.group_count << '\n';
    std::cout << "program_instructions=" << compiled.program->size() << '\n';
    std::cout << "program_one_pass=" << std::boolalpha << one_pass << '\n';
    std::cout << "one_pass_capture_eligible=" << one_pass_capture_eligible << '\n';
    std::cout << "bit_state_eligible=" << compiled.program->CanBitState() << '\n';
    std::cout << "anchored_dfa_skipped=" << anchored_dfa_skipped << '\n';
    std::cout << "bit_state_lists=" << compiled.program->list_count() << '\n';
    std::cout << "bit_state_text_max_size=" << compiled.program->bit_state_text_max_size() << '\n';
    std::cout << "capture_engine=" << capture_route(compiled) << '\n';
    std::cout << "capture_calls=" << compiled.lines.size() << '\n';
    std::cout << "capture_bytes=" << capture_bytes << '\n';
    std::cout << "public_result=" << run_public(compiled) << '\n';
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "usage: re2_capture_diagnostics --manifest|--stage=public|--stage=bit-state\n";
        return 1;
    }

    CompiledBenchmark compiled(read_benchmark());
    std::string_view command(argv[1]);
    if (command == "--manifest") {
        write_manifest(compiled);
        return 0;
    }
    if (command == "--stage=public") {
        run_stage(compiled, run_public);
        return 0;
    }
    if (command == "--stage=bit-state") {
        run_stage(compiled, run_direct_bit_state);
        return 0;
    }
    std::cerr << "Unknown command: " << command << '\n';
    return 1;
}
