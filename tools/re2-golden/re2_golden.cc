#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "re2/prog.h"
#include "re2/regexp.h"
#include "re2/re2.h"
#include "re2/stringpiece.h"

#include "random_corpus.h"

namespace {

enum class Mode {
  kCompileDump,
  kByteMap,
  kMatch,
  kRandomCorpus,
};

struct RegexpDeleter {
  void operator()(re2::Regexp* re) const {
    if (re) {
      re->Decref();
    }
  }
};

struct Options {
  Mode mode = Mode::kCompileDump;
  int flags = 0;
  std::string group;
};

void usage(const char* argv0) {
  std::cerr << "Usage: " << argv0 << " --mode=compile_dump|bytemap --flags=<int>\n"
            << "       " << argv0 << " --mode=match\n"
            << "       " << argv0 << " --mode=random_corpus --group=<name>\n";
}

bool starts_with(const std::string& s, const char* prefix) {
  return s.rfind(prefix, 0) == 0;
}

std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
      case '\\': out.append("\\\\"); break;
      case '"': out.append("\\\""); break;
      case '\n': out.append("\\n"); break;
      case '\r': out.append("\\r"); break;
      case '\t': out.append("\\t"); break;
      default: out.push_back(c); break;
    }
  }
  return out;
}

Options parse_args(int argc, char** argv) {
  Options options;
  bool have_mode = false;
  bool have_flags = false;

  for (int i = 1; i < argc; i++) {
    std::string arg(argv[i]);
    if (starts_with(arg, "--mode=")) {
      std::string mode = arg.substr(strlen("--mode="));
      if (mode == "compile_dump") {
        options.mode = Mode::kCompileDump;
      } else if (mode == "bytemap") {
        options.mode = Mode::kByteMap;
      } else if (mode == "match") {
        options.mode = Mode::kMatch;
      } else if (mode == "random_corpus") {
        options.mode = Mode::kRandomCorpus;
      } else {
        std::cerr << "Unknown mode: " << mode << "\n";
        usage(argv[0]);
        std::exit(2);
      }
      have_mode = true;
    } else if (starts_with(arg, "--flags=")) {
      std::string value = arg.substr(strlen("--flags="));
      options.flags = std::stoi(value, nullptr, 0);
      have_flags = true;
    } else if (starts_with(arg, "--group=")) {
      options.group = arg.substr(strlen("--group="));
    } else if (arg == "--help" || arg == "-h") {
      usage(argv[0]);
      std::exit(0);
    } else {
      std::cerr << "Unknown arg: " << arg << "\n";
      usage(argv[0]);
      std::exit(2);
    }
  }

  if (!have_mode ||
      (options.mode != Mode::kMatch &&
       options.mode != Mode::kRandomCorpus && !have_flags) ||
      (options.mode == Mode::kRandomCorpus && options.group.empty())) {
    usage(argv[0]);
    std::exit(2);
  }

  return options;
}

int hex_value(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  throw std::invalid_argument("invalid hexadecimal character");
}

std::string decode_hex(const std::string& value) {
  if (value == "-") {
    return std::string();
  }
  if ((value.size() & 1) != 0) {
    throw std::invalid_argument("hexadecimal value has odd length");
  }

  std::string decoded;
  decoded.reserve(value.size() / 2);
  for (size_t index = 0; index < value.size(); index += 2) {
    decoded.push_back(static_cast<char>((hex_value(value[index]) << 4) |
                                        hex_value(value[index + 1])));
  }
  return decoded;
}

std::vector<std::string> split_tabs(const std::string& line) {
  std::vector<std::string> fields;
  size_t start = 0;
  while (true) {
    size_t tab = line.find('\t', start);
    if (tab == std::string::npos) {
      fields.push_back(line.substr(start));
      return fields;
    }
    fields.push_back(line.substr(start, tab - start));
    start = tab + 1;
  }
}

re2::RE2::Anchor parse_anchor(const std::string& value) {
  if (value == "unanchored") {
    return re2::RE2::UNANCHORED;
  }
  if (value == "start") {
    return re2::RE2::ANCHOR_START;
  }
  if (value == "both") {
    return re2::RE2::ANCHOR_BOTH;
  }
  throw std::invalid_argument("unknown anchor: " + value);
}

re2::RE2::Options::Encoding parse_encoding(const std::string& value) {
  if (value == "utf8") {
    return re2::RE2::Options::EncodingUTF8;
  }
  if (value == "latin1") {
    return re2::RE2::Options::EncodingLatin1;
  }
  throw std::invalid_argument("unknown encoding: " + value);
}

bool parse_boolean(const std::string& value) {
  if (value == "true") {
    return true;
  }
  if (value == "false") {
    return false;
  }
  throw std::invalid_argument("expected true or false: " + value);
}

std::string group_offsets(absl::string_view text,
                          const std::vector<absl::string_view>& groups) {
  std::ostringstream out;
  for (size_t index = 0; index < groups.size(); index++) {
    if (index != 0) {
      out << ',';
    }
    const absl::string_view& group = groups[index];
    if (group.data() == nullptr) {
      out << "-1:-1";
      continue;
    }
    size_t start = static_cast<size_t>(group.data() - text.data());
    out << start << ':' << (start + group.size());
  }
  return out.str();
}

void emit_match(const std::vector<std::string>& fields) {
  if (fields.size() != 9) {
    throw std::invalid_argument(
        "match input requires 9 tab-separated fields: "
        "id, pattern hex, text hex, start, end, anchor, encoding, longest, groups");
  }

  const std::string& id = fields[0];
  std::string pattern = decode_hex(fields[1]);
  std::string text_value = decode_hex(fields[2]);
  size_t start = static_cast<size_t>(std::stoull(fields[3]));
  size_t end = static_cast<size_t>(std::stoull(fields[4]));
  re2::RE2::Anchor anchor = parse_anchor(fields[5]);
  re2::RE2::Options::Encoding encoding = parse_encoding(fields[6]);
  bool longest = parse_boolean(fields[7]);
  int group_count = std::stoi(fields[8]);

  if (start > end || end > text_value.size()) {
    throw std::invalid_argument("invalid match window");
  }
  if (group_count < 0) {
    throw std::invalid_argument("group count is negative");
  }

  re2::RE2::Options options;
  options.set_encoding(encoding);
  options.set_longest_match(longest);
  options.set_log_errors(false);
  re2::RE2 regexp(re2::StringPiece(pattern), options);

  std::cout << "{\"id\":\"" << json_escape(id)
            << "\",\"patternHex\":\"" << fields[1]
            << "\",\"textHex\":\"" << fields[2]
            << "\",\"start\":\"" << fields[3]
            << "\",\"end\":\"" << fields[4]
            << "\",\"anchor\":\"" << fields[5]
            << "\",\"encoding\":\"" << fields[6]
            << "\",\"longest\":" << (longest ? "true" : "false")
            << ",\"groupCount\":\"" << fields[8] << "\"";

  if (!regexp.ok()) {
    std::cout << ",\"ok\":false,\"matched\":false,\"groups\":\"\""
              << ",\"error\":\"" << json_escape(regexp.error()) << "\"}\n";
    return;
  }

  absl::string_view text(text_value);
  std::vector<absl::string_view> groups(static_cast<size_t>(group_count));
  bool matched = regexp.Match(text, start, end, anchor, groups.data(), group_count);

  std::cout << ",\"ok\":true,\"matched\":" << (matched ? "true" : "false")
            << ",\"groups\":\"" << (matched ? group_offsets(text, groups) : "")
            << "\",\"error\":null}\n";
}

std::string flags_string(int flags) {
  std::ostringstream out;
  out << "0x" << std::hex << std::nouppercase << flags;
  return out.str();
}

void emit_error(const std::string& pattern, const std::string& flags, const std::string& error) {
  std::cout << "{\"pattern\":\"" << json_escape(pattern)
            << "\",\"flags\":\"" << flags << "\",\"ok\":false,\"error\":\"" << json_escape(error) << "\"}\n";
}

void emit_dump(const std::string& pattern, const std::string& flags, const std::string& dump) {
  std::cout << "{\"pattern\":\"" << json_escape(pattern)
            << "\",\"flags\":\"" << flags << "\",\"ok\":true,\"dump\":\"" << json_escape(dump) << "\"}\n";
}

void emit_bytemap(const std::string& pattern, const std::string& flags, const std::string& bytemap) {
  std::cout << "{\"pattern\":\"" << json_escape(pattern)
            << "\",\"flags\":\"" << flags << "\",\"ok\":true,\"bytemap\":\"" << json_escape(bytemap) << "\"}\n";
}

}  // namespace

int main(int argc, char** argv) {
  Options options = parse_args(argc, argv);
  if (options.mode == Mode::kRandomCorpus) {
    generate_random_corpus(options.group, std::cout);
    return 0;
  }
  std::string flags = flags_string(options.flags);

  std::string line;
  while (std::getline(std::cin, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (!line.empty() && line[0] == '#') {
      continue;
    }

    if (options.mode == Mode::kMatch) {
      try {
        emit_match(split_tabs(line));
      } catch (const std::exception& error) {
        std::cerr << "Invalid match input: " << error.what() << " -> " << line << "\n";
        return 2;
      }
      continue;
    }

    re2::RegexpStatus status;
    std::unique_ptr<re2::Regexp, RegexpDeleter> re(
        re2::Regexp::Parse(re2::StringPiece(line),
                          static_cast<re2::Regexp::ParseFlags>(options.flags),
                          &status));
    if (!status.ok()) {
      emit_error(line, flags, status.Text());
      continue;
    }

    std::unique_ptr<re2::Prog> prog(re->CompileToProg(0));
    if (!prog) {
      emit_error(line, flags, "compile failed");
      continue;
    }

    switch (options.mode) {
      case Mode::kCompileDump:
        emit_dump(line, flags, prog->Dump());
        break;
      case Mode::kByteMap:
        emit_bytemap(line, flags, prog->DumpByteMap());
        break;
      case Mode::kMatch:
      case Mode::kRandomCorpus:
        std::abort();
    }
  }

  return 0;
}
