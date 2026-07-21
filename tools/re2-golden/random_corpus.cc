#include "random_corpus.h"

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "re2/re2.h"
#include "re2/testing/regexp_generator.h"
#include "re2/testing/string_generator.h"

namespace {

constexpr int32_t kRegexpSeed = 404;
constexpr int kRegexpConstructionCount = 100;
constexpr int32_t kStringSeed = 200;
constexpr int kStringCount = 100;
constexpr int kWrapperCount = 4;
constexpr int kMaxSubmatch = 1 + 16;

const char* const kWrappers[] = {
    "none",
    "both",
    "start",
    "end",
};

struct GroupConfig {
  std::string name;
  int max_atoms;
  int max_ops;
  std::vector<std::string> atoms;
  std::vector<std::string> ops;
  int max_string_length;
  std::vector<std::string> string_alphabet;
};

std::string hex_encode(absl::string_view value) {
  if (value.empty()) {
    return "-";
  }

  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (unsigned char byte : value) {
    output << std::setw(2) << static_cast<int>(byte);
  }
  return output.str();
}

std::string group_offsets(absl::string_view text,
                          const std::vector<absl::string_view>& groups) {
  std::ostringstream output;
  for (size_t index = 0; index < groups.size(); index++) {
    if (index != 0) {
      output << ',';
    }
    if (groups[index].data() == nullptr) {
      output << "-1:-1";
      continue;
    }
    size_t start = static_cast<size_t>(groups[index].data() - text.data());
    output << start << ':' << start + groups[index].size();
  }
  return output.str();
}

GroupConfig find_group(const std::string& name) {
  const std::vector<std::string>& egrep_ops = re2::RegexpGenerator::EgrepOps();
  std::vector<GroupConfig> groups = {
      {"small_egrep_literals", 5, 5, re2::Explode("abc."), egrep_ops,
       15, re2::Explode("abc")},
      {"big_egrep_literals", 10, 10, re2::Explode("abc."), egrep_ops,
       15, re2::Explode("abc")},
      {"small_egrep_captures", 5, 5, re2::Split(" ", "a (b) ."), egrep_ops,
       15, re2::Explode("abc")},
      {"big_egrep_captures", 10, 10, re2::Split(" ", "a (b) ."), egrep_ops,
       15, re2::Explode("abc")},
      {"complicated", 10, 10,
       re2::Split(" ",
                  ". (?:^) (?:$) \\a \\f \\n \\r \\t \\v "
                  "\\d \\D \\s \\S \\w \\W (?:\\b) (?:\\B) "
                  "a (a) b c - \\\\"),
       re2::Split(" ",
                  "%s%s %s|%s %s* %s*? %s+ %s+? %s? %s?? "
                  "%s{0} %s{0,} %s{1} %s{1,} %s{0,1} %s{0,2} %s{1,2} "
                  "%s{2} %s{2,} %s{3,4} %s{4,5}"),
       20, re2::Explode("abc123\001\002\003\t\r\n\v\f\a")},
  };

  auto selected = std::find_if(groups.begin(), groups.end(), [&](const GroupConfig& group) {
    return group.name == name;
  });
  if (selected == groups.end()) {
    throw std::invalid_argument("unknown random corpus group: " + name);
  }
  return *selected;
}

class CorpusGenerator final : public re2::RegexpGenerator {
 public:
  CorpusGenerator(const GroupConfig& config, std::ostream& output)
      : RegexpGenerator(config.max_atoms, config.max_ops, config.atoms, config.ops),
        config_(config),
        strings_(config.max_string_length, config.string_alphabet),
        output_(output) {}

  void Generate() {
    output_ << "# pinned-re2=972a15cedd008d846f1a39b2e88ce48d7f166cbd\n"
            << "# upstream-null-input=java-not-applicable\n"
            << "# group\tregexp-seed\tconstruction-id\tregexp-case-id\twrapper"
               "\tstring-seed\ttext-case-id\tcase-id\tregexp-hex\ttext-hex"
               "\tmatched\tgroup-count\tgroups\n";
    GenerateRandom(kRegexpSeed, kRegexpConstructionCount);
    if (regexp_case_id_ != kRegexpConstructionCount * kWrapperCount) {
      throw std::runtime_error("unexpected generated regexp count");
    }
  }

  void HandleRegexp(const std::string& regexp_value) override {
    int construction_id = regexp_case_id_ / kWrapperCount;
    int wrapper_id = regexp_case_id_ % kWrapperCount;

    re2::RE2::Options options;
    options.set_log_errors(false);
    re2::RE2 regexp(regexp_value, options);
    if (!regexp.ok()) {
      throw std::runtime_error("generated regexp did not compile: " + regexp.error());
    }

    int group_count = std::min(kMaxSubmatch, regexp.NumberOfCapturingGroups() + 1);
    strings_.Reset();
    strings_.Random(kStringSeed, kStringCount);
    for (int text_case_id = 0; text_case_id < kStringCount; text_case_id++) {
      if (!strings_.HasNext()) {
        throw std::runtime_error("random string sequence ended early");
      }
      absl::string_view text = strings_.Next();
      std::vector<absl::string_view> groups(static_cast<size_t>(group_count));
      bool matched = regexp.Match(text, 0, text.size(), re2::RE2::UNANCHORED,
                                  groups.data(), group_count);

      std::ostringstream case_id;
      case_id << config_.name << "-r" << std::setw(3) << std::setfill('0')
              << construction_id << "-w" << wrapper_id << "-t"
              << std::setw(3) << text_case_id;
      output_ << config_.name << '\t'
              << kRegexpSeed << '\t'
              << construction_id << '\t'
              << regexp_case_id_ << '\t'
              << kWrappers[wrapper_id] << '\t'
              << kStringSeed << '\t'
              << text_case_id << '\t'
              << case_id.str() << '\t'
              << hex_encode(regexp_value) << '\t'
              << hex_encode(text) << '\t'
              << (matched ? "true" : "false") << '\t'
              << group_count << '\t'
              << (matched ? group_offsets(text, groups) : "-") << '\n';
    }
    if (strings_.HasNext()) {
      throw std::runtime_error("random string sequence exceeded expected count");
    }
    regexp_case_id_++;
  }

 private:
  const GroupConfig& config_;
  re2::StringGenerator strings_;
  std::ostream& output_;
  int regexp_case_id_ = 0;
};

}  // namespace

void generate_random_corpus(const std::string& group, std::ostream& output) {
  GroupConfig config = find_group(group);
  CorpusGenerator generator(config, output);
  generator.Generate();
}
