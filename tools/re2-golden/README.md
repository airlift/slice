# RE2 Golden Generator

This directory contains native maintenance tools built against upstream RE2.
They are **not** part of the Maven build, CI environment, published JAR, or Java
runtime. No production code loads them through JNI or the Foreign Function and
Memory API.

The tools have two purposes:

- `re2_golden` is a correctness oracle. It generates expected results that are
  committed under `src/test/resources` and consumed by ordinary Java tests.
- `re2_count_diagnostics` and `re2_capture_diagnostics` are native reference
  programs used by the benchmark tooling. Their results are qualification
  evidence, not test fixtures.

## Pinned dependencies

The build scripts fetch exact source revisions into `target/`:

- RE2: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`
- Abseil: `d38452e1ee03523a208362186fd42248ff2609f6`

Override `RE2_GOLDEN_DEPENDENCY_DIR`, `RE2_DIR`, or `ABSL_DIR` to use an
existing verified checkout.

## Prerequisites

- Git
- CMake 3.20 or newer
- A C++17 compiler

## Build

```bash
./build.sh
```

This builds `target/re2-golden-build/re2_golden` from pinned source dependencies.

The diagnostic executables are built explicitly by the benchmark automation
with the native compiler flags recorded in its result manifest. They are not
built by `build.sh`.

## Usage

### Compile dump goldens

```bash
../../target/re2-golden-build/re2_golden \
    --mode=compile_dump --flags=0x0 \
    < patterns.txt \
    > compile_dump.jsonl
```

- `patterns.txt` is one UTF-8 pattern per line.
- `flags` is the numeric parse flag mask from `re2/regexp.h` (e.g.,
  `Regexp::LikePerl | Regexp::Latin1`).
- The JSONL output encodes flags as lowercase hex with a `0x` prefix.
- Output is JSONL with fields: `pattern`, `flags`, `dump`, `ok`, `error`.

### Byte map goldens

```bash
../../target/re2-golden-build/re2_golden --mode=bytemap --flags=0x0 < patterns.txt > bytemap.jsonl
```

- Output is JSONL with fields: `pattern`, `flags`, `bytemap`, `ok`, `error`.

### Public match goldens

```bash
../../target/re2-golden-build/re2_golden --mode=match < match-cases.tsv > match-cases.jsonl
```

Each non-comment input line has nine tab-separated fields:

1. stable case identifier
2. pattern bytes as hexadecimal, or `-` for empty
3. text bytes as hexadecimal, or `-` for empty
4. inclusive search start offset
5. exclusive search end offset
6. anchor: `unanchored`, `start`, or `both`
7. encoding: `utf8` or `latin1`
8. longest-match option: `true` or `false`
9. requested group count, including group zero

Hex encoding allows the corpus to contain arbitrary bytes without conversion
through a platform string encoding. Output group ranges use
`start:end,start:end` with `-1:-1` for unmatched groups.

### Committed fixtures

Run these commands from the repository root after building the generator. The
compile dump is generated from its committed pattern list:

```bash
GENERATOR=target/re2-golden-build/re2_golden

"${GENERATOR}" --mode=compile_dump --flags=0x220 \
    < src/test/resources/io/airlift/slice/re2/prog/upstream_compile_dump_patterns.txt \
    > src/test/resources/io/airlift/slice/re2/prog/upstream_compile_dump.jsonl
```

The byte-map fixture contains cases using three parse-flag combinations:

```bash
GENERATOR=target/re2-golden-build/re2_golden

{
    printf '%s\n' '.' '[0-9A-Fa-f]+' | "${GENERATOR}" --mode=bytemap --flags=0x220
    printf '%s\n' '\b' '[^_]' | "${GENERATOR}" --mode=bytemap --flags=0x7b4
    printf '%s\n' '.' | "${GENERATOR}" --mode=bytemap --flags=0x200
} > src/test/resources/io/airlift/slice/re2/prog/upstream_compile_bytemap.jsonl
```

The public-match fixture is generated from the committed byte-oriented case
table:

```bash
GENERATOR=target/re2-golden-build/re2_golden

"${GENERATOR}" --mode=match \
    < tools/re2-golden/public-match-cases.tsv \
    > src/test/resources/io/airlift/slice/re2/upstream_public_match.jsonl
```

The remaining native corpora have dedicated scripts that build the generator
and replace their corresponding resources:

```bash
tools/re2-golden/generate-longest-boundary-corpus.sh
tools/re2-golden/generate-random-corpus.sh
```

`generate-longest-boundary-corpus.sh` owns
`testing/native_longest_boundary.jsonl`. `generate-random-corpus.sh` owns the
five `testing/random_agreement_*.tsv.gzip` resources.

## Maintenance triggers

Regenerate the affected fixtures when:

- the pinned RE2 or Abseil revision changes;
- a committed oracle input or corpus definition changes;
- a correctness investigation needs a new native expected result; or
- oracle behavior, output format, or generation logic changes.

Do not regenerate fixtures merely because the JDK, Java implementation, or
Maven build changes. In particular, never rewrite a golden result to agree with
the Java implementation without first establishing that pinned native RE2
produces the new result.

Run the native diagnostic programs and the applicable Intel and Graviton
benchmark campaign when:

- a matching hot loop, capture path, count pipeline, DFA representation, memory
  budget, or engine-selection rule changes;
- the supported JDK or target processor generation changes materially;
- the pinned RE2 revision used as the native performance baseline changes; or
- formal release qualification is being refreshed.

A fixture-only coverage addition does not require benchmarking unless it also
changes production behavior or performance-sensitive code.

## Updating the upstream pin

Treat an upstream-pin change as a deliberate compatibility update, not routine
fixture churn:

1. Review the upstream commits between the old and new revisions for semantic,
   API, Unicode, compiler, and execution changes.
2. Update the exact revisions in `fetch-dependencies.sh`, then update every
   documented pin and benchmark manifest that identifies the native baseline.
3. Build the oracle in a clean dependency and build directory. The fetch script
   verifies that each checkout resolves to the requested commit.
4. Regenerate every committed native fixture using the commands above.
5. Inspect the complete fixture diff. Explain every changed native result from
   an upstream change or an intentional corpus change; do not accept unexplained
   drift.
6. Run the focused fixture consumers, the complete RE2 test selector, and the
   repository build.
7. Refresh the native comparison on dedicated Intel and Graviton hosts before
   treating the new revision as the performance baseline.

Use a new `RE2_GOLDEN_DEPENDENCY_DIR` and `RE2_GOLDEN_BUILD_DIR`, or remove only
the generated `target/re2-golden-*` directories, when a clean native build is
required. Never commit fetched dependencies or native build output.

## Review checklist

For any oracle or fixture change, verify that:

- the input corpus and generated output change together when applicable;
- regeneration is deterministic and a second run produces no diff;
- arbitrary bytes remain encoded as hexadecimal rather than converted through
  a platform string encoding;
- fixture size, case-count, and checksum assertions are updated intentionally;
- every unexpected Java/native disagreement is investigated rather than
  normalized into the fixture; and
- `./mvnw "-Dtest=**/re2/**/Test*" test` and `./mvnw clean install` pass.

## Notes

- The generator uses internal RE2 APIs (`re2::Regexp::Parse`, `re2::CompileToProg`).
- It intentionally mirrors upstream `compile_test.cc` behavior.
- This tool is optional and never run in CI.
