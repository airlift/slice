# RE2 Port Project

This project is a faithful port of Google RE2 to Java byte[] for Slice.

## Key Principle

**Follow upstream RE2 behavior by default.** When deviating, document in `RE2_DECISIONS.md`.

## Quick Reference

```bash
# Run all RE2 tests
./mvnw "-Dtest=**/re2/**/Test*" test

# Compile for benchmarking
./mvnw test-compile -q

# Set classpath via Maven (reliable, handles all dependencies)
CP="target/test-classes:target/classes:$(./mvnw -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

# Run a benchmark class
java --add-modules jdk.incubator.vector -cp "$CP" \
  io.airlift.slice.re2.BenchmarkRe2Search
```

## Documentation

**Root directory (frequently used):**
- `RE2_DECISIONS.md` -- Documented deviations from upstream RE2
- `RE2_TASKS.md` -- Authoritative status, resume point, and work tracker

**docs/ directory (reference):**
- `docs/README.md` -- Documentation index and authority rules
- `docs/porting/WORKFLOW.md` -- Development workflow and conventions
- `docs/integrations/TRINO.md` -- Current public API and Trino design research
- `docs/benchmarks/METHODOLOGY.md` -- Benchmarking guide and lessons learned
- `docs/benchmarks/history/` -- Historical results and campaign evidence
- `docs/audits/PROCEDURES.md` -- Audit and verification procedures

## References

ALWAYS SEE:

- `docs/development-philosophy.md`
- `docs/coding-standards.md`
- `docs/testing.md`
- `docs/git.md`

Conditional:

- When adding, removing, or rewriting code comments, SEE: `docs/comments.md`
- When adding or renaming variables/methods/types, SEE: `docs/naming.md`

## Build & Test

This project uses Maven with Maven Daemon (`mvnd`) for faster local iteration.

```bash
mvnd clean install                                         # full build + tests
mvnd test -pl <module>                                    # test one module
mvnd test -pl <module> -Dtest=TestClassName              # test one class
mvnd test -pl <module> -Dtest=TestClassName#testMethod   # test one method
mvnd clean install -DskipTests                            # build without tests
```

Use the `./mvnw` quick-reference commands above when running benchmark-specific flows.

## Verification

Before considering work complete:

1. Run `mvnd test -pl <module>` for each module changed.
2. If changes span multiple modules, run `mvnd clean install`.
3. Ensure every commit builds cleanly and tests pass.

Machine-specific preferences can be added to `CLAUDE.local.md` or `AGENTS.override.md` (neither checked in).

## Skills

Use this file as the canonical skill registry for this repository.

### Available skills

- `java-perf-analysis`: Advanced Java performance analysis (critical path, JIT assembly, cycle budgets, benchmarking, optimization patterns).
  - Path: `.claude/skills/java-perf-analysis/SKILL.md`

### Skill trigger rules

- If a task is about performance gaps, benchmarking, assembly/JIT output, cycle accounting, or optimization strategy, use `java-perf-analysis`.
- If a task explicitly mentions `java-perf-analysis`, always use it.
- If the skill file is missing/unreadable, continue with best-effort local analysis and state that briefly.

## Implementation Standards

- **No compatibility transition needed for cleanup refactors.** This code tree is new and not published; prefer direct renames/cleanup over temporary alias or bridge APIs.
- **Features must be fully implemented.** A fix that only works for one mode (e.g., LATIN1) but not the default mode (UTF-8) is NOT complete. Do not claim something is "fixed" unless it works for all supported configurations.
- **Verify against the actual benchmark.** If a plan targets a specific benchmark, the fix must improve that exact benchmark, not a variant of it.
- **Default mode is UTF-8.** Most patterns compile with UTF-8 encoding by default. Any optimization must work for UTF-8 programs, not just LATIN1.
- **100% upstream compatibility required.** Do not skip features or document gaps as "known limitations." Every upstream feature must be implemented. If something is complex, plan how to implement it, not how to avoid it.
- **Test-first for optimizations.** Before fixing any performance issue:
  1. Write a failing test that demonstrates the problem
  2. Test must directly verify the code path (e.g., sentinel returned), not timing
  3. Test must be based on C++ behavior (golden test or generated expected results)
  4. Implement the fix
  5. Test must pass - fix is NOT complete until test passes
  See `docs/audits/PROCEDURES.md` for detailed guidance.

## Upstream Source

- Pinned commit: `972a15cedd008d846f1a39b2e88ce48d7f166cbd`
- Reproducible fetch/build: `tools/re2-golden/build.sh`
- Fetched source location: `target/re2-golden-dependencies/re2/`
