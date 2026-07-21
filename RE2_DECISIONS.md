# RE2 Port Decisions

This file records current intentional differences between the Java API or
implementation and pinned upstream RE2. Superseded experiments belong in Git
history and the historical audit files, not in this document.

The default rule is to preserve upstream behavior. A Java adaptation must not
change which byte strings match, capture boundaries, parse behavior, or resource
fallback semantics.

## Byte-Oriented Java API

- Public patterns, inputs, captures, and rewrite results use `Slice`.
  Convenience overloads that silently convert `byte[]` or `String` are
  intentionally absent. Parser, compiler, AST metadata, and execution engines
  also use `Slice` directly.
- Execution engines represent a search window as the original context `Slice`
  plus explicit start and end offsets. This avoids wrapper allocation and
  preserves surrounding context for zero-length windows, which must not be
  collapsed to `Slices.EMPTY_SLICE` when evaluating boundary assertions.
- Compiling copies the usually small pattern bytes. This prevents later mutation
  of the caller's Slice from changing the compiled pattern or `pattern()` result.
- `replace`, `globalReplace`, and `extract` return new byte-backed values instead
  of resizing an input string in place.
- `matchInto` accepts caller-owned `[start,end,...]` capture storage for the
  allocation-sensitive API. `MatchResult` provides the ordinary Java result API,
  including named captures and numeric conversion.
- `Re2Matcher` owns one reusable capture buffer for repeated matching. Callers
  may retain only group zero and a requested prefix of explicit captures when
  complete capture extraction is unnecessary. It advances empty UTF-8 matches
  by one code point, permits one terminal empty match, and exposes zero-copy
  Slice group views. Matchers are mutable and not thread-safe; compiled `Re2`
  instances remain thread-safe.
- No-match is represented by `false` or `null`, depending on the method. Numeric
  conversion failures use `NumberFormatException`.

These choices replace C++ pointer and out-parameter APIs without changing match
semantics.

## Exceptions At API Boundaries

- Invalid patterns throw `RegexpParseException` with the upstream status code,
  error argument, and byte offset when known.
- Program compilation failures throw `RegexpCompileException`;
  `RegexpCompileOutOfMemoryException` identifies an insufficient compile budget.
- Lazy reverse-program compilation still treats an insufficient reverse budget as
  an execution fallback and uses the NFA, matching upstream behavior.
- Native rewrite validation remains internal to the rewrite implementation; it
  is not part of the public Java surface.

The port does not retain invalid `Re2` instances or C++-style success flags after
construction fails.

## JVM-Sourced Unicode

- General categories, scripts, blocks, binary properties, Java properties, and
  simple case equivalence come from the executing JVM's public `Character` APIs.
  Pinned upstream RE2's generated Unicode tables are intentionally not retained.
- Properties are converted lazily to immutable `CharClass` ranges and cached by
  canonical identity. Case equivalence is built once as an immutable cyclic
  lookup table. Matching uses only the resulting ranges and compiled
  instructions; it does not call `Character` or perform property lookup.
- Unicode-version-sensitive results can change when the runtime JDK changes.
  Deployments that require identical Unicode behavior must use equivalent
  supported JDK versions.
- RE2's ASCII definitions of shorthand classes remain regex-language semantics,
  not Unicode data. Java-specific shorthand modes and other syntax extensions
  require separate explicit decisions.

This intentionally differs from pinned upstream's Unicode snapshot while
preserving its matching algorithms, byte semantics, and resource guarantees.

## Trino Compatibility Adapter

- `TrinoRegexp` implements Trino's contains, count, position, extraction, split,
  and replacement behavior over the public Slice API. It does not call execution
  engines directly.
- Trino's Java-style `$g` and `${name}` replacement grammar and adjacent empty
  replacement behavior remain separate from native RE2 rewrite syntax.
- Pattern syntax is RE2 syntax, not the complete Java `Pattern` language. Known
  same-text/different-meaning collisions such as `(?U)` and character-class
  intersection are rejected explicitly.
- Trino's malformed-byte termination probe supplies malformed bytes as both
  pattern and input. The adapter retries only a `BAD_UTF8` pattern in Latin-1
  byte mode; valid patterns always retain normal UTF-8 behavior.

Trino SQL registration, `CHAR` padding, block construction, and error-code
translation belong in the Trino repository rather than this core Slice package.

## Memory Budgets And DFA Caching

- Public compilation defaults to a 96 MiB planning budget. Unlike upstream's
  8 MiB default, this retains representative large Java DFA graphs without
  reset thrashing under both compressed and uncompressed ordinary references.
- Two thirds of the configured budget is assigned to the forward program and one
  third to a lazily compiled reverse program.
- Compilation accounts for the program and list-head storage before assigning the
  remaining budget to DFA caches. Internal compilation with a non-positive budget
  uses upstream's 1 MiB default DFA allowance.
- A forward program divides its DFA allowance between first-match and
  longest-match caches. A reverse longest-match DFA and a set's many-match DFA use
  their program's complete allowance.
- DFA construction derives retained array and object sizes from the running VM.
  Expanded backing arrays and hash-table capacity remain charged after reset;
  only unreachable per-state objects return to the budget. A full cache resets,
  and repeated reset thrashing falls back to the NFA.

`maxMemory` is a planning budget, not an eager allocation or a byte-exact JVM
heap limit. The default is demand-driven, so simple patterns retain only the
storage they use. Object headers, collector metadata, and allocator behavior are
VM-specific; the cache model accounts for reachable Java objects but does not
promise exact process-memory equality.

## Shared DFA Concurrency

Compiled `Re2` instances are safe to share across threads:

- searches register in padded thread-local reader slots before accessing a DFA
  cache and unregister when the search ends
- the global weak reader registry is locked only when a thread first registers
  or stale references are drained; stale-heavy drains replace the backing set so
  a short-lived thread wave does not retain its peak hash table
- warm transition reads are plain loads with no lock or acquire operation in the
  per-byte loop
- cold construction and reset block new readers, wait for active readers to
  quiesce, and replace cache storage under an exclusive mutation lock
- cache generations prevent queued builders from repeating work already
  published by another thread

This preserves upstream's separation between transition access and cache
lifetime synchronization without placing a read lock in Java's hot loop.

## Bounded Paired DFA Transitions

Long generic forward searches may use an immutable depth-two transition table
for small DFA graphs. The table is demand-built only after a search of at least
256 bytes, is published under the existing exclusive cache mutation protocol,
and is charged against the DFA state budget. Normal state allocation discards
the table before reporting budget exhaustion.

Eligibility uses a 64 KiB retained-size estimate with eight-byte references and
includes row overhead. This conservative absolute cap is independent of the
configured memory budget: larger budgets must not enable cache-hostile global
transition expansion. When the complete graph does not fit, partial pairing is
limited to exactly two selected rows and 16 KiB. Selection considers cached
forward entry states before states with observed self-loops. Broader and larger
partial tables remain on the compact path because corpus measurements found no
consistent benefit and one severe state-budget regression.

The paired loop decodes already-computed normal, dead, full-match, and match
transitions directly. After a first-match candidate, it preserves the candidate
while continuing through paired rows and switches to the compact loop at the
same state and byte boundary when a row or transition is unavailable. Cache
exhaustion and every uncomputed transition retain the compact path's reset and
fallback behavior.

After a match transition enters a matching self-loop, the continuation scans
the authoritative primitive transition row directly until a byte changes the
state or stops matching. It updates the match boundary for every consumed byte
and leaves the first exceptional byte to the existing paired decoder. This
avoids repeatedly decoding a deliberately null paired reference without adding
another transition representation.

Some eligible expressions produce repeated matches before pairing can amortize
its entry and terminal handling. After 16 paired searches return within 16
bytes, the DFA retries under the existing exclusive cache protocol, releases
the paired table's complete memory charge, and uses only compact transitions
until the next cache reset. The observation counter is a best-effort performance
hint shared by concurrent readers; races may change when rejection occurs but
cannot change matching behavior. A cache reset clears the hint and allows the
new cache generation to be evaluated again.

## Bounded Absolute-Pointer DFA Sidecar

Eligible forward one-byte DFA searches may use a private 64-bit absolute-pointer
FFM sidecar after the existing paired-table selection does not produce a usable
route. Long searches request it immediately when pairing is unavailable.
Short searches request it after 16 repeated calls, so one isolated short search
does not prevent the same DFA instance from later selecting a paired table. The
observation count is only a performance hint; races can change promotion timing
but not matching behavior. A finite-budget DFA initially allocates a small
row-rounded table and geometrically enlarges it as the graph grows. Each
allocation is limited so that the remaining budget can still hold the minimum
Java state behind every additional native row. The integer and object
transition tables remain authoritative, and every zero native entry delegates
to the unchanged object continuation.

Native access is optional. The restricted everything segment is initialized
only when the containing module has native access enabled; otherwise no native
allocation, warning, or initialization failure occurs. The owning automatic
arena is reachable through each raw traversal. Allocation, backfill,
incremental writes, publication, and reset use the existing exclusive DFA cache
mutation protocol.

The current allocation is permanently charged to the `DfaInstance` budget.
Growth occurs only under the existing exclusive cache-mutation protocol, so the
table may be copied and rebased without an active reader retaining the old
address. The retired automatic arena then becomes unreachable, and geometric
growth bounds the sum of unreclaimed prior allocations below twice the current
allocation. Reset clears and reuses the current segment without returning its
charge. Once an instance selects this sidecar, it does not switch to object rows
as the DFA grows. Failure to fund the next geometric expansion is ordinary DFA
budget exhaustion and follows the existing reset and matcher-fallback policy.
Explicitly unlimited DFAs remain object-based because they have no finite
native-allocation budget. Paired transitions remain disabled across later
resets because pairing was evaluated first.

The known JDK 25 C2/AArch64 loop emits an extra dependent address operation and
retains an everything-segment range comparison. This is accepted based on
target-host production evidence: the integrated state-changing route takes
0.996x native RE2 time on Intel and 1.006x on Graviton while materially beating
the object-row control on both. The 75,850-state bounded-context route takes
0.908x/0.946x native time on Intel/Graviton. Repeated short Ruff searches take
0.998x/0.983x native time, closing the former short object-row deficit.

## Boolean Partial-Match Specializations

`Re2.partialMatch` answers two provably simple boolean queries without entering
the ordinary matcher cascade. A complete case-sensitive literal, including one
wrapped in captures, uses the existing exact prefix scanner. A nullable pattern
returns immediately when the cached DFA proves that the empty match at the
initial position wins before the current byte.

These shortcuts apply only when the caller requests a boolean. APIs that return
boundaries or captures retain the ordinary engines. Unsupported literal shapes,
case folding, position-dependent empty matches, unavailable DFA metadata, and
candidate initial bytes also retain the ordinary path. This preserves matching
semantics and keeps both shortcuts allocation-free.

## Folded Prefix Candidate Scan

Case-insensitive prefix acceleration keeps upstream's maximum nine-byte prefix
but replaces ShiftDFA with fused first-byte and last-byte candidate masks. The
Vector API path evaluates sixteen candidate starts per iteration; the portable
SWAR path evaluates eight. Only positions where both folded boundary bytes
match run the complete folded-prefix comparison.

This is a performance-only deviation. Upstream ShiftDFA carries eight serial
table-dependent shifts through each unrolled block. The fused scan uses two
independent loads and parallel masks, which fits HotSpot code generation better
on the supported Intel and Graviton hosts. Randomized equivalence tests compare
the accelerated result with a scalar folded-prefix search.

## Bounded Character-Class Counting

Counting a complete, unanchored, non-nullable greedy repetition of one character
class scans decoded input once instead of running a complete matcher for every
result. Each maximal matching run is partitioned according to the repetition's
finite minimum and maximum, preserving ordinary leftmost-first match counts.

Eligible UTF-8 patterns lazily build an exact Unicode membership bitset of 136
KiB; Latin-1 patterns use 32 bytes. Construction is single-writer and the table
is retained by the compiled pattern. This fixed bound is independent of
`maxMemory`, which remains the upstream-compatible program and DFA planning
budget, and it does not scale with the expression or configured budget.
Unsupported shapes continue through the ordinary DFA or matcher path. Invalid
UTF-8 bytes never match the bitset, while a valid encoded U+FFFD remains an
ordinary code point.

## Retained Regexp Tree

`Re2` retains the immutable parsed `Regexp` tree and exposes it only within the
package. `Prefilter` consumes that exact tree, and lazy reverse compilation reuses
the required-prefix suffix instead of parsing the pattern again. The retained
reference is an intentional memory-for-consistency tradeoff.

## Match-Every-Byte Shortcut

The full-match shortcut is enabled only for patterns that accept every possible
byte sequence: `\C*` in UTF-8 mode and dot-all `.*` in Latin-1 mode. UTF-8 dot-all
`.*` is deliberately excluded because malformed UTF-8 must still be rejected.

## Bounded Direct BitState Capture

Eligible unanchored leftmost-first searches through a reusable matcher that
request subgroup captures run BitState directly when its complete visited
bitmap fits within 256 KiB. This single pass replaces forward and reverse DFA
boundary searches followed by an anchored NFA capture pass. Inputs above the
fixed bound, programs that cannot use BitState, and stateless caller-buffer
operations retain the ordinary engine cascade.

Upstream caps this bitmap at 256 Kibit (32 KiB). The Java port deliberately uses
more temporary memory because it targets server workloads where the bounded
256 KiB allocation is small compared with input pages, and Intel and Graviton
measurements show that the direct route materially reduces long capture-search
time.

## Matcher-Owned Capture Workspaces

`Re2Matcher` owns reusable BitState and NFA scratch storage. BitState reuses its
visited bitmap, capture scratch, and bounded initial traversal jobs. NFA reuses
its sparse queues, traversal stacks, best-match storage, and primitive capture
thread arena. These workspaces are private to the non-thread-safe matcher and
never retain input bytes or caller group arrays.

An exceptional NFA exit invalidates its workspace before propagating the
failure. This prevents a later search from observing partially live capture
threads or queue state.

The low-level `Re2.matchInto` API remains stateless and allocation-free. It does
not acquire hidden shared state or a reusable workspace. This separates the
ordinary Java matcher lifecycle from the caller-managed low-level contract.

## Allocation-Free Matcher Regions

`Re2Matcher.reset(input, start, end)` binds a logical region of an existing
Slice without constructing a Slice view. Matcher offsets remain relative to the
region, and DFA start-state analysis treats the region boundaries exactly like
the boundaries of an actual Slice view. This is required for anchors, empty
matches, word boundaries, and malformed or split UTF-8 input.

The region is represented only in matcher and DFA setup state; protected DFA
transition loops remain unchanged. A real Slice view is constructed lazily only
when execution reaches a capture engine or when the caller requests a
`MatchResult`; a line rejected by DFA constructs no view. The ordinary
full-input path reuses the caller's original Slice.

## Bounded Candidate-Start Cursor

Eligible reusable matchers use a private candidate-start cursor for repeated
group-zero searches. The cursor scans the forward DFA's start-byte candidates
and verifies each candidate with an anchored first-match traversal. This avoids
the ordinary forward match followed by reverse boundary recovery for dense,
variable-length matches such as the Date Rebar workload.

Eligibility is deliberately narrow: first-match semantics, no required prefix,
no anchors or empty-width instructions, a non-nullable variable-length program,
and an available start-byte accelerator. The route has a linear reset-scoped
work budget and a 128-transition limit per candidate. Exhausting either limit,
encountering an unsupported match shape, or failing DFA cache mutation falls
back to the existing forward-plus-reverse matcher path for the rest of that
matcher reset. The cursor is re-enabled by the next reset.

The implementation uses the existing DFA reader and exclusive-cache mutation
protocol. Cached object transitions still require recovering the authoritative
state offset before the end-of-text transition; tests cover this warm-cache
case directly. The cursor adds no public API and does not change stateless,
capturing, longest-match, anchored, nullable, or fixed-length searches.

Target-host measurements retain the route because it reduces Date group-zero
span time materially without regressing the protected English controls. It
makes Date faster than native RE2 on Graviton, but Intel remains 21.8% slower;
that residual is a formal-qualification issue rather than a claim of parity.

## Extended OnePass Captures

Pinned native RE2 stores OnePass capture actions in spare bits of a 32-bit
transition word, which limits OnePass execution to group zero plus four explicit
capture groups. Java retains that exact transition representation and hot loop
for requests within the native limit.

For one-pass programs that contain higher capture slots, compilation additionally
builds parallel 64-bit capture masks for match and transition actions. The
separate extended loop iterates only the set capture bits and supports up to 32
groups including group zero. The sidecar is charged to the OnePass share of the
compiled memory budget. Requests beyond 32 groups retain the ordinary BitState
or NFA fallback.

This representation is a performance deviation, not a matching-semantics
deviation. It avoids sending deterministic high-capture expressions through
BitState solely because of native RE2's packed-word limit, while keeping the
more common small-capture loop unchanged.

## Java Representation Adaptations

The implementation uses garbage collection, Java collections and arrays, integer
sentinels where C++ uses distinguished pointers, and unsigned-byte masking with
`& 0xFF`. C++ logging and debug-only assertions are not part of the public Java
contract. These representation changes are acceptable only when tests establish
the same externally observable behavior as pinned upstream RE2.
