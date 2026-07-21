I'll read both files to perform a detailed comparison.
## File: BitState.java vs bitstate.cc

### Method: Job (struct)
C++ Location: bitstate.cc:36-40
Java Location: BitState.java:88-93

#### Difference 1
C++:  `const char* p;`
Java: `int p;`
Note: C++ uses a pointer for position, Java uses an int offset.

---

### Method: ShouldVisit
C++ Location: bitstate.cc:95-102
Java Location: BitState.java:188-198

#### Difference 1
C++:  `bool BitState::ShouldVisit(absl::string_view text, uint64_t* visited, uint16_t list_id, const char* p)`
Java: `private static boolean shouldVisit(int textLen, long[] visited, int listId, int pOffset)`
Note: C++ takes text as string_view and p as pointer; Java takes textLen as int and pOffset as pre-computed int offset.

#### Difference 2
C++:  `int n = list_id * static_cast<int>(text.size()+1) + static_cast<int>(p-text.data());`
Java: `int n = listId * (textLen + 1) + pOffset;`
Note: C++ computes offset from pointer difference; Java receives pre-computed offset.

#### Difference 3
C++:  `uint16_t list_id`
Java: `int listId`
Note: C++ parameter type is uint16_t; Java uses int.

---

### Method: GrowStack
C++ Location: bitstate.cc:105-109
Java Location: BitState.java:222-230

#### Difference 1
C++:  `PODArray<Job> tmp(2*job_.size());`
      `memmove(tmp.data(), job_.data(), njob_*sizeof job_[0]);`
      `job_ = std::move(tmp);`
Java: `int old = job.length;`
      `Job[] tmp = Arrays.copyOf(job, old * 2);`
      `for (int i = old; i < tmp.length; i++) {`
      `    tmp[i] = new Job();`
      `}`
      `job = tmp;`
Note: Java pre-allocates new Job objects for expanded slots; C++ leaves new slots uninitialized.

---

### Method: Push
C++ Location: bitstate.cc:112-139
Java Location: BitState.java:200-220

#### Difference 1
C++:  `if (njob_ >= job_.size()) {`
      `  GrowStack();`
      `  if (njob_ >= job_.size()) {`
      `    ABSL_LOG(DFATAL) << "GrowStack() failed: "`
      `                     << "njob_ = " << njob_ << ", "`
      `                     << "job_.size() = " << job_.size();`
      `    return;`
      `  }`
      `}`
Java: `if (njob >= job.length) {`
      `    growStack();`
      `}`
Note: C++ re-checks after GrowStack() and logs DFATAL if still too small; Java has no such check.

#### Difference 2
C++:  `void BitState::Push(int id, const char* p)`
Java: `private void push(int id, int p)`
Note: C++ uses `const char* p`; Java uses `int p`.

---

### Method: TrySearch
C++ Location: bitstate.cc:143-292
Java Location: BitState.java:232-389

#### Difference 1
C++:  `uint16_t* list_heads = prog_->list_heads();`
Java: `// listHeads is a field of type short[]`
Note: C++ loads list_heads into local variable in TrySearch; Java uses instance field from constructor.

#### Difference 2
C++:  `int& rle = job_[njob_].rle;`
Java: `int rle = job[njob].rle;`
Note: C++ uses reference to modify in place; Java copies and later modifies via `job[njob].rle = rle - 1`.

#### Difference 3
C++:  `case kInstFail:`
      `  break;`
Java: `case FAIL -> {`
      `    id = 0;`
      `}`
Note: C++ breaks out of switch (exits inner loop); Java sets id = 0 and relies on check at bottom of loop.

#### Difference 4
C++:  `default:`
      `  ABSL_LOG(DFATAL) << "Unexpected opcode: " << ip->opcode();`
      `  return false;`
Java: `case ALT -> {`
      `    // Unreachable in flattened programs.`
      `    id = 0;`
      `}`
Note: C++ has default case that logs DFATAL and returns false; Java handles ALT explicitly and has no default case.

#### Difference 5
C++:  `goto Next;`
      (within kInstAltMatch, kInstByteRange, kInstEmptyWidth, kInstMatch cases)
Java: `id = nextInList(ip, id);`
Note: C++ uses goto Next label; Java calls nextInList helper method.

#### Difference 6
C++:  `goto CheckAndLoop;`
      (within kInstByteRange, kInstCapture, kInstEmptyWidth, kInstNop cases)
Java: `if (!checkAndLoop(textLen, id, p)) {`
      `    id = 0;`
      `}`
Note: C++ uses goto label; Java calls checkAndLoop helper method and sets id = 0 on failure.

#### Difference 7
C++:  `goto Loop;`
      (within kInstAltMatch, CheckAndLoop, Next)
Java: `continue;` (in kInstAltMatch)
      `// continues outer while loop via checkAndLoop return` (other cases)
Note: C++ uses goto Loop; Java uses continue or relies on setting id and loop control.

#### Difference 8 (Undo Capture handling)
C++:  `if (id < 0) {`
      `  // Undo the Capture.`
      `  cap_[prog_->inst(-id)->cap()] = p;`
      `  continue;`
      `}`
Java: `if (id < 0) {`
      `    // Undo the Capture.`
      `    int capIndex = prog.inst(-id).cap();`
      `    if (0 <= capIndex && capIndex < cap.length) {`
      `        cap[capIndex] = p;`
      `    }`
      `    continue;`
      `}`
Note: Java adds bounds check (`0 <= capIndex && capIndex < cap.length`) that C++ does not have.

#### Difference 9 (kInstMatch best match check)
C++:  `if (submatch_[0].data() == NULL ||`
      `    (longest_ && p > submatch_[0].data() + submatch_[0].size())) {`
Java: `boolean haveBest = submatch[0] >= 0;`
      `int bestEnd = haveBest ? (textBegin + submatch[1]) : -1;`
      `if (!haveBest || (longest && p > bestEnd)) {`
Note: C++ checks if submatch_[0].data() == NULL; Java checks if submatch[0] >= 0. C++ computes end as data()+size(); Java uses textBegin + submatch[1].

#### Difference 10 (kInstMatch submatch storage)
C++:  `for (int i = 0; i < nsubmatch_; i++)`
      `  submatch_[i] = absl::string_view(`
      `      cap_[2 * i],`
      `      static_cast<size_t>(cap_[2 * i + 1] - cap_[2 * i]));`
Java: `for (int i = 0; i < nsubmatch; i++) {`
      `    int a = cap[2 * i];`
      `    int b = cap[2 * i + 1];`
      `    int o = 2 * i;`
      `    if (a < 0 || b < 0) {`
      `        submatch[o] = -1;`
      `        submatch[o + 1] = -1;`
      `    }`
      `    else {`
      `        submatch[o] = a - textBegin;`
      `        submatch[o + 1] = b - textBegin;`
      `    }`
      `}`
Note: Java handles negative cap values with explicit -1 assignment and converts to relative offsets; C++ stores string_view directly without bounds check.

---

### Method: nextInList (Java only)
C++ Location: N/A (inline via `Next:` label at bitstate.cc:282-287)
Java Location: BitState.java:391-397

#### Difference 1
C++:  `Next:`
      `  if (!ip->last()) {`
      `    id++;`
      `    goto Loop;`
      `  }`
      `  break;`
Java: `private int nextInList(Inst ip, int id)`
      `{`
      `    if (!ip.last()) {`
      `        return id + 1;`
      `    }`
      `    return 0;`
      `}`
Note: C++ implements as inline goto label; Java extracts to helper method.

---

### Method: checkAndLoop (Java only)
C++ Location: N/A (inline via `CheckAndLoop:` label at bitstate.cc:241-247)
Java Location: BitState.java:399-413

#### Difference 1
C++:  `CheckAndLoop:`
      `  // Sanity check: id is the head of its list, which must`
      `  // be the case if id-1 is the last of *its* list. :)`
      `  ABSL_DCHECK(id == 0 || prog_->inst(id-1)->last());`
      `  if (ShouldVisit(text_, visited, list_heads[id], p))`
      `    goto Loop;`
      `  break;`
Java: `private boolean checkAndLoop(int textLen, int id, int p)`
      `{`
      `    if (id == 0) {`
      `        return false;`
      `    }`
      `    // id must be the head of its list (debug assertion in upstream).`
      `    if (id != 0 && id - 1 >= 0) {`
      `        if (!prog.inst(id - 1).last()) {`
      `            throw new IllegalStateException("expected list head but id-1 is not last (id=" + id + ")");`
      `        }`
      `    }`
      `    return shouldVisit(textLen, visited, listHeads[id], p - textBegin);`
      `}`
Note: C++ uses ABSL_DCHECK (debug assertion only); Java throws IllegalStateException at runtime. Java checks `id == 0` early and returns false; C++ includes this in DCHECK. Java has redundant condition `id != 0 && id - 1 >= 0`.

---

### Method: Search (BitStateImpl.search)
C++ Location: bitstate.cc:295-360
Java Location: BitState.java:159-186

#### Difference 1
C++:  `const char* etext = text.data() + text.size();`
      `for (const char* p = text.data(); p <= etext; p++) {`
Java: `for (int p = textBegin; p <= textEnd; p++) {`
Note: C++ uses pointer arithmetic; Java uses int offsets.

#### Difference 2
C++:  `p = reinterpret_cast<const char*>(prog_->PrefixAccel(p, etext - p));`
      `if (p == NULL)`
      `  p = etext;`
Java: `int found = prog.prefixAccel(bytes, p, textEnd - p);`
      `if (found < 0) {`
      `    p = textEnd;`
      `}`
      `else {`
      `    p = found;`
      `}`
Note: C++ PrefixAccel returns NULL on no match; Java prefixAccel returns -1 on no match.

#### Difference 3
C++:  `// Avoid invoking undefined behavior (arithmetic on a null pointer)`
      `// by simply not continuing the loop.`
      `if (p == NULL)`
      `  break;`
Java: (no equivalent code)
Note: C++ has explicit NULL check at end of loop to avoid undefined behavior; Java doesn't need this since using int index.

---

### Method: search (static entry point)
C++ Location: bitstate.cc:295-360 (BitState::Search) + bitstate.cc:363-387 (Prog::SearchBitState)
Java Location: BitState.java:28-86

#### Difference 1
C++:  (no equivalent - check is in Prog::SearchBitState or elsewhere)
Java: `if (!prog.canBitState()) {`
      `    return false;`
      `}`
Note: Java checks canBitState() at entry; C++ equivalent is in Prog::SearchBitState or handled elsewhere.

#### Difference 2
C++:  (no equivalent)
Java: `if (submatch != null && (submatch.length % 2) != 0) {`
      `    throw new IllegalArgumentException("submatch length must be even: " + submatch.length);`
      `}`
Note: Java validates submatch length is even; C++ has no such validation.

#### Difference 3
C++:  (no equivalent)
Java: `if (prog.start() == 0) {`
      `    return false;`
      `}`
Note: Java early-returns false if prog.start() is 0; C++ has no explicit check.

#### Difference 4
C++:  (no equivalent - context is string_view, can check data() == NULL)
Java: `if (text.byteArray() != context.byteArray()) {`
      `    return false;`
      `}`
      `...`
      `if (textBegin < ctxBegin || textEnd > ctxEnd) {`
      `    return false;`
      `}`
Note: Java explicitly validates text lies within context bounds using same byte array; C++ doesn't have this explicit check.

#### Difference 5
C++:  `for (int i = 0; i < nsubmatch_; i++)`
      `  submatch_[i] = absl::string_view();`
Java: `if (nsubmatch > 0) {`
      `    for (int i = 0; i < submatch.length; i++) {`
      `        submatch[i] = -1;`
      `    }`
      `}`
Note: C++ initializes only first nsubmatch_ elements with empty string_view; Java initializes ALL submatch elements to -1.

---

### Method: BitStateImpl constructor
C++ Location: bitstate.cc:78-86 (BitState constructor) + bitstate.cc:316-328 (allocation in Search)
Java Location: BitState.java:119-157

#### Difference 1
C++:  `int ncap = 2*nsubmatch;`
      `if (ncap < 2)`
      `  ncap = 2;`
Java: `int ncap = Math.max(2, 2 * nsubmatch);`
Note: Logically equivalent, different expression style.

#### Difference 2
C++:  `memset(cap_.data(), 0, ncap*sizeof cap_[0]);`
Java: `Arrays.fill(this.cap, -1);`
Note: C++ initializes cap to 0 (NULL pointers); Java initializes to -1.

#### Difference 3
C++:  `job_ = PODArray<Job>(64);`
      (no initialization of Job contents)
Java: `this.job = new Job[64];`
      `for (int i = 0; i < job.length; i++) {`
      `    job[i] = new Job();`
      `}`
Note: Java pre-allocates all Job objects; C++ PODArray doesn't initialize.

---

### Method: Prog::SearchBitState (C++ only)
C++ Location: bitstate.cc:363-387
Java Location: N/A (equivalent logic likely in Prog.java)

#### Difference 1
C++:  `absl::string_view sp0;`
      `if (kind == kFullMatch) {`
      `  anchor = kAnchored;`
      `  if (nmatch < 1) {`
      `    match = &sp0;`
      `    nmatch = 1;`
      `  }`
      `}`
Java: (not present in BitState.java)
Note: C++ has kFullMatch handling that ensures match[0] exists; Java equivalent may be elsewhere.

#### Difference 2
C++:  `if (kind == kFullMatch && EndPtr(match[0]) != EndPtr(text))`
      `  return false;`
Java: (not present in BitState.java)
Note: C++ verifies full match ends at text end; Java equivalent may be elsewhere.

---

### Summary for BitState.java vs bitstate.cc
- Methods compared: 10 (ShouldVisit, GrowStack, Push, TrySearch, nextInList, checkAndLoop, Search/search, static search entry, BitStateImpl constructor, Prog::SearchBitState)
- Total differences found: 32
