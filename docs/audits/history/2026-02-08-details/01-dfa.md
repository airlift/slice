I'll read both files completely to perform a thorough line-by-line audit.
Now I'll perform a systematic line-by-line comparison of the key methods. Let me document all differences.

## File: Dfa.java vs dfa.cc

### Method: workqToCachedState
C++ Location: dfa.cc:592-741
Java Location: Dfa.java:827-938

#### Difference 1
C++:  `State* DFA::WorkqToCachedState(Workq* q, Workq* mq, uint32_t flag)`
Java: `private State workqToCachedState(Workq q, int flag)`
Note: Java version does not have the `mq` (match queue) parameter for kManyMatch support

#### Difference 2
C++:  `PODArray<int> inst(q->size());`
Java: `int[] inst = new int[qsize];`
Note: C++ uses PODArray, Java uses raw int array. Equivalent behavior.

#### Difference 3
C++:  `uint32_t needflags = 0;`
Java: `boolean needFlags = false;`
Note: C++ tracks the actual needed flags bitmask, Java only tracks a boolean

#### Difference 4
C++:  `bool sawmark = false;`
Java: (missing)
Note: Java does not track sawmark variable for kLongestMatch mode

#### Difference 5
C++:  
```cpp
for (Workq::iterator it = q->begin(); it != q->end(); ++it) {
    int id = *it;
    if (sawmatch && (kind_ == Prog::kFirstMatch || q->is_mark(id)))
      break;
    if (q->is_mark(id)) {
      if (n > 0 && inst[n-1] != Mark) {
        sawmark = true;
        inst[n++] = Mark;
      }
      continue;
    }
```
Java:
```java
for (int it = 0; it < qsize; it++) {
    int id = q.denseAt(it);
    if (id <= 0) {
        continue;
    }

    if (sawMatch && kind == Kind.FIRST_MATCH) {
        break;
    }
```
Note: Java lacks Mark handling (q->is_mark check and Mark insertion). Java checks `id <= 0` instead of using is_mark().

#### Difference 6
C++:
```cpp
if (kind_ != Prog::kManyMatch &&
    (kind_ != Prog::kFirstMatch ||
     (it == q->begin() && ip->greedy(prog_))) &&
    (kind_ != Prog::kLongestMatch || !sawmark) &&
    (flag & kFlagMatch)) {
  if (ExtraDebug)
    absl::FPrintF(stderr, " -> FullMatchState\n");
  return FullMatchState;
}
```
Java:
```java
if (op == InstOp.ALT_MATCH &&
        (kind != Kind.FIRST_MATCH || (isFirstEntry && ip.greedy(prog))) &&
        (kind == Kind.LONGEST_MATCH || (flag & FLAG_MATCH) != 0)) {
    return FULL_MATCH;
}
```
Note: C++ has `kind_ != Prog::kManyMatch` check. Java has no kManyMatch equivalent. C++ has `kind_ != Prog::kLongestMatch || !sawmark` check. Java has `kind == Kind.LONGEST_MATCH || (flag & FLAG_MATCH) != 0` which differs in logic.

#### Difference 7
C++:  `if (ip->opcode() == kInstEmptyWidth) needflags |= ip->empty();`
Java: `needFlags |= op == InstOp.EMPTY_WIDTH;`
Note: C++ accumulates actual empty flags, Java just sets boolean true

#### Difference 8
C++:  `if (prog_->inst(id-1)->last()) inst[n++] = *it;`
Java: `if (id > 0 && prog.inst(id - 1).last()) { inst[n++] = id; }`
Note: C++ does not have the `id > 0` check, Java does

#### Difference 9
C++:  `if (ip->opcode() == kInstMatch && !prog_->anchor_end()) sawmatch = true;`
Java: `if (op == InstOp.MATCH && !prog.anchorEnd()) { sawMatch = true; }`
Note: Equivalent logic

#### Difference 10
C++:
```cpp
if (n > 0 && inst[n-1] == Mark)
    n--;
```
Java: (missing)
Note: Java does not strip trailing Mark

#### Difference 11
C++:  `if (needflags == 0) flag &= kFlagMatch;`
Java: `if (!needFlags) { flag &= ~FLAG_EMPTY_MASK; }`
Note: C++ clears everything except kFlagMatch, Java clears only FLAG_EMPTY_MASK (preserves FLAG_MATCH and FLAG_LAST_WORD)

#### Difference 12
C++:
```cpp
if (kind_ == Prog::kLongestMatch) {
    int* ip = inst.data();
    int* ep = ip + n;
    while (ip < ep) {
      int* markp = ip;
      while (markp < ep && *markp != Mark)
        markp++;
      std::sort(ip, markp);
      if (markp < ep)
        markp++;
      ip = markp;
    }
  }
```
Java: (missing)
Note: Java does not sort instruction groups for kLongestMatch canonicalization

#### Difference 13
C++:
```cpp
if (kind_ == Prog::kManyMatch) {
    int* ip = inst.data();
    int* ep = ip + n;
    std::sort(ip, ep);
  }
```
Java: (missing)
Note: Java has no kManyMatch sorting

#### Difference 14
C++:
```cpp
if (mq != NULL) {
    inst[n++] = MatchSep;
    for (Workq::iterator i = mq->begin(); i != mq->end(); ++i) {
      int id = *i;
      Prog::Inst* ip = prog_->inst(id);
      if (ip->opcode() == kInstMatch)
        inst[n++] = ip->match_id();
    }
  }
```
Java: (missing)
Note: Java does not append MatchSep and match IDs for kManyMatch

#### Difference 15
C++:  `flag |= needflags << kFlagNeedShift;`
Java: (missing)
Note: Java does not store needflags in upper bits of flag

#### Difference 16
C++:  `if (n == 0 && flag == 0) { return DeadState; }`
Java: `if (n == 0 && (flag & FLAG_MATCH) == 0) { return DEAD; }`
Note: C++ checks `flag == 0`, Java checks only `(flag & FLAG_MATCH) == 0`

---

### Method: runStateOnByte
C++ Location: dfa.cc:1022-1114
Java Location: Dfa.java:749-810

#### Difference 1
C++:
```cpp
if (state <= SpecialStateMax) {
    if (state == FullMatchState) {
      return FullMatchState;
    }
    if (state == DeadState) {
      ABSL_LOG(DFATAL) << "DeadState in RunStateOnByte";
      return NULL;
    }
    if (state == NULL) {
      ABSL_LOG(DFATAL) << "NULL state in RunStateOnByte";
      return NULL;
    }
    ABSL_LOG(DFATAL) << "Unexpected special state in RunStateOnByte";
    return NULL;
  }
```
Java:
```java
if (state == DEAD) {
    return DEAD;
}
if (state == FULL_MATCH) {
    return FULL_MATCH;
}
```
Note: C++ returns NULL for DeadState (with DFATAL log), Java returns DEAD directly

#### Difference 2
C++:  `State* ns = state->next_[ByteMap(c)].load(std::memory_order_relaxed);`
Java: `State ns = state.next[cls];`
Note: C++ uses atomic load with memory_order_relaxed, Java uses plain array access

#### Difference 3
C++:
```cpp
uint32_t needflag = state->flag_ >> kFlagNeedShift;
uint32_t beforeflag = state->flag_ & kFlagEmptyMask;
uint32_t oldbeforeflag = beforeflag;
```
Java:
```java
int before = state.emptyFlags();
int after = 0;
```
Note: C++ extracts needflag from upper bits and saves oldbeforeflag, Java does not

#### Difference 4
C++:
```cpp
if (beforeflag & ~oldbeforeflag & needflag) {
    RunWorkqOnEmptyString(q0_, q1_, beforeflag);
    using std::swap;
    swap(q0_, q1_);
  }
```
Java:
```java
runWorkqOnEmptyString(q0, q1, before);
swapWorkq();
```
Note: C++ conditionally runs empty string pass only if new useful flags, Java always runs it

#### Difference 5
C++:  `RunWorkqOnByte(q0_, q1_, c, afterflag, &ismatch);`
Java: `ismatch = runWorkqOnByte(q0, q1, c, after, ismatch);`
Note: C++ passes ismatch by pointer, Java passes/returns by value

#### Difference 6
C++:
```cpp
if (ismatch && kind_ == Prog::kManyMatch)
    ns = WorkqToCachedState(q0_, q1_, flag);
  else
    ns = WorkqToCachedState(q0_, NULL, flag);
```
Java:
```java
ns = workqToCachedState(q0, flag);
if (ns == null) {
    ns = DEAD;
}
```
Note: C++ passes match queue for kManyMatch, Java has no mq parameter. Java explicitly handles null->DEAD

#### Difference 7
C++:  `state->next_[ByteMap(c)].store(ns, std::memory_order_release);`
Java: `state.next[cls] = ns;`
Note: C++ uses atomic store with memory_order_release, Java uses plain assignment

---

### Method: runWorkqOnByte
C++ Location: dfa.cc:951-1011
Java Location: Dfa.java:1132-1177

#### Difference 1
C++:
```cpp
void DFA::RunWorkqOnByte(Workq* oldq, Workq* newq,
                         int c, uint32_t flag, bool* ismatch)
```
Java:
```java
private boolean runWorkqOnByte(Workq oldq, Workq newq, int c, int flag, boolean ismatch)
```
Note: C++ modifies ismatch via pointer, Java returns boolean

#### Difference 2
C++:
```cpp
if (oldq->is_mark(*i)) {
      if (*ismatch)
        return;
      newq->mark();
      continue;
    }
```
Java: (missing)
Note: Java has no Mark handling in runWorkqOnByte

#### Difference 3
C++:
```cpp
case kInstFail:        // never succeeds
case kInstCapture:     // already followed
case kInstNop:         // already followed
case kInstAltMatch:    // already followed
case kInstEmptyWidth:  // already followed
  break;
```
Java:
```java
default -> {
    // In flattened programs, the work queues should contain only byte range and match insts.
}
```
Note: C++ explicitly lists all fallthrough cases, Java uses default

#### Difference 4
C++:
```cpp
Prog::Inst* ip0 = ip;
while (!ip->last())
  ++ip;
i += ip - ip0;
```
Java:
```java
Prog.Inst j = ip;
int jid = id;
while (!j.last()) {
    jid++;
    j = prog.inst(jid);
}
it += jid - id;
```
Note: C++ uses pointer arithmetic, Java uses index arithmetic. Equivalent.

#### Difference 5
C++:
```cpp
case kInstMatch:
  if (prog_->anchor_end() && c != kByteEndText &&
      kind_ != Prog::kManyMatch)
    break;
```
Java:
```java
case MATCH -> {
    if (prog.anchorEnd() && c != BYTE_END_TEXT) {
        break;
    }
```
Note: C++ has extra `kind_ != Prog::kManyMatch` condition, Java does not

---

### Method: addToQueue
C++ Location: dfa.cc:837-919
Java Location: Dfa.java:1058-1122

#### Difference 1
C++:
```cpp
Loop:
    if (id == Mark) {
      q->mark();
      continue;
    }
```
Java: (missing)
Note: Java does not handle Mark value in addToQueue

#### Difference 2
C++:
```cpp
case kInstNop:
  if (!ip->last())
    stk[nstk++] = id+1;

  // If this instruction is the [00-FF]* loop at the beginning of
  // a leftmost-longest unanchored search, separate with a Mark so
  // that future threads (which will start farther to the right in
  // the input string) are lower priority than current threads.
  if (ip->opcode() == kInstNop && q->maxmark() > 0 &&
      id == prog_->start_unanchored() && id != prog_->start())
    stk[nstk++] = Mark;
  id = ip->out();
  goto Loop;
```
Java:
```java
case CAPTURE, NOP -> {
    if (!ip.last()) {
        stack[sp++] = id + 1;
    }
    id = ip.out();
    continue;
}
```
Note: Java does not push Mark for unanchored search start

#### Difference 3
C++:
```cpp
case kInstAltMatch:
  ABSL_DCHECK(!ip->last());
  id = id+1;
  goto Loop;
```
Java:
```java
case ALT, ALT_MATCH -> {
    if (!ip.last()) {
        stack[sp++] = id + 1;
    }
    // Push out1 first so out is processed first (leftmost-first order).
    stack[sp++] = ip.out1();
    id = ip.out();
    continue;
}
```
Note: C++ handles ALT_MATCH separately from ALT, just advancing id. Java handles ALT and ALT_MATCH identically, following both branches.

#### Difference 4
C++:  (missing FAIL handling in addToQueue switch)
Java:
```java
case FAIL -> {
    if (!ip.last()) {
        stack[sp++] = id + 1;
    }
    id = 0;
}
```
Note: C++ has no FAIL case in switch (falls through to default DFATAL), Java explicitly handles FAIL

---

### Method: stateToWorkq
C++ Location: dfa.cc:821-834
Java Location: Dfa.java:819-825

#### Difference 1
C++:
```cpp
if (s->inst_[i] == Mark) {
      q->mark();
    } else if (s->inst_[i] == MatchSep) {
      // Nothing after this is an instruction!
      break;
    } else {
      // Explore from the head of the list.
      AddToQueue(q, s->inst_[i], s->flag_ & kFlagEmptyMask);
    }
```
Java:
```java
for (int i = 0; i < s.ninst; i++) {
    q.insertNew(s.inst[i]);
}
```
Note: C++ handles Mark, MatchSep and expands via AddToQueue. Java just inserts instruction IDs directly without expanding.

---

### Method: runWorkqOnEmptyString
C++ Location: dfa.cc:937-945
Java Location: Dfa.java:1124-1130

#### Difference 1
C++:
```cpp
for (Workq::iterator i = oldq->begin(); i != oldq->end(); ++i) {
    if (oldq->is_mark(*i))
      AddToQueue(newq, Mark, flag);
    else
      AddToQueue(newq, *i, flag);
  }
```
Java:
```java
for (int it = 0; it < oldq.size(); it++) {
    addToQueue(newq, oldq.denseAt(it), flag);
}
```
Note: C++ handles marks specially, Java does not handle marks

---

### Method: analyzeStart (AnalyzeSearch + AnalyzeSearchHelper)
C++ Location: dfa.cc:1646-1749
Java Location: Dfa.java:647-739

#### Difference 1
C++:
```cpp
int start;
uint32_t flags;
if (params->run_forward) {
  if (BeginPtr(text) == BeginPtr(context)) {
    start = kStartBeginText;
    flags = kEmptyBeginText|kEmptyBeginLine;
  } else if (BeginPtr(text)[-1] == '\n') {
```
Java:
```java
int flags;
State existing;
int dirIndex = params.runForward ? 1 : 0;
int anchorIndex = params.anchored ? 1 : 0;
if (params.runForward) {
    if (textBegin == ctxBegin) {
        flags = EmptyOp.EMPTY_BEGIN_TEXT | EmptyOp.EMPTY_BEGIN_LINE;
        existing = startBeginText[dirIndex][anchorIndex];
```
Note: Java uses 2D arrays indexed by direction and anchor, C++ uses a single start_[] array with combined index

#### Difference 2
C++:  `if (params->anchored) start |= kStartAnchored;`
Java: (uses anchorIndex in 2D array indexing)
Note: Different caching structure

#### Difference 3
C++:
```cpp
if (!AnalyzeSearchHelper(params, info, flags)) {
    ResetCache(params->cache_lock);
    if (!AnalyzeSearchHelper(params, info, flags)) {
      params->failed = true;
      ABSL_LOG(DFATAL) << "Failed to analyze start state.";
      return false;
    }
  }
```
Java: (no retry with cache reset in analyzeStart itself)
Note: Java analyzeStart doesn't retry with cache reset, relies on caller

#### Difference 4
C++:
```cpp
// Even if we could prefix accel, we cannot do so when anchored and,
// less obviously, we cannot do so when we are going to need flags.
// This trick works only when there is a single byte that leads to a
// different state!
if (prog_->can_prefix_accel() &&
    !params->anchored &&
    params->start > SpecialStateMax &&
    params->start->flag_ >> kFlagNeedShift == 0)
  params->can_prefix_accel = true;
```
Java: (prefix accel check is in search() method, not analyzeStart)
Note: Different placement

---

### Method: Search (InlinedSearchLoop)
C++ Location: dfa.cc:1348-1569
Java Location: Dfa.java:239-394 (search loop)

#### Difference 1
C++:  `const uint8_t* resetp = NULL;`
Java: `int resetPosition = -1; int resetCount = 0; final int maxResets = 3;`
Note: Java has resetCount and maxResets for additional thrashing protection, C++ does not count resets

#### Difference 2
C++:
```cpp
if (s->IsMatch()) {
    matched = true;
    lastmatch = p;
    // ...
    if (params->matches != NULL) {
      for (int i = s->ninst_ - 1; i >= 0; i--) {
        int id = s->inst_[i];
        if (id == MatchSep)
          break;
        params->matches->insert(id);
      }
    }
    if (want_earliest_match) {
      params->ep = reinterpret_cast<const char*>(lastmatch);
      return true;
    }
  }
```
Java:
```java
if (s.isMatch()) {
    matched = true;
    lastMatchBoundary = p;
    if (wantEarliestMatch && !endmatch) {
        return finalizeResult(textBegin, textEnd, endmatch, matched, lastMatchBoundary, runForward);
    }
}
```
Note: C++ handles params->matches (kManyMatch), Java does not. Java checks `!endmatch` in early return.

#### Difference 3
C++:
```cpp
if (can_prefix_accel && s == start) {
      // In start state, only way out is to find the prefix,
      // so we use prefix accel (e.g. memchr) to skip ahead.
      // If not found, we can skip to the end of the string.
      p = BytePtr(prog_->PrefixAccel(p, ep - p));
      if (p == NULL) {
        p = ep;
        break;
      }
    }
```
Java:
```java
if (canPrefixAccel && s == start) {
    int next = prog.prefixAccel(bytes, p, textEnd - p);
    if (next < 0) {
        p = textEnd;
        break;
    }
    p = next;
}
```
Note: C++ prefix accel returns pointer/NULL, Java returns int (-1 for not found)

#### Difference 4
C++:
```cpp
if (dfa_should_bail_when_slow && resetp != NULL &&
    static_cast<size_t>(p - resetp) < 10*state_cache_.size() &&
    kind_ != Prog::kManyMatch) {
  params->failed = true;
  return false;
}
```
Java:
```java
if (resetPosition >= 0 && (p - resetPosition) < 10 * dfa.cacheSize()) {
    // We've reset recently and filled cache again quickly.
    // This is pathological - fall back to NFA.
    return SEARCH_FAILED;  // Signal: use NFA
}
```
Note: C++ has `dfa_should_bail_when_slow` flag and `kind_ != Prog::kManyMatch` check, Java has neither

#### Difference 5
C++:
```cpp
if (ns <= SpecialStateMax) {
      if (ns == DeadState) {
        params->ep = reinterpret_cast<const char*>(lastmatch);
        return matched;
      }
      // FullMatchState
      params->ep = reinterpret_cast<const char*>(ep);
      return true;
    }
```
Java:
```java
if (s == DEAD) {
    break;
}
if (s == FULL_MATCH) {
    // Rest of string matches (upstream dfa.cc:1474-1476).
    return finalizeResult(textBegin, textEnd, endmatch, true, textEnd, runForward);
}
```
Note: C++ returns from loop on DeadState, Java breaks. C++ uses ep for FullMatch, Java uses textEnd.

#### Difference 6
C++:
```cpp
if (s->IsMatch()) {
      matched = true;
      // The DFA notices the match one byte late,
      // so adjust p before using it in the match.
      if (run_forward)
        lastmatch = p - 1;
      else
        lastmatch = p + 1;
```
Java:
```java
if (s.isMatch()) {
    matched = true;
    // Matches are delayed by one byte; see re2/dfa.cc.
    lastMatchBoundary = p - 1;
```
Note: Java forward search loop has only `p - 1`, reverse loop separately has `p + 1`. C++ handles both in template.

#### Difference 7
C++:
```cpp
if (params->matches != NULL) {
        for (int i = s->ninst_ - 1; i >= 0; i--) {
          int id = s->inst_[i];
          if (id == MatchSep)
            break;
          params->matches->insert(id);
        }
      }
```
Java: (missing)
Note: Java does not populate matches set (no kManyMatch support)

#### Difference 8
C++:  `if (want_earliest_match) { params->ep = ...; return true; }`
Java: `if (wantEarliestMatch && !endmatch) { break; }`
Note: C++ returns immediately, Java breaks from loop. Java checks `!endmatch`.

#### Difference 9
C++:
```cpp
if (run_forward) {
    if (EndPtr(params->text) == EndPtr(params->context))
      lastbyte = kByteEndText;
    else
      lastbyte = EndPtr(params->text)[0] & 0xFF;
  } else {
    if (BeginPtr(params->text) == BeginPtr(params->context))
      lastbyte = kByteEndText;
    else
      lastbyte = BeginPtr(params->text)[-1] & 0xFF;
  }
```
Java:
```java
if (runForward) {
    lastbyte = (textEnd == ctxEnd) ? BYTE_END_TEXT : (bytes[textEnd] & 0xFF);
}
else {
    lastbyte = (textBegin == ctxBegin) ? BYTE_END_TEXT : (bytes[textBegin - 1] & 0xFF);
}
```
Note: Equivalent logic, different expressions

#### Difference 10 (Post-loop lastbyte processing)
C++:
```cpp
State* ns = s->next_[ByteMap(lastbyte)].load(std::memory_order_acquire);
  if (ns == NULL) {
    ns = RunStateOnByteUnlocked(s, lastbyte);
    if (ns == NULL) {
      StateSaver save_s(this, s);
      ResetCache(params->cache_lock);
      if ((s = save_s.Restore()) == NULL) {
        params->failed = true;
        return false;
      }
      ns = RunStateOnByteUnlocked(s, lastbyte);
      if (ns == NULL) {
        ABSL_LOG(DFATAL) << "RunStateOnByteUnlocked failed after Reset";
        params->failed = true;
        return false;
      }
    }
  }
```
Java:
```java
s = dfa.runStateOnByte(s, lastbyte);
if (s != DEAD && s.isMatch()) {
    matched = true;
    lastMatchBoundary = p;
}
```
Note: C++ handles cache reset for lastbyte, Java does not. C++ checks ns separately, Java just reassigns s.

#### Difference 11
C++:
```cpp
if (ns <= SpecialStateMax) {
    if (ns == DeadState) {
      params->ep = reinterpret_cast<const char*>(lastmatch);
      return matched;
    }
    // FullMatchState
    params->ep = reinterpret_cast<const char*>(ep);
    return true;
  }
```
Java: (checks `s != DEAD` but no explicit FullMatch handling for lastbyte)
Note: Java lacks explicit FullMatchState handling for lastbyte processing

---

### Method: Constructor DFA/DfaInstance
C++ Location: dfa.cc:420-467
Java Location: Dfa.java:588-634

#### Difference 1
C++:
```cpp
int nmark = 0;
if (kind_ == Prog::kLongestMatch)
    nmark = prog_->size();
```
Java: (no nmark handling)
Note: Java has no Mark support for kLongestMatch

#### Difference 2
C++:
```cpp
int nstack = prog_->inst_count(kInstCapture) +
             prog_->inst_count(kInstEmptyWidth) +
             prog_->inst_count(kInstNop) +
             nmark + 1;  // + 1 for start inst
```
Java:
```java
this.stack = new int[Math.max(16, prog.size() * 3 + 32)];
```
Note: C++ calculates precise stack size based on instruction counts, Java uses prog.size() * 3 + 32 heuristic

#### Difference 3
C++:
```cpp
int64_t one_state = sizeof(State) + nnext*sizeof(std::atomic<State*>) +
                    (prog_->list_count()+nmark)*sizeof(int);
if (state_budget_ < 20*one_state) {
    init_failed_ = true;
    return;
  }
```
Java:
```java
long oneState = estimateStateSize();
if (stateBudget < 20 * oneState) {
    this.initFailed = true;
    return;
}
```
Note: C++ uses `prog_->list_count()+nmark`, Java uses fixed estimate in `estimateStateSize()`

---

### Method: Workq class
C++ Location: dfa.cc:368-418
Java Location: Dfa.java:521-554

#### Difference 1
C++:
```cpp
Workq(int n, int maxmark) :
    SparseSet(n+maxmark),
    n_(n),
    maxmark_(maxmark),
    nextmark_(n),
    last_was_mark_(true) {
  }
```
Java:
```java
Workq(int maxInst)
{
    this.set = new SparseSet(maxInst);
}
```
Note: C++ Workq has mark support with n_, maxmark_, nextmark_, last_was_mark_. Java has none.

#### Difference 2
C++:  `bool is_mark(int i) { return i >= n_; }`
Java: (missing)
Note: Java has no is_mark method

#### Difference 3
C++:  `void mark() { if (last_was_mark_) return; last_was_mark_ = false; SparseSet::insert_new(nextmark_++); }`
Java: (missing)
Note: Java has no mark method

#### Difference 4
C++:  `int maxmark() { return maxmark_; }`
Java: (missing)
Note: Java has no maxmark method

---

### Method: StateSaver class
C++ Location: dfa.cc:1219-1275
Java Location: Dfa.java:997-1056

#### Difference 1
C++:
```cpp
StateSaver(DFA* dfa, State* state) {
  dfa_ = dfa;
  if (state <= SpecialStateMax) {
    inst_ = NULL;
    ninst_ = 0;
    flag_ = 0;
    is_special_ = true;
    special_ = state;
    return;
  }
  // ...
}
```
Java:
```java
public StateSaver(State s)
{
    if (s == DEAD || s == FULL_MATCH || s == null) {
        this.inst = null;
        this.ninst = 0;
        this.flag = 0;
    }
    else {
        // ...
    }
}
```
Note: C++ stores reference to dfa, checks `state <= SpecialStateMax`, keeps is_special_ and special_ fields. Java checks explicitly against DEAD/FULL_MATCH/null, no is_special_ flag.

#### Difference 2
C++:
```cpp
State* Restore() {
  if (is_special_)
    return special_;
  absl::MutexLock l(dfa_->mutex_);
  State* s = dfa_->CachedState(inst_, ninst_, flag_);
  if (s == NULL)
    ABSL_LOG(DFATAL) << "StateSaver failed to restore state.";
  return s;
}
```
Java:
```java
public State restore(DfaInstance dfa)
{
    if (inst == null) {
        return DEAD;
    }
    // Recreate state in fresh cache...
}
```
Note: C++ returns original special state, Java returns DEAD for null inst (loses distinction between DEAD/FULL_MATCH/NULL)

---

### Method: SearchDFA (Prog::SearchDFA)
C++ Location: dfa.cc:1844-1915
Java Location: Dfa.java:120-214

#### Difference 1
C++:
```cpp
bool anchored = anchor == kAnchored || anchor_start() || kind == kFullMatch;
bool endmatch = false;
if (kind == kManyMatch) {
    // This is split out in order to avoid clobbering kind.
  } else if (kind == kFullMatch || anchor_end()) {
    endmatch = true;
    kind = kLongestMatch;
  }
```
Java:
```java
boolean anchoredEffective = anchored || anchorStart;
// ...
boolean endmatch = false;
if (anchorEnd || fullMatch) {
    endmatch = true;
}
```
Note: C++ modifies kind to kLongestMatch when endmatch. Java doesn't modify kind but uses separate `fullMatch` parameter.

#### Difference 2
C++:
```cpp
bool want_earliest_match = false;
if (kind == kManyMatch) {
    // This is split out in order to avoid clobbering kind.
    if (matches == NULL) {
      want_earliest_match = true;
    }
  } else if (match0 == NULL && !endmatch) {
    want_earliest_match = true;
    kind = kLongestMatch;
  }
```
Java: `wantEarliestMatch` is passed as parameter, not computed internally
Note: C++ computes want_earliest_match internally based on match0 and endmatch. Java takes it as parameter.

#### Difference 3
C++:
```cpp
if (params.start == FullMatchState) {
    if (run_forward == want_earliest_match)
      *epp = text.data();
    else
      *epp = text.data() + text.size();
    return true;
  }
```
Java:
```java
if (start == FULL_MATCH) {
    // Like upstream: if run_forward == want_earliest_match, return text start; else text end.
    return (runForward == wantEarliestMatch) ? 0 : text.length();
}
```
Note: Equivalent logic, different types (pointer vs int)

---

### Method: ResetCache
C++ Location: dfa.cc:1187-1201
Java Location: Dfa.java:945-977

#### Difference 1
C++:
```cpp
void DFA::ResetCache(RWLocker* cache_lock) {
  // Re-acquire the cache_mutex_ for writing (exclusive use).
  cache_lock->LockForWriting();

  hooks::GetDFAStateCacheResetHook()({
      state_budget_,
      state_cache_.size(),
  });

  // Clear the cache, reset the memory budget.
  for (int i = 0; i < kMaxStart; i++)
    start_[i].start.store(NULL, std::memory_order_relaxed);
  ClearCache();
  mem_budget_ = state_budget_;
}
```
Java:
```java
private void resetCache()
{
    // Clear start state cache.
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < 2; j++) {
            startBeginText[i][j] = null;
            startBeginLine[i][j] = null;
            startAfterWord[i][j] = null;
            startAfterNonWord[i][j] = null;
        }
    }

    // Clear state cache.
    cache.clear();

    // Restore budget.
    memBudget = stateBudget;
}
```
Note: C++ uses hook for metrics, Java does not. Different cache structure (single array vs multiple 2D arrays).

---

### Method: State struct
C++ Location: dfa.cc:119-142
Java Location: Dfa.java:439-468

#### Difference 1
C++:  `std::atomic<State*> next_[];`
Java: `private final State[] next;`
Note: C++ uses atomic array, Java uses regular array

#### Difference 2
C++:  `uint32_t flag_;`
Java: `private final int flag;`
Note: Same semantics, different types. Java flag is final.

---

### Summary for Dfa.java vs dfa.cc
- Methods compared: 14 (workqToCachedState, runStateOnByte, runWorkqOnByte, addToQueue, stateToWorkq, runWorkqOnEmptyString, analyzeStart, search/InlinedSearchLoop, DfaInstance constructor, Workq class, StateSaver class, SearchDFA entry, ResetCache, State struct)
- Total differences found: 65

### Critical Differences Summary

**Missing Features in Java:**
1. **No Mark support** - kLongestMatch mode relies on Marks to separate thread groups by match start position. Java lacks is_mark(), mark(), maxmark(), and Mark handling throughout.
2. **No kManyMatch support** - Match IDs, MatchSep, mq parameter in WorkqToCachedState all missing.
3. **No needflags optimization** - C++ stores needflags in upper bits of flag and conditionally runs empty string pass. Java always runs it.
4. **ALT_MATCH handled differently in addToQueue** - C++ just advances id, Java explores both branches like ALT.
5. **stateToWorkq doesn't expand** - C++ calls AddToQueue for each inst, Java just inserts directly.

**Behavioral Differences:**
1. **FullMatchState condition in workqToCachedState** - Different conditions for returning FullMatchState
2. **Dead state check** - C++ checks `flag == 0`, Java checks `(flag & FLAG_MATCH) == 0`
3. **StateSaver.restore loses special state identity** - Returns DEAD for any null inst

**Thread Safety Differences:**
1. C++ uses `memory_order_acquire/release` for atomics, Java uses plain assignments
2. C++ has RWLocker for lock upgrade pattern, Java uses separate read/write lock methods
