I'll read both files to perform a detailed comparison.
## File: Nfa.java vs nfa.cc

### Method: NfaImpl constructor
C++ Location: nfa.cc:134-152 (NFA::NFA) + nfa.cc:479-497 (Search setup)
Java Location: Nfa.java:175-235

#### Difference 1
C++:  `int nstack = 2*prog_->inst_count(kInstCapture) + prog_->inst_count(kInstEmptyWidth) + prog_->inst_count(kInstNop) + 1;`
Java: `int nstack = 2 * captures + emptyWidths + nops + 1;` (computed via loop over prog.inst(id).opcode())
Note: C++ uses prog_->inst_count() method; Java manually iterates and counts opcodes

#### Difference 2
C++:  `match_ = new const char*[ncapture_]; memset(match_, 0, ncapture_*sizeof match_[0]);`
Java: `this.match = new int[ncapture]; Arrays.fill(this.match, -1);`
Note: C++ initializes match array to NULL/0; Java initializes to -1

#### Difference 3
C++:  `btext_ = context.data();` (line 495)
Java: (no equivalent field)
Note: C++ stores btext_ for debugging FormatCapture; Java omits this debug field

---

### Method: search (entry point)
C++ Location: nfa.cc:632-652 (Prog::SearchNFA)
Java Location: Nfa.java:47-105

#### Difference 4
C++:  `if (kind == kFullMatch) { anchor = kAnchored; if (nmatch == 0) { match = &sp; nmatch = 1; } }`
Java: (no equivalent - fullMatch handled by separate method)
Note: C++ SearchNFA handles kFullMatch internally; Java has separate fullMatch() method

#### Difference 5
C++:  `if (kind == kFullMatch && EndPtr(match[0]) != EndPtr(text)) return false;`
Java: `return m[0] == 0 && m[1] == text.length();` (in fullMatch method at line 125)
Note: C++ post-checks full match in SearchNFA; Java does post-check in separate fullMatch() method

#### Difference 6
C++:  `if (nsubmatch < 0) { ABSL_LOG(DFATAL) << "Bad args: nsubmatch=" << nsubmatch; return false; }`
Java: `if (submatch != null && (submatch.length % 2) != 0) { throw new IllegalArgumentException("submatch length must be even: " + submatch.length); }`
Note: C++ checks for negative nsubmatch; Java checks for odd submatch array length

---

### Method: AllocThread
C++ Location: nfa.cc:160-174
Java Location: Nfa.java:565-581

#### Difference 7
C++:  `t->capture = new const char*[ncapture_];`
Java: `if (threadCapture[id] == null || threadCapture[id].length != ncapture) { threadCapture[id] = new int[ncapture]; }`
Note: C++ always allocates new capture array for new threads; Java conditionally allocates only if null or wrong size

---

### Method: Incref
C++ Location: nfa.cc:176-180
Java Location: Nfa.java:597-601

#### Difference 8
C++:  `ABSL_DCHECK(t != NULL);`
Java: (no assertion)
Note: C++ has debug assertion that thread is non-null; Java omits assertion

---

### Method: Decref
C++ Location: nfa.cc:182-190
Java Location: Nfa.java:603-611

#### Difference 9
C++:  `ABSL_DCHECK(t != NULL);`
Java: (no assertion)
Note: C++ has debug assertion that thread is non-null; Java omits assertion

#### Difference 10
C++:  `ABSL_DCHECK_EQ(t->ref, 0);`
Java: (no assertion)
Note: C++ asserts ref count is exactly 0 before adding to freelist; Java omits assertion

---

### Method: AddToThreadq
C++ Location: nfa.cc:196-323
Java Location: Nfa.java:432-554

#### Difference 11
C++:  `stk[nstk++] = {id0, NULL};`
Java: `addIdStack[sp] = id0; addRestoreThreadStack[sp] = 0; sp++;`
Note: C++ uses struct AddState; Java uses parallel arrays

#### Difference 12
C++:  `ABSL_DCHECK_LE(nstk, stack_.size());`
Java: (check in pushAddState: `if (pos >= addIdStack.length) { throw new IllegalStateException(...); }`)
Note: C++ uses debug assertion; Java throws exception on overflow

#### Difference 13
C++:  `if (ExtraDebug) absl::FPrintF(stderr, "  [%d%s]\n", id, FormatCapture(t0->capture));`
Java: (no equivalent)
Note: C++ has debug printing when revisiting an instruction; Java omits debug output

#### Difference 14 - kInstFail handling
C++:  `case kInstFail: break;`
Java: `case FAIL -> { id = 0; }`
Note: C++ breaks out of switch; Java explicitly sets id = 0 then breaks from inner loop

#### Difference 15 - kInstAltMatch handling
C++:  `ABSL_DCHECK(!ip->last());`
Java: `if (ip.last()) { id = 0; break; }`
Note: C++ asserts last() is false; Java explicitly handles case where last() is true

#### Difference 16 - kInstByteRange debug
C++:  `if (ExtraDebug) absl::FPrintF(stderr, " + %d%s\n", id, FormatCapture(t0->capture));`
Java: (no equivalent)
Note: C++ has debug printing on ByteRange match; Java omits debug output

#### Difference 17 - kInstMatch debug
C++:  `if (ExtraDebug) absl::FPrintF(stderr, " ! %d%s\n", id, FormatCapture(t0->capture));`
Java: (no equivalent)
Note: C++ has debug printing on Match; Java omits debug output

#### Difference 18 - kInstAlt handling
C++:  `default: ABSL_LOG(DFATAL) << "unhandled " << ip->opcode() << " in AddToThreadq"; break;`
Java: `case ALT -> { pushAddState(ip.out1(), 0, sp++); id = ip.out(); }`
Note: C++ logs fatal error for unhandled opcodes including ALT; Java explicitly handles ALT

---

### Method: Step
C++ Location: nfa.cc:332-431
Java Location: Nfa.java:346-430

#### Difference 19 - longest match pruning
C++:  `if (matched_ && match_[0] < t->capture[0])`
Java: `if (matched && match[0] >= 0 && match[0] < threadCapture[t][0])`
Note: Java adds extra check `match[0] >= 0` since -1 is sentinel for unset

#### Difference 20 - Step signature
C++:  `int NFA::Step(Threadq* runq, Threadq* nextq, int c, absl::string_view context, const char* p)`
Java: `private int step(SparseIntArray runq, SparseIntArray nextq, int c, int p)`
Note: C++ passes context as parameter; Java uses instance field

#### Difference 21 - kInstByteRange call
C++:  `AddToThreadq(nextq, ip->out(), c, context, p, t);`
Java: `addToThreadq(nextq, ip.out(), c, p, t);`
Note: C++ passes context; Java doesn't (uses instance field)

#### Difference 22 - kInstMatch null pointer handling
C++:  `if (p == NULL) { CopyCapture(match_, t->capture); match_[1] = p; matched_ = true; break; }`
Java: (no equivalent)
Note: C++ has special case for null pointer to avoid undefined behavior; Java doesn't need this (uses integer indices)

#### Difference 23 - default case
C++:  `default: ABSL_LOG(DFATAL) << "Unhandled " << ip->opcode() << " in step"; break;`
Java: `default -> { // Unexpected in flattened programs... }`
Note: C++ logs fatal error; Java has empty default with comment

---

### Method: search (main loop)
C++ Location: nfa.cc:510-611
Java Location: Nfa.java:237-327

#### Difference 24 - debug printing
C++:  Lines 511-528 (extensive debug printing block with ExtraDebug)
Java: (no equivalent)
Note: C++ has debug printing for current character and thread queue; Java omits all debug output

#### Difference 25 - null pointer loop termination
C++:  `if (p == NULL) { (void) Step(runq, nextq, -1, context, p); ... break; }`
Java: (no equivalent)
Note: C++ has special null pointer handling at end of loop; Java doesn't need this

#### Difference 26 - prefix accel return value handling
C++:  `p = reinterpret_cast<const char*>(prog_->PrefixAccel(p, etext_ - p)); if (p == NULL) p = etext_;`
Java: `int found = prog.prefixAccel(bytes, p, textEnd - p); if (found < 0) { p = textEnd; } else { p = found; }`
Note: C++ uses NULL return; Java uses -1 return value

#### Difference 27 - byte character after prefix accel
C++:  (no recalculation of c after prefix accel)
Java: `c = (p < textEnd) ? (bytes[p] & 0xFF) : -1;`
Note: Java recalculates c after prefix acceleration; C++ passes expression to AddToThreadq

#### Difference 28 - runq empty check
C++:  `if (runq->size() == 0) { if (ExtraDebug) absl::FPrintF(stderr, "dead\n"); break; }`
Java: `if (runq.isEmpty()) { break; }`
Note: C++ has debug print; Java doesn't

#### Difference 29 - DCHECK after Step
C++:  `ABSL_DCHECK_EQ(runq->size(), 0);`
Java: (no assertion)
Note: C++ asserts runq is empty after Step; Java omits assertion

---

### Method: writeSubmatch
C++ Location: nfa.cc:618-627 (in Search method)
Java Location: Nfa.java:329-344

#### Difference 30 - submatch output format
C++:  `submatch[i] = absl::string_view(match_[2 * i], static_cast<size_t>(match_[2 * i + 1] - match_[2 * i]));`
Java: `out[o] = a - textBegin; out[o + 1] = b - textBegin;`
Note: C++ returns string_view (pointer+length); Java returns pair of integer offsets

#### Difference 31 - unmatched group handling
C++:  (no explicit handling - null pointers remain)
Java: `if (a < 0 || b < 0) { out[o] = -1; out[o + 1] = -1; }`
Note: Java explicitly writes -1 for unmatched groups; C++ relies on null pointers

---

### Method: emptyFlags
C++ Location: (in prog.cc as Prog::EmptyFlags, not nfa.cc)
Java Location: Nfa.java:618-667

#### Difference 32
C++:  (EmptyFlags is defined in prog.cc, takes context and p as parameters)
Java: `private int emptyFlags(int p)` (instance method using ctxBegin, ctxEnd fields)
Note: C++ EmptyFlags is in prog.cc; Java has its own implementation in NfaImpl

---

### Method: isWordChar
C++ Location: (in prog.cc as IsWordChar)
Java Location: Nfa.java:669-676

#### Difference 33
C++:  (defined in prog.cc)
Java: `private static boolean isWordChar(byte b)`
Note: C++ IsWordChar is in prog.cc; Java duplicates the implementation in NfaImpl

---

### Method: FormatCapture
C++ Location: nfa.cc:433-447
Java Location: (not present)

#### Difference 34
C++:  `std::string NFA::FormatCapture(const char** capture) { ... }`
Java: (no equivalent)
Note: C++ has debug formatting method; Java omits it entirely

---

### Method: Fanout
C++ Location: nfa.cc:660-712
Java Location: (not present)

#### Difference 35
C++:  `void Prog::Fanout(SparseArray<int>* fanout) { ... }`
Java: (no equivalent)
Note: C++ has Fanout analysis method; Java doesn't implement it (may be elsewhere or omitted)

---

### Method: pushAddState
C++ Location: (not present - uses inline struct assignment)
Java Location: Nfa.java:556-563

#### Difference 36
C++:  `stk[nstk++] = {id+1, NULL};` (inline struct assignment)
Java: `private void pushAddState(int id, int restoreThread, int pos) { ... }`
Note: Java has explicit helper method with bounds checking; C++ uses inline struct assignment

---

### Method: ensureThreadCapacity
C++ Location: (not present - uses std::deque that grows automatically)
Java Location: Nfa.java:583-595

#### Difference 37
C++:  `arena_.emplace_back();` (deque auto-grows)
Java: `private void ensureThreadCapacity(int id) { ... newLen *= 2; ... Arrays.copyOf(...); }`
Note: C++ uses std::deque with automatic growth; Java manually manages array growth

---

### Method: NFA destructor
C++ Location: nfa.cc:154-158
Java Location: (not present - relies on GC)

#### Difference 38
C++:  `NFA::~NFA() { delete[] match_; for (const Thread& t : arena_) delete[] t.capture; }`
Java: (no destructor - garbage collected)
Note: C++ has explicit destructor to free memory; Java relies on garbage collection

---

### Summary for Nfa.java vs nfa.cc
- Methods compared: 14 (AllocThread, Incref, Decref, AddToThreadq, Step, Search, CopyCapture, emptyFlags, isWordChar, writeSubmatch, constructor, entry point, pushAddState, ensureThreadCapacity)
- Total differences found: 38
