I'll read both files to perform a detailed line-by-line comparison.
Now I'll analyze both files and compare them method by method.

## File: Simplifier.java vs simplify.cc

### Method: simplify (entry point)
C++ Location: simplify.cc:180-199 (Regexp::Simplify)
Java Location: Simplifier.java:27-32

#### Difference 1
C++:  `Regexp* Regexp::Simplify() { CoalesceWalker cw; Regexp* cre = cw.Walk(this, NULL); if (cre == NULL) return NULL; if (cw.stopped_early()) { cre->Decref(); return NULL; } SimplifyWalker sw; Regexp* sre = sw.Walk(cre, NULL); ... }`
Java: `public static Regexp simplify(Regexp re) { requireNonNull(re, "re is null"); return simplifyInternal(Coalescer.coalesce(re), Mode.FULL); }`
Note: C++ checks for stopped_early() after both walker invocations. Java does not have this check (possibly unnecessary in Java walker implementation).

### Method: simplifyForParse
C++ Location: N/A
Java Location: Simplifier.java:40-44

#### Difference 2
C++:  No equivalent method exists
Java: `public static Regexp simplifyForParse(Regexp re) { requireNonNull(re, "re is null"); return simplifyInternal(re, Mode.PARSE); }`
Note: Java has a PARSE mode that doesn't exist in C++. The C++ upstream doesn't have a parse-time simplification concept at this level.

### Method: simplifyInternal (main switch)
C++ Location: simplify.cc:467-572 (SimplifyWalker::PostVisit)
Java Location: Simplifier.java:48-81

#### Difference 3
C++:  `case kRegexpNoMatch: case kRegexpEmptyMatch: case kRegexpLiteral: case kRegexpLiteralString: case kRegexpBeginLine: case kRegexpEndLine: case kRegexpBeginText: case kRegexpWordBoundary: case kRegexpNoWordBoundary: case kRegexpEndText: case kRegexpAnyChar: case kRegexpAnyByte: case kRegexpHaveMatch: re->simple_ = true; return re->Incref();`
Java: `case NO_MATCH, EMPTY_MATCH, ANY_CHAR, ANY_BYTE, BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT, HAVE_MATCH -> re; case LITERAL -> re;`
Note: C++ includes LITERAL and LITERAL_STRING in the same case block; Java handles LITERAL separately but the behavior is the same (returns the input).

#### Difference 4
C++:  N/A (handled in same case as above)
Java: `case LITERAL_STRING -> { int[] runes = re.runes(); if (runes.length == 1) { yield Regexp.literal(flags, runes[0]); } yield re; }`
Note: Java has special handling to convert single-rune LITERAL_STRING to LITERAL. C++ does not have this optimization.

#### Difference 5
C++:  `case kRegexpCapture: { Regexp* newsub = child_args[0]; if (newsub == re->sub()[0]) { newsub->Decref(); re->simple_ = true; return re->Incref(); } Regexp* nre = new Regexp(kRegexpCapture, re->parse_flags()); nre->AllocSub(1); nre->sub()[0] = newsub; nre->cap_ = re->cap(); nre->simple_ = true; return nre; }`
Java: `case CAPTURE -> { Regexp sub = simplifyInternal(re.sub(0), mode); yield Regexp.capture(flags, sub, re.cap(), re.name()); }`
Note: C++ preserves capture if child unchanged; Java always creates new capture. Also Java passes `re.name()` which C++ doesn't show preserving.

#### Difference 6
C++:  `case kRegexpConcat: case kRegexpAlternate: { if (!ChildArgsChanged(re, child_args)) { re->simple_ = true; return re->Incref(); } Regexp* nre = new Regexp(re->op(), re->parse_flags()); nre->AllocSub(re->nsub()); Regexp** nre_subs = nre->sub(); for (int i = 0; i < re->nsub(); i++) nre_subs[i] = child_args[i]; nre->simple_ = true; return nre; }`
Java: `case CONCAT -> simplifyConcat(mode, flags, re); case ALTERNATE -> simplifyAlternate(mode, flags, re);`
Note: C++ handles CONCAT and ALTERNATE identically (just propagate simplified children). Java handles them in separate methods with additional logic (literal merging for CONCAT, char class merging and factoring for ALTERNATE).

### Method: simplifyUnary (STAR, PLUS, QUEST)
C++ Location: simplify.cc:521-547 (PostVisit case kRegexpStar/Plus/Quest)
Java Location: Simplifier.java:85-112

#### Difference 7
C++:  `if (newsub->op() == kRegexpEmptyMatch) return newsub;`
Java: `if (sub.op() == RegexpOp.EMPTY_MATCH) { return Regexp.emptyMatch(flags); }`
Note: C++ returns the newsub itself; Java creates a new emptyMatch. Semantically equivalent but different object handling.

#### Difference 8
C++:  N/A
Java: `if (sub.op() == RegexpOp.NO_MATCH) { return switch (op) { case STAR, QUEST -> Regexp.emptyMatch(flags); case PLUS -> Regexp.noMatch(flags); }; }`
Note: Java has special handling for NO_MATCH sub. C++ does not have this case in the PostVisit for star/plus/quest.

#### Difference 9
C++:  `if (re->op() == newsub->op() && re->parse_flags() == newsub->parse_flags()) return newsub;`
Java: `if (sub.op() == switch (op) { case STAR -> RegexpOp.STAR; case PLUS -> RegexpOp.PLUS; case QUEST -> RegexpOp.QUEST; } && sub.parseFlags() == flags) { return sub; }`
Note: Both check same-op idempotency. C++ compares re->op() to newsub->op(). Java compares the unary op enum to the sub's op. Equivalent logic.

### Method: simplifyRepeat
C++ Location: simplify.cc:602-673 (SimplifyWalker::SimplifyRepeat)
Java Location: Simplifier.java:114-192

#### Difference 10
C++:  `Regexp* SimplifyWalker::SimplifyRepeat(Regexp* re, int min, int max, Regexp::ParseFlags f)`
Java: `private static Regexp simplifyRepeat(Mode mode, int flags, Regexp re)`
Note: C++ receives min/max as parameters; Java extracts them from re inside the method.

#### Difference 11
C++:  N/A (no mode concept)
Java: `if (mode == Mode.PARSE) { return Regexp.repeat(flags, sub, min, max); }`
Note: Java has PARSE mode that preserves counted repeats. C++ has no equivalent.

#### Difference 12
C++:  N/A (handled in PostVisit case kRegexpRepeat)
Java: `if (sub.op() == RegexpOp.EMPTY_MATCH) { return sub; }`
Note: Java checks EMPTY_MATCH in simplifyRepeat. C++ checks this in PostVisit before calling SimplifyRepeat: `if (newsub->op() == kRegexpEmptyMatch) return newsub;`

#### Difference 13
C++:  `if (IsEmptyOp(re) || ((re->op() == kRegexpConcat || re->op() == kRegexpAlternate) && std::all_of(re->sub(), re->sub() + re->nsub(), IsEmptyOp)))`
Java: `if (isEmptyOp(sub) || isConcatOrAlternateOfEmptyOps(sub))`
Note: C++ uses std::all_of; Java uses a separate helper method. Equivalent logic.

#### Difference 14
C++:  `min = std::min(min, 1); max = std::min(max, 1);`
Java: `min = Math.min(min, 1); max = Math.min(max, 1);`
Note: Equivalent logic, different math library.

#### Difference 15
C++:  `return Regexp::Star(re->Incref(), f);`
Java: `return Regexp.star(flags, sub);`
Note: C++ uses Incref() for reference counting; Java uses garbage collection (no ref counting).

#### Difference 16
C++:  `PODArray<Regexp*> nre_subs(min); for (int i = 0; i < min-1; i++) nre_subs[i] = re->Incref(); nre_subs[min-1] = Regexp::Plus(re->Incref(), f); return Regexp::Concat(nre_subs.data(), min, f);`
Java: `List<Regexp> subs = new ArrayList<>(min); for (int i = 0; i < min - 1; i++) { subs.add(sub); } subs.add(Regexp.plus(flags, sub)); return Regexp.concat(flags, subs);`
Note: C++ uses PODArray with explicit size; Java uses ArrayList. Equivalent logic.

#### Difference 17
C++:  `return new Regexp(kRegexpEmptyMatch, f);`
Java: `return Regexp.emptyMatch(flags);`
Note: Different construction syntax, equivalent semantics.

#### Difference 18
C++:  `return re->Incref();`
Java: `return sub;`
Note: For min==1, max==1 case. C++ uses re->Incref(), Java returns sub directly (the simplified child, not the original).

#### Difference 19
C++:  `PODArray<Regexp*> nre_subs(min); for (int i = 0; i < min; i++) nre_subs[i] = re->Incref(); nre = Regexp::Concat(nre_subs.data(), min, f);`
Java: `List<Regexp> prefix = new ArrayList<>(min); for (int i = 0; i < min; i++) { prefix.add(sub); } out = (prefix.size() == 1) ? prefix.getFirst() : Regexp.concat(flags, prefix);`
Note: Java has special case for size==1 to avoid creating a concat of one element. C++ always creates concat.

#### Difference 20
C++:  `suf = Regexp::Quest(Concat2(re->Incref(), suf, f), f);`
Java: `suf = Regexp.quest(flags, Regexp.concat(flags, List.of(sub, suf)));`
Note: C++ uses Concat2 helper; Java uses Regexp.concat with List.of.

#### Difference 21
C++:  `if (nre == NULL) nre = suf; else nre = Concat2(nre, suf, f);`
Java: `out = (out == null) ? suf : Regexp.concat(flags, List.of(out, suf));`
Note: Equivalent logic with different syntax.

#### Difference 22
C++:  `ABSL_LOG(DFATAL) << "Malformed repeat of " << re->ToString() << " min " << min << " max " << max; return new Regexp(kRegexpNoMatch, f);`
Java: `return Regexp.noMatch(flags);`
Note: C++ logs a fatal error for degenerate cases; Java just returns noMatch silently.

### Method: isEmptyOp
C++ Location: simplify.cc:587-594 (IsEmptyOp)
Java Location: Simplifier.java:194-200

#### Difference 23
C++:  `return (re->op() == kRegexpBeginLine || re->op() == kRegexpEndLine || re->op() == kRegexpWordBoundary || re->op() == kRegexpNoWordBoundary || re->op() == kRegexpBeginText || re->op() == kRegexpEndText);`
Java: `return switch (re.op()) { case BEGIN_LINE, END_LINE, WORD_BOUNDARY, NO_WORD_BOUNDARY, BEGIN_TEXT, END_TEXT -> true; default -> false; };`
Note: Same ops checked. Equivalent logic.

### Method: isConcatOrAlternateOfEmptyOps
C++ Location: simplify.cc:607-609 (inline std::all_of call)
Java Location: Simplifier.java:202-213

#### Difference 24
C++:  `((re->op() == kRegexpConcat || re->op() == kRegexpAlternate) && std::all_of(re->sub(), re->sub() + re->nsub(), IsEmptyOp))`
Java: `if (re.op() != RegexpOp.CONCAT && re.op() != RegexpOp.ALTERNATE) { return false; } for (Regexp sub : re.subs()) { if (!isEmptyOp(sub)) { return false; } } return true;`
Note: C++ inlines this; Java has a separate method. Equivalent logic.

### Method: simplifyConcat
C++ Location: simplify.cc:490-504 (PostVisit case kRegexpConcat)
Java Location: Simplifier.java:215-250

#### Difference 25
C++:  N/A (C++ just builds new concat with simplified children)
Java: `if (s.op() == RegexpOp.NO_MATCH) { return Regexp.noMatch(flags); }`
Note: Java short-circuits on NO_MATCH. C++ doesn't have this optimization in PostVisit.

#### Difference 26
C++:  N/A
Java: `if (s.op() == RegexpOp.EMPTY_MATCH) { continue; }`
Note: Java filters out EMPTY_MATCH from concat. C++ doesn't do this in PostVisit.

#### Difference 27
C++:  N/A
Java: `if (s.op() == RegexpOp.CONCAT) { out.addAll(s.subs()); }`
Note: Java flattens nested concats. C++ doesn't do this in PostVisit.

#### Difference 28
C++:  N/A
Java: `if (!out.isEmpty() && isLiteralOrLiteralString(out.getLast()) && isLiteralOrLiteralString(s) && out.getLast().parseFlags() == s.parseFlags()) { Regexp merged = mergeAdjacentLiterals(out.getLast(), s); out.set(out.size() - 1, merged); }`
Note: Java merges adjacent literals into literal strings. C++ doesn't do this.

#### Difference 29
C++:  N/A
Java: `if (out.isEmpty()) { return Regexp.emptyMatch(flags); }`
Note: Java returns emptyMatch for empty result. C++ doesn't have this case.

#### Difference 30
C++:  N/A
Java: `if (out.size() == 1) { return out.getFirst(); }`
Note: Java unwraps single-element concat. C++ doesn't do this in PostVisit.

### Method: simplifyAlternate
C++ Location: simplify.cc:490-504 (same as CONCAT case)
Java Location: Simplifier.java:271-329

#### Difference 31
C++:  (same as CONCAT - just propagates children)
Java: `if (s.op() == RegexpOp.NO_MATCH) { continue; }`
Note: Java filters out NO_MATCH from alternations. C++ doesn't do this.

#### Difference 32
C++:  N/A
Java: `if (s.op() == RegexpOp.ALTERNATE) { out.addAll(s.subs()); }`
Note: Java flattens nested alternates. C++ doesn't do this.

#### Difference 33
C++:  N/A
Java: `if (out.isEmpty()) { return Regexp.noMatch(flags); }`
Note: Java returns noMatch for empty alternation. C++ doesn't have this case.

#### Difference 34
C++:  N/A
Java: The entire PARSE mode handling with AnyByte/AnyChar dominance (lines 294-320) and factorAlternationForParse has no C++ equivalent.
Note: Java has extensive parse-time factoring logic that doesn't exist in upstream.

#### Difference 35
C++:  N/A
Java: `List<Regexp> merged = mergeAlternateCharClassesWherePossible(mode, flags, out);`
Note: Java has char class merging in alternations that C++ doesn't have in simplify.cc.

### Method: simplifyCharClass
C++ Location: simplify.cc:677-687 (SimplifyWalker::SimplifyCharClass)
Java Location: Simplifier.java:331-383

#### Difference 36
C++:  `if (cc->empty()) return new Regexp(kRegexpNoMatch, re->parse_flags());`
Java: `if (cc.isEmpty()) { return Regexp.noMatch(flags); }`
Note: Equivalent logic.

#### Difference 37
C++:  `if (cc->full()) return new Regexp(kRegexpAnyChar, re->parse_flags());`
Java: (lines 339-348) `if (cc.nranges() == 1) { RuneRange rr = cc.range(0); int maxRune = Regexp.maxRune(flags); if (rr.lo() == 0 && rr.hi() == maxRune) { if (maxRune == 0xFF) { return Regexp.anyByte(flags); } return Regexp.anyChar(flags); } }`
Note: C++ uses cc->full() which checks for full unicode range. Java manually checks if single range covers 0 to maxRune, and distinguishes ANY_BYTE (maxRune==0xFF) from ANY_CHAR.

#### Difference 38
C++:  N/A
Java: (lines 354-374) The entire FoldCase literal optimization for ASCII letter pairs [Aa] has no C++ equivalent in SimplifyCharClass.
Note: Java has special case to fold letter-pair classes to FoldCase literals.

#### Difference 39
C++:  N/A
Java: (lines 376-382) `if (cc.nrunes() == 1 && cc.nranges() == 1) { RuneRange rr = cc.range(0); if (rr.lo() == rr.hi()) { return Regexp.literal(flags, rr.lo()); } }`
Note: Java has optimization to convert single-rune char class to literal. C++ doesn't have this.

#### Difference 40
C++:  `return re->Incref();`
Java: `return Regexp.charClass(flags, cc);`
Note: C++ returns the original (with incref); Java may create new or return existing.

### Method: Coalescer/CoalesceWalker
C++ Location: simplify.cc:109-133, 221-306, 308-445
Java Location: Simplifier.java:735-983

#### Difference 41
C++:  `virtual Regexp* Copy(Regexp* re);` (line 114) and `Regexp* CoalesceWalker::Copy(Regexp* re) { return re->Incref(); }` (lines 221-223)
Java: N/A (no Copy method in Java walker)
Note: C++ has Copy method; Java walker implementation doesn't need it.

#### Difference 42
C++:  `virtual Regexp* ShortVisit(Regexp* re, Regexp* parent_arg);` with body that logs DFATAL (lines 225-231)
Java: `protected Regexp shortVisit(Regexp re, Regexp parentArg) { throw new IllegalStateException("Coalescer should not short-visit"); }` (lines 754-757)
Note: C++ logs and returns incref; Java throws exception.

#### Difference 43
C++:  PostVisit (lines 233-306) handles non-concat case: `Regexp* nre = new Regexp(re->op(), re->parse_flags()); nre->AllocSub(re->nsub()); Regexp** nre_subs = nre->sub(); for (int i = 0; i < re->nsub(); i++) nre_subs[i] = child_args[i]; if (re->op() == kRegexpRepeat) { nre->min_ = re->min(); nre->max_ = re->max(); } else if (re->op() == kRegexpCapture) { nre->cap_ = re->cap(); }`
Java: `rebuildNonConcat` (lines 827-844) handles this with switch: `case CAPTURE -> Regexp.capture(re.parseFlags(), childArgs.getFirst(), re.cap(), re.name()); case STAR -> Regexp.rawUnary(RegexpOp.STAR, re.parseFlags(), childArgs.getFirst()); ...`
Note: C++ directly sets fields; Java uses factory methods. Java also preserves name() for captures.

#### Difference 44
C++:  DoCoalesce creates `new Regexp(kRegexpEmptyMatch, Regexp::NoParseFlags)` (line 415)
Java: `Regexp.emptyMatch(0)` (lines 927, 932, 938, 948, 956, 973)
Note: C++ uses NoParseFlags; Java uses 0. These should be equivalent.

#### Difference 45
C++:  DoCoalesce (lines 348-445) handles memory with Decref at end: `r1->Decref(); r2->Decref();` (lines 443-444)
Java: doCoalesce (lines 899-981) returns Pair without explicit memory management
Note: C++ has explicit reference counting cleanup; Java uses garbage collection.

### Method: CanCoalesce
C++ Location: simplify.cc:308-346
Java Location: Simplifier.java:857-885

#### Difference 46
C++:  `r2->runes()[0] == r1->sub()[0]->rune()` (line 338)
Java: `r2.runes()[0] == r1.sub(0).rune()` (line 879)
Note: Functionally equivalent; different syntax for array access vs method call.

#### Difference 47
C++:  Checks literal string length implicitly (accessing runes()[0] assumes non-empty)
Java: `r2.runes().length > 0` (line 878)
Note: Java has explicit length check before accessing first element.

### Method: DoCoalesce
C++ Location: simplify.cc:348-445
Java Location: Simplifier.java:899-981

#### Difference 48
C++:  Uses goto LeaveEmpty for multiple cases (lines 385, 390, 395, 403, 412, 430)
Java: Each case explicitly returns a Pair (lines 927, 932, 938, 948, 956, 973)
Note: C++ uses goto for common epilogue; Java uses explicit returns.

#### Difference 49
C++:  `Regexp* nre = Regexp::Repeat(r1->sub()[0]->Incref(), r1->parse_flags(), 0, 0);` (lines 352-353)
Java: Creates repeat at the end: `Regexp nre = Regexp.repeat(r1.parseFlags(), atom, min, max);` (lines 927, 932, etc.)
Note: C++ pre-creates the repeat with dummy values then modifies; Java creates with final values.

#### Difference 50
C++:  `default: nre->Decref(); ABSL_LOG(DFATAL) << "DoCoalesce failed: r1->op() is " << r1->op(); return;` (lines 376-379)
Java: `default -> throw new IllegalStateException("unexpected r1 op: " + r1.op());` (line 922)
Note: C++ logs and returns; Java throws exception.

### Method: mergeAlternateCharClassesWherePossible
C++ Location: N/A
Java Location: Simplifier.java:385-433

#### Difference 51
C++:  No equivalent exists
Java: Entire method `mergeAlternateCharClassesWherePossible` with `mergeableAlternateSub` helper
Note: Java has char class merging in alternations that C++ simplify.cc doesn't have.

### Method: factorAlternation* methods
C++ Location: N/A
Java Location: Simplifier.java:446-604 (factorAlternationForParse, factorAlternationRound1, factorAlternationRound2, factorAlternationRound3)

#### Difference 52
C++:  No equivalent exists
Java: Entire factorization logic for parse mode (lines 446-604)
Note: Java has extensive alternation factoring that C++ simplify.cc doesn't have.

### Method: leadingString, leadingRegexp, removeLeadingString, removeLeadingRegexp, commonPrefixLength
C++ Location: N/A
Java Location: Simplifier.java:631-732

#### Difference 53
C++:  No equivalent exists in simplify.cc
Java: These helper methods (lines 631-732)
Note: Java has these helpers for factorization logic that doesn't exist in C++ simplify.cc.

### Method: ComputeSimple
C++ Location: simplify.cc:47-102
Java Location: N/A

#### Difference 54
C++:  `bool Regexp::ComputeSimple()` with full implementation checking if regexp is already simple
Java: No equivalent method
Note: C++ has ComputeSimple to determine if simplification is needed; Java doesn't have this optimization.

### Method: SimplifyWalker::PreVisit
C++ Location: simplify.cc:459-465
Java Location: N/A

#### Difference 55
C++:  `Regexp* SimplifyWalker::PreVisit(Regexp* re, Regexp* parent_arg, bool* stop) { if (re->simple()) { *stop = true; return re->Incref(); } return NULL; }`
Java: No equivalent (Java's simplifyInternal doesn't short-circuit on already-simple)
Note: C++ checks re->simple() to skip already-simplified regexps; Java doesn't have this optimization.

### Method: SimplifyRegexp
C++ Location: simplify.cc:26-43
Java Location: N/A

#### Difference 56
C++:  `bool Regexp::SimplifyRegexp(absl::string_view src, ParseFlags flags, std::string* dst, RegexpStatus* status)` - parses, simplifies, and returns string
Java: No equivalent - Java separates parsing and simplification
Note: C++ has combined parse+simplify+toString; Java doesn't have this convenience method.

### Summary for Simplifier.java vs simplify.cc
- Methods compared: 18 (including Coalescer inner class)
- Total differences found: 56

Key observations:
1. Java has a Mode.PARSE concept that doesn't exist in C++ simplify.cc
2. Java has extensive alternation factoring logic (factorAlternationForParse, etc.) not in C++
3. Java merges adjacent literals in concat and char classes in alternations - C++ doesn't
4. Java flattens nested concat/alternate - C++ doesn't
5. Java has special handling for NO_MATCH and EMPTY_MATCH in concat/alternate - C++ doesn't
6. Java has char class optimizations (single-rune to literal, full-range to anyByte/anyChar) beyond C++
7. C++ has ComputeSimple() and PreVisit short-circuit for already-simple regexps - Java doesn't
8. C++ uses reference counting (Incref/Decref); Java uses garbage collection
