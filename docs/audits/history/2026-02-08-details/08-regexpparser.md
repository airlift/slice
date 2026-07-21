I'll read both files to perform a comprehensive line-by-line audit.
Let me read the C++ file in parts.
Now I have read both files completely. Let me compile the comprehensive audit.

## File: RegexpParser.java vs parse.cc

### Method: parse (entry point)
C++ Location: parse.cc:2265-2527
Java Location: RegexpParser.java:43-67

#### Difference 1
C++:  `if (global_flags & Latin1) { std::string* tmp = new std::string; ConvertLatin1ToUTF8(t, tmp); status->set_tmp(tmp); t = *tmp; }`
Java: No Latin1 to UTF-8 conversion of the input string
Note: C++ converts Latin1 patterns to UTF-8 for internal processing; Java keeps bytes as-is

#### Difference 2
C++:  Uses stack-based parsing with `stacktop_`, `down_` linked list
Java: Uses recursive descent parsing (`parseAlt()` -> `parseConcat()` -> `parseRepeat()` -> `parseAtom()`)
Note: Fundamental architectural difference in parsing approach

#### Difference 3
C++:  `RegexpStatus xstatus; if (status == NULL) status = &xstatus;`
Java: `requireNonNull(pattern, "pattern is null");`
Note: C++ allows null status, Java requires non-null pattern

#### Difference 4
C++:  No explicit UTF-8 validation for non-Latin1 before parsing
Java: `if ((flags & Regexp.LATIN1) == 0 && !isValidUtf8(pattern)) { return new ParseResult(null, new RegexpStatus(RegexpStatusCode.BAD_UTF8, null), 0); }`
Note: Java pre-validates UTF-8 before parsing

#### Difference 5
C++:  `Regexp* result = ps.DoFinish();` returns directly
Java: `Regexp simplified = Simplifier.simplifyForParse(re);`
Note: Java applies simplification after parsing; C++ does not call simplify here

---

### Method: parseRegexp / Literal handling
C++ Location: parse.cc:2283-2293
Java Location: RegexpParser.java:151-169

#### Difference 6
C++:  `if (global_flags & Literal) { while (!t.empty()) { Rune r; if (StringViewToRune(&r, &t, status) < 0) return NULL; if (!ps.PushLiteral(r)) return NULL; } return ps.DoFinish(); }`
Java: `if ((flags & Regexp.LITERAL) != 0) { return parseLiteralString(); }`
Note: Both handle LITERAL flag, but implementation differs

---

### Method: PushCaret / parseAtom (^)
C++ Location: parse.cc:447-452
Java Location: RegexpParser.java:506-508

#### Difference 7
C++:  `if (flags_ & OneLine) { return PushSimpleOp(kRegexpBeginText); } return PushSimpleOp(kRegexpBeginLine);`
Java: `return ((flags & Regexp.ONE_LINE) != 0) ? Regexp.beginText(flags) : Regexp.beginLine(flags);`
Note: Equivalent logic, different syntax

---

### Method: PushDollar / parseAtom ($)
C++ Location: parse.cc:462-473
Java Location: RegexpParser.java:510-516

#### Difference 8
C++:  `if (flags_ & OneLine) { Regexp::ParseFlags oflags = flags_; flags_ = flags_ | WasDollar; bool ret = PushSimpleOp(kRegexpEndText); flags_ = oflags; return ret; }`
Java: `if ((flags & Regexp.ONE_LINE) != 0) { return Regexp.endText(flags | Regexp.WAS_DOLLAR); }`
Note: C++ temporarily modifies flags_, Java passes WAS_DOLLAR directly

---

### Method: PushDot / parseAtom (.)
C++ Location: parse.cc:476-485
Java Location: RegexpParser.java:486-505

#### Difference 9
C++:  `re->ccb_->AddRange(0, '\n' - 1); re->ccb_->AddRange('\n' + 1, rune_max_);`
Java: `if (maxRune < '\n') { bld.addRange(0, maxRune); } else { if ('\n' > 0) { bld.addRange(0, '\n' - 1); } if ('\n' + 1 <= maxRune) { bld.addRange('\n' + 1, maxRune); } }`
Note: Java has additional boundary checks for maxRune < '\n' case; C++ assumes '\n' < rune_max_

---

### Method: PushRepeatOp / applyRepeat (* + ?)
C++ Location: parse.cc:496-530
Java Location: RegexpParser.java:305-362

#### Difference 10
C++:  `if (stacktop_ == NULL || IsMarker(stacktop_->op())) { status_->set_code(kRegexpRepeatArgument); status_->set_error_arg(s); return false; }`
Java: `if (atom == null) { throw error(RegexpStatusCode.REPEAT_ARGUMENT, errorArgAtCurrent()); }`
Note: C++ checks stack, Java checks parsed atom

#### Difference 11
C++:  `Regexp::ParseFlags fl = flags_; if (nongreedy) fl = fl ^ NonGreedy;`
Java: `boolean nongreedy = !greedyByDefault; ... int nodeFlags = nongreedy ? (flags | Regexp.NON_GREEDY) : (flags & ~Regexp.NON_GREEDY);`
Note: C++ XORs NonGreedy flag; Java computes based on greedyByDefault

#### Difference 12
C++:  `if (op == stacktop_->op() && fl == stacktop_->parse_flags()) return true;`
Java: No equivalent squashing of duplicate operators
Note: C++ squashes **, ++, ??; Java does not implement this optimization

#### Difference 13
C++:  `if ((stacktop_->op() == kRegexpStar || stacktop_->op() == kRegexpPlus || stacktop_->op() == kRegexpQuest) && fl == stacktop_->parse_flags()) { stacktop_->op_ = kRegexpStar; return true; }`
Java: No equivalent squashing of *+, *?, +*, +?, ?*, ?+
Note: C++ squashes mixed repeat operators to star; Java does not

---

### Method: PushRepetition / applyRepeat ({m,n})
C++ Location: parse.cc:589-623
Java Location: RegexpParser.java:305-362

#### Difference 14
C++:  `if ((max != -1 && max < min) || min > maximum_repeat_count || max > maximum_repeat_count)`
Java: `if (m > MAX_REPEAT || (n != -1 && n > MAX_REPEAT))`
Note: C++ uses `maximum_repeat_count` variable; Java uses constant `MAX_REPEAT`

#### Difference 15
C++:  `static int maximum_repeat_count = 1000;`
Java: `MAX_REPEAT` imported from Regexp class
Note: C++ has fuzzing-modifiable limit; Java uses fixed constant

#### Difference 16
C++:  `if (min >= 2 || max >= 2) { RepetitionWalker w; if (w.Walk(stacktop_, maximum_repeat_count) == 0) {...} }`
Java: `if (wouldExceedRepeatLimit(atom, min, max)) { throw error(RegexpStatusCode.REPEAT_SIZE, null); }`
Note: C++ uses Walker pattern post-push; Java checks pre-creation with recursive function

---

### Method: MaybeParseRepetition / parseRepetitionCounts
C++ Location: parse.cc:1387-1417
Java Location: RegexpParser.java:417-452

#### Difference 17
C++:  `if (s.empty() || s[0] != '{') return false;`
Java: Method is called after `{` is already consumed
Note: C++ validates leading `{`; Java assumes `{` already consumed

#### Difference 18
C++:  `if (s.size() >= 2 && (*s)[0] == '0' && absl::ascii_isdigit((*s)[1] & 0xFF)) return false;`
Java: No disallow of leading zeros
Note: C++ disallows leading zeros in repeat counts; Java allows them

#### Difference 19
C++:  `if (n >= 100000000) return false;` (overflow avoidance)
Java: `if (value > MAX_REPEAT) { overflow = true; }`
Note: C++ uses hard overflow limit 100000000; Java caps at MAX_REPEAT

#### Difference 20
C++:  Returns `false` if pattern is malformed (treats `{` as literal)
Java: `throw error(RegexpStatusCode.MISSING_BRACKET, ...)` or `throw error(RegexpStatusCode.REPEAT_ARGUMENT, ...)`
Note: C++ returns failure to trigger literal treatment; Java throws exceptions

---

### Method: ParseInteger / parseDecimal
C++ Location: parse.cc:1359-1376
Java Location: RegexpParser.java:1472-1500

#### Difference 21
C++:  `if (s->empty() || !absl::ascii_isdigit((*s)[0] & 0xFF)) return false;`
Java: `if (atEnd()) { throw error(RegexpStatusCode.REPEAT_ARGUMENT, null); }`
Note: C++ returns false on empty; Java throws exception

#### Difference 22
C++:  `if (s->size() >= 2 && (*s)[0] == '0' && absl::ascii_isdigit((*s)[1] & 0xFF)) return false;`
Java: No leading zero check
Note: C++ rejects leading zeros; Java does not

---

### Method: DoLeftParen / parseGroup
C++ Location: parse.cc:632-638
Java Location: RegexpParser.java:547-639

#### Difference 23
C++:  `re->cap_ = ++ncap_;`
Java: `numCaptures++; if (numCaptures > MAX_CAP) { throw error(RegexpStatusCode.PATTERN_TOO_LARGE, null); } captureIndex = numCaptures;`
Note: Java checks MAX_CAP limit; C++ does not have this check

---

### Method: ParsePerlFlags / parseInlineFlags
C++ Location: parse.cc:2092-2244
Java Location: RegexpParser.java:641-671

#### Difference 24
C++:  `if ((t.size() > 3 && (t[2] == '=' || t[2] == '!')) || (t.size() > 4 && t[2] == '<' && (t[3] == '=' || t[3] == '!'))) { status_->set_code(kRegexpBadPerlOp); status_->set_error_arg(absl::string_view(t.data(), t[2] == '<' ? 4 : 3)); return false; }`
Java: `if (c == '=' || c == '!') { throw error(RegexpStatusCode.BAD_PERL_OP, ByteSlice.wrap(bytes, groupStart, 3)); } if (c == '<' && index + 1 < end) { int c2 = bytes[index + 1] & 0xFF; if (c2 == '=' || c2 == '!') { throw error(RegexpStatusCode.BAD_PERL_OP, ByteSlice.wrap(bytes, groupStart, 4)); } }`
Note: Both detect look-around assertions; slightly different bounds checking

#### Difference 25
C++:  `case '-': if (negated) goto BadPerlOp; negated = true; sawflags = false; break;`
Java: `if (c == '-') { consumeByte('-'); clearing = true; continue; }`
Note: C++ rejects double negation with error; Java allows `-` without prior flags

#### Difference 26
C++:  `if (negated && !sawflags) goto BadPerlOp;`
Java: No equivalent check
Note: C++ requires flags after `-`; Java allows standalone `-`

---

### Method: ParseCharClass / parseCharClass
C++ Location: parse.cc:1947-2051
Java Location: RegexpParser.java:727-839

#### Difference 27
C++:  `bool first = true; while (!s->empty() && ((*s)[0] != ']' || first))`
Java: `boolean first = true; while (!atEnd()) { int b = peekByte(); if (b == ']' && !first) { ... break; } }`
Note: Equivalent logic, different loop structure

#### Difference 28
C++:  `if ((*s)[0] == '-' && !first && !(flags_&PerlX) && (s->size() == 1 || (*s)[1] != ']'))`
Java: `if (b == '-' && !first && (flags & Regexp.PERL_X) == 0 && !isAtClassEndAfterDash())`
Note: Same logic, Java uses helper method `isAtClassEndAfterDash()`

#### Difference 29
C++:  `re->ccb_->AddRangeFlags(rr.lo, rr.hi, flags_ | Regexp::ClassNL);`
Java: `builder.addRangeFlags(lo, hi, nodeFlags | Regexp.CLASS_NL);`
Note: Same logic: explicit ranges don't filter newline

#### Difference 30
C++:  `if (negated) re->ccb_->Negate();`
Java: `if (negate) { builder.negate(); builder.removeAbove(Regexp.maxRune(nodeFlags)); } else { builder.removeAbove(Regexp.maxRune(nodeFlags)); }`
Note: Java calls removeAbove in both branches; C++ only negates

---

### Method: ParseCCRange / lookaheadIsRangeEnd
C++ Location: parse.cc:1921-1942
Java Location: RegexpParser.java:1035-1043

#### Difference 31
C++:  `if (s->size() >= 2 && (*s)[0] == '-' && (*s)[1] != ']')`
Java: `if (!atEnd() && peekByte() == '-' && lookaheadIsRangeEnd())`
Note: C++ inlines check; Java uses helper. Java's `lookaheadIsRangeEnd()` returns true when next != ']', which is opposite sense

#### Difference 32
C++:  `rr->hi = rr->lo;` (for single character)
Java: `int hi = lo;` then range assigned
Note: Equivalent logic

---

### Method: ParseEscape
C++ Location: parse.cc:1485-1630
Java Location: RegexpParser.java:1078-1179

#### Difference 33
C++:  `if (c < Runeself && !absl::ascii_isalnum(c)) { *rp = c; return true; }`
Java: `if (b < 0x80 && !Character.isLetterOrDigit((char) b)) { yield literalFromRune(b); }`
Note: Same logic, `Runeself` (0x80) vs literal `0x80`

#### Difference 34
C++:  Handles `\1`-`\7` backreferences: `case '1': ... case '7': if (s->empty() || (*s)[0] < '0' || (*s)[0] > '7') goto BadEscape;`
Java: `case '0', '1', '2', '3', '4', '5', '6', '7' -> literalFromRune(parseOctalEscape(b, flags, seqStart));`
Note: Both require second octal digit for `\1`-`\7`; same behavior

#### Difference 35
C++:  `case 'b': // disabled comment about backspace`
Java: No `\b` case in parseEscape (handled separately as word boundary)
Note: C++ has commented-out backspace code; Java handles `\b` in parseAtom

#### Difference 36
C++:  `case 'Q': case 'E':` handled in main parsing loop separately
Java: `case 'Q', 'E' -> throw error(RegexpStatusCode.BAD_ESCAPE, ByteSlice.wrap(bytes, seqStart, index - seqStart));`
Note: Java rejects `\Q` and `\E` in escape parsing; C++ handles them in main loop

---

### Method: parseOctalEscape
C++ Location: parse.cc:1517-1545
Java Location: RegexpParser.java:1343-1371

#### Difference 37
C++:  `code = c - '0'; if (!s->empty() && '0' <= (c = (*s)[0]) && c <= '7') { code = code * 8 + c - '0'; ...`
Java: `int code = firstDigit - '0'; for (int i = 0; i < 2; i++) { ... code = (code * 8) + (c - '0'); }`
Note: C++ uses inline loop; Java uses for loop with i < 2

#### Difference 38
C++:  Consumes up to 2 more octal digits after first
Java: Consumes up to 2 more octal digits after first  
Note: Same behavior, different structure

---

### Method: parseHexEscape
C++ Location: parse.cc:1548-1587
Java Location: RegexpParser.java:1294-1341

#### Difference 39
C++:  `if (StringViewToRune(&c, s, status) < 0) return false;`
Java: `int r = readRune(flags);`
Note: C++ handles UTF-8 decoding error; Java's readRune throws exception

#### Difference 40
C++:  `while (IsHex(c)) { nhex++; code = code * 16 + UnHex(c); if (code > rune_max) goto BadEscape; ...`
Java: `while (!atEnd() && peekByte() != '}') { int r = readRune(flags); int d = hexDigit(r); if (d < 0) { throw error(...); } digits++; value = (value << 4) | d; if (value > runeMax) { throw error(...); }`
Note: Similar logic; Java reads until `}`, C++ reads while hex

---

### Method: ParseUnicodeGroup / parseUnicodeGroupInto
C++ Location: parse.cc:1770-1855
Java Location: RegexpParser.java:1181-1229

#### Difference 41
C++:  `if (c != '{') { const char* p = seq.data() + 2; name = absl::string_view(p, static_cast<size_t>(s->data() - p)); }`
Java: `int nameStart = index; int r = readRune(parseFlags); int nameEnd = index; if (r == '^') { sign = -sign; nameStart = index; ... }`
Note: C++ gets name from already-consumed portion; Java tracks start/end positions

#### Difference 42
C++:  `if (!name.empty() && name[0] == '^') { sign = -sign; name.remove_prefix(1); }`
Java: `if (nameEnd > nameStart && (bytes[nameStart] & 0xFF) == '^') { sign = -sign; nameStart++; }`
Note: Same logic, different implementations

#### Difference 43
C++:  `const UGroup* g = LookupUnicodeGroup(name);`
Java: `CharClass group = UnicodeGroups.lookup(name);`
Note: C++ uses UGroup type; Java uses CharClass

---

### Method: AddUGroup / addGroup
C++ Location: parse.cc:1695-1738
Java Location: RegexpParser.java:888-929

#### Difference 44
C++:  `for (int i = 0; i < g->nr16; i++) { cc->AddRangeFlags(g->r16[i].lo, g->r16[i].hi, parse_flags); } for (int i = 0; i < g->nr32; i++) { cc->AddRangeFlags(g->r32[i].lo, g->r32[i].hi, parse_flags); }`
Java: `for (int i = 0; i < group.nranges(); i++) { RuneRange rr = group.range(i); cc.addRangeFlags(rr.lo(), rr.hi(), parseFlags); }`
Note: C++ has separate 16-bit and 32-bit range arrays; Java uses unified ranges

#### Difference 45
C++:  `if (next <= Runemax) cc->AddRangeFlags(next, Runemax, parse_flags);`
Java: `if (next <= Regexp.RUNEMAX) { cc.addRangeFlags(next, Regexp.RUNEMAX, parseFlags); }`
Note: Same logic, different constant names

---

### Method: AddFoldedRangeLatin1
C++ Location: parse.cc:341-352
Java Location: RegexpParser.java:1429-1451

#### Difference 46
C++:  `while (lo <= hi) { cc->AddRange(lo, lo); if ('A' <= lo && lo <= 'Z') { cc->AddRange(lo - 'A' + 'a', lo - 'A' + 'a'); } if ('a' <= lo && lo <= 'z') { cc->AddRange(lo - 'a' + 'A', lo - 'a' + 'A'); } lo++; }`
Java: `for (int r = lo; r <= hi; r++) { if ('A' <= r && r <= 'Z') { cc.addRange(r, r); cc.addRange(r + ('a' - 'A'), r + ('a' - 'A')); } else if ('a' <= r && r <= 'z') { cc.addRange(r, r); cc.addRange(r - ('a' - 'A'), r - ('a' - 'A')); } else { cc.addRange(r, r); } }`
Note: C++ always adds lo,lo first then fold; Java adds in conditional order

---

### Method: AddFoldedRange (Unicode)
C++ Location: parse.cc:357-406
Java Location: Not directly present - uses UnicodeCaseFold.cycleFoldRune in literalFromRune

#### Difference 47
C++:  Uses recursive `AddFoldedRange` with depth limit to handle Unicode case folding
Java: Uses `UnicodeCaseFold.cycleFoldRune` in a cycle loop
Note: Different approaches to Unicode case folding

---

### Method: CycleFoldRune
C++ Location: parse.cc:333-338
Java Location: Uses UnicodeCaseFold.cycleFoldRune (external)

#### Difference 48
C++:  `const CaseFold* f = LookupCaseFold(unicode_casefold, num_unicode_casefold, r); if (f == NULL || r < f->lo) return r; return ApplyFold(f, r);`
Java: Delegated to `UnicodeCaseFold.cycleFoldRune()`
Note: Same algorithm, different location

---

### Method: IsValidCaptureName / isValidCaptureNameRune
C++ Location: parse.cc:2054-2085
Java Location: RegexpParser.java:722-725, 931-943

#### Difference 49
C++:  Static lambda builds CharClass once: `static const CharClass* const cc = []() { CharClassBuilder ccb; for (absl::string_view group : {"Lu", "Ll", "Lt", "Lm", "Lo", "Nl", "Mn", "Mc", "Nd", "Pc"}) AddUGroup(&ccb, LookupGroup(group, unicode_groups, num_unicode_groups), +1, Regexp::NoParseFlags); return ccb.GetCharClass(); }();`
Java: Static field: `private static final CharClass VALID_CAPTURE_NAME = buildValidCaptureNameCharClass();`
Note: Same groups, different initialization patterns

---

### Method: posixCharClass
C++ Location: parse.cc:1671-1673 (uses LookupPosixGroup)
Java Location: RegexpParser.java:989-1033

#### Difference 50
C++:  `return LookupGroup(name, posix_groups, num_posix_groups);`
Java: Inline switch statement building CharClass for each POSIX group name
Note: C++ uses lookup table; Java builds inline

#### Difference 51
C++:  POSIX groups include `[:ascii:]`
Java: No `ascii` case in switch statement
Note: Java missing `[:ascii:]` POSIX class

---

### Method: parseCaptureName
C++ Location: parse.cc:2128-2158
Java Location: RegexpParser.java:673-705

#### Difference 52
C++:  `size_t end = t.find('>', begin);`
Java: `while (nameEnd < end && (bytes[nameEnd] & 0xFF) != endDelimiter) { nameEnd++; }`
Note: C++ uses find; Java uses manual loop

#### Difference 53
C++:  `if (!IsValidUTF8(name, status_)) return false;`
Java: `int r = readUtf8RuneInName(); if (!isValidCaptureNameRune(r)) { throw error(...); }`
Note: C++ validates UTF-8 first, then checks name validity; Java validates character-by-character

---

### Method: DoVerticalBar (alternation)
C++ Location: parse.cc:648-695
Java Location: RegexpParser.java:172-186 (parseAlt)

#### Difference 54
C++:  Has optimization to subsume AnyChar: `if (r3->op() == kRegexpAnyChar && (r1->op() == kRegexpLiteral || r1->op() == kRegexpCharClass || r1->op() == kRegexpAnyChar)) { stacktop_ = r2; r1->Decref(); return true; }`
Java: No equivalent AnyChar subsumption optimization
Note: C++ optimizes `.` alternations; Java does not

---

### Method: DoFinish / end of parsing
C++ Location: parse.cc:737-747
Java Location: RegexpParser.java:52-57

#### Difference 55
C++:  `if (re != NULL && re->down_ != NULL) { status_->set_code(kRegexpMissingParen); status_->set_error_arg(whole_regexp_); return NULL; }`
Java: `if (!p.atEnd()) { if (p.peekByte() == ')') { throw p.error(RegexpStatusCode.UNEXPECTED_PAREN, p.errorArgAll()); } throw p.error(RegexpStatusCode.INTERNAL_ERROR, p.errorArgAtCurrent()); }`
Note: C++ checks stack depth; Java checks remaining input

---

### Method: PushLiteral / literalFromRune
C++ Location: parse.cc:409-444
Java Location: RegexpParser.java:1393-1427

#### Difference 56
C++:  `if (MaybeConcatString(r, flags_)) return true;`
Java: Literal string concatenation done in parseConcat
Note: C++ tries to concat strings in PushLiteral; Java handles in parseConcat

#### Difference 57
C++:  `if ((flags_ & NeverNL) && r == '\n') return PushRegexp(new Regexp(kRegexpNoMatch, flags_));`
Java: `if (((flags & Regexp.NEVER_NL) != 0) && rune == '\n') { return Regexp.noMatch(flags); }`
Note: Same logic

---

### Method: MaybeConcatString
C++ Location: parse.cc:1309-1353
Java Location: RegexpParser.java:189-245 (in parseConcat)

#### Difference 58
C++:  `if ((re1->parse_flags_ & FoldCase) != (re2->parse_flags_ & FoldCase)) return false;`
Java: `if (literalFlags != null && literalFlags != atom.parseFlags()) { subs.add(makeLiteralString(literalFlags, literalRunes)); literalRunes.clear(); literalFlags = null; }`
Note: C++ compares FoldCase specifically; Java compares entire parseFlags

---

### Method: PushRegexp optimizations
C++ Location: parse.cc:235-265
Java Location: No direct equivalent (simplification done in Simplifier)

#### Difference 59
C++:  `if (re->op_ == kRegexpCharClass && re->ccb_ != NULL) { re->ccb_->RemoveAbove(rune_max_); if (re->ccb_->size() == 1) { Rune r = re->ccb_->begin()->lo; re->Decref(); re = new Regexp(kRegexpLiteral, flags_); re->rune_ = r; } else if (re->ccb_->size() == 2) { ... } }`
Java: Done in Simplifier.simplifyForParse()
Note: C++ does char class to literal optimization inline; Java defers to Simplifier

---

### Method: FactorAlternation
C++ Location: parse.cc:956-1050
Java Location: Not present in RegexpParser.java

#### Difference 60
C++:  `int Regexp::FactorAlternation(Regexp** sub, int nsub, ParseFlags flags)` - complex factoring algorithm
Java: No equivalent alternation factoring in parser
Note: C++ has prefix factoring optimization; Java does not implement this

---

### Method: \Q...\E handling
C++ Location: parse.cc:2471-2485
Java Location: RegexpParser.java:525-545 (tryConsumeQuoteDirective)

#### Difference 61
C++:  `if (t[1] == 'Q') { t.remove_prefix(2); while (!t.empty()) { if (t.size() >= 2 && t[0] == '\\' && t[1] == 'E') { t.remove_prefix(2); break; } Rune r; if (StringViewToRune(&r, &t, status) < 0) return NULL; if (!ps.PushLiteral(r)) return NULL; } break; }`
Java: `private boolean tryConsumeQuoteDirective() { if ((flags & Regexp.PERL_X) == 0) { return false; } if (peekByte() != '\\' || index + 1 >= end) { return false; } int next = bytes[index + 1] & 0xFF; if (!inQuote && next == 'Q') { index += 2; inQuote = true; return true; } if (inQuote && next == 'E') { index += 2; inQuote = false; return true; } return false; }`
Note: C++ handles inline; Java uses state flag `inQuote`

---

### Method: StringViewToRune / readRune
C++ Location: parse.cc:1424-1449
Java Location: RegexpParser.java:1453-1470

#### Difference 62
C++:  `if (*r > Runemax) { n = 1; *r = Runeerror; }`
Java: No explicit Runemax check; relies on Utf8.decode
Note: C++ explicitly checks for values > Runemax

#### Difference 63
C++:  `if (!(n == 1 && *r == Runeerror)) { sp->remove_prefix(n); return n; }`
Java: `if (width == 1 && cp == Utf8.RUNE_ERROR && (bytes[index] & 0xFF) >= 0x80) { throw error(RegexpStatusCode.BAD_UTF8, null); }`
Note: C++ returns -1 on error; Java throws exception

---

### Method: CharClassBuilder.AddRangeFlags
C++ Location: parse.cc:1634-1658
Java Location: CharClassBuilder.java (separate file)

#### Difference 64
C++:  `bool cutnl = !(parse_flags & Regexp::ClassNL) || (parse_flags & Regexp::NeverNL);`
Java: Implementation in CharClassBuilder.addRangeFlags
Note: Same logic should be in CharClassBuilder

---

### Method: isValidRepeatBrace
C++ Location: Not present as separate function - handled in MaybeParseRepetition
Java Location: RegexpParser.java:294-303

#### Difference 65
C++:  `if (!ParseInteger(&s, lo)) return false;`
Java: `if (index + 1 >= end) { return false; } int next = bytes[index + 1] & 0xFF; return next >= '0' && next <= '9';`
Note: Java has simplified check for `{` being repetition; C++ tries full parse

---

### Method: POSIX character classes inside []
C++ Location: parse.cc:1991-1999
Java Location: RegexpParser.java:791-794

#### Difference 66
C++:  `if (s->size() > 2 && (*s)[0] == '[' && (*s)[1] == ':')`
Java: `if (tryParsePosixCharClass(nodeFlags, builder))`
Note: C++ checks size > 2; Java method handles all checking

---

### Method: wouldExceedRepeatLimit / repetitionBudget
C++ Location: parse.cc:541-585 (RepetitionWalker class)
Java Location: RegexpParser.java:366-415

#### Difference 67
C++:  Uses Walker pattern with PreVisit/PostVisit
Java: Uses recursive `repetitionBudget` function
Note: Same algorithm, different implementation pattern

---

### Summary for RegexpParser.java vs parse.cc
- Methods compared: ~35 major functions/methods
- Total differences found: 67

Major architectural differences:
1. C++ uses stack-based parsing; Java uses recursive descent
2. C++ converts Latin1 to UTF-8 for processing; Java keeps bytes as-is
3. C++ has inline optimizations (squash repeat ops, subsume AnyChar); Java defers to Simplifier
4. C++ has alternation prefix factoring; Java does not
5. C++ disallows leading zeros in repeat counts; Java allows them
6. Java missing `[:ascii:]` POSIX class
7. Java pre-validates UTF-8; C++ validates during parsing
