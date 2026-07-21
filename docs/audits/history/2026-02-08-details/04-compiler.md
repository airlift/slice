I'll read both files to perform a comprehensive line-by-line audit.
Now I'll perform a comprehensive line-by-line audit comparing each method.

## File: Compiler.java vs compile.cc

### Method: PatchList.Mk / PatchList.mk
C++ Location: compile.cc:43-45
Java Location: Compiler.java:1029-1032

#### Difference 1
C++:  `return {p, p};`
Java: `return new PatchList(p, p);`
Note: Java uses explicit constructor call instead of initializer list.

---

### Method: PatchList.Patch / PatchList.patch
C++ Location: compile.cc:49-60
Java Location: Compiler.java:1034-1048

#### Difference 2
C++:  `Prog::Inst* ip = &inst0[l.head>>1];`
Java: `Prog.Inst ip = prog.inst(head >>> 1);`
Note: C++ uses direct array access with pointer; Java uses method call and unsigned right shift.

#### Difference 3
C++:  `if (l.head&1) {`
Java: `if ((head & 1) != 0) {`
Note: Java requires explicit comparison to 0.

#### Difference 4
C++:  `ip->out1_ = p;`
Java: `ip.setOut1(p);`
Note: C++ directly modifies field; Java uses setter method.

#### Difference 5
C++:  `ip->set_out(p);`
Java: `ip.setOut(p);`
Note: Slight naming convention difference.

---

### Method: PatchList.Append / PatchList.append
C++ Location: compile.cc:63-74
Java Location: Compiler.java:1050-1067

#### Difference 6
C++:  `Prog::Inst* ip = &inst0[l1.tail>>1];`
Java: `Prog.Inst ip = prog.inst(l1.tail >>> 1);`
Note: C++ uses direct array access; Java uses method call.

#### Difference 7
C++:  `if (l1.tail&1)`
Java: `if ((l1.tail & 1) != 0)`
Note: Java requires explicit comparison.

#### Difference 8
C++:  `ip->out1_ = l2.head;`
Java: `ip.setOut1(l2.head);`
Note: Direct field access vs setter method.

#### Difference 9
C++:  `return {l1.head, l2.tail};`
Java: `return new PatchList(l1.head, l2.tail);`
Note: Java uses explicit constructor.

---

### Method: Frag constructor
C++ Location: compile.cc:88-90
Java Location: Compiler.java:1076-1086

#### Difference 10
C++:  `Frag() : begin(0), end(kNullPatchList), nullable(false) {}`
Java: `Frag() { this(0, PatchList.NULL, false); }`
Note: Java chains constructors; C++ uses initializer list.

---

### Method: Compiler::Compiler() / CompilerImpl constructor
C++ Location: compile.cc:227-238
Java Location: Compiler.java:248-256

#### Difference 11
C++:  
```
prog_ = new Prog();
failed_ = false;
encoding_ = kEncodingUTF8;
reversed_ = false;
ninst_ = 0;
max_ninst_ = 1;  // make AllocInst for fail instruction okay
max_mem_ = 0;
int fail = AllocInst(1);
inst_[fail].InitFail();
max_ninst_ = 0;
```
Java: 
```
final Prog prog = new Prog();
boolean failed;
boolean reversed;
Encoding encoding = Encoding.UTF8;
int maxNinst;
```
Note: C++ explicitly allocates fail instruction in constructor. Java appears to handle this differently in Prog initialization.

---

### Method: AllocInst / add
C++ Location: compile.cc:244-265
Java Location: Compiler.java:361-368

#### Difference 12
C++:  `int Compiler::AllocInst(int n)`
Java: `private int add(Prog.Inst inst)`
Note: C++ allocates multiple instructions at once; Java adds a single instruction.

#### Difference 13
C++:  `if (failed_ || ninst_ + n > max_ninst_) {`
Java: `if (failed || prog.size() + 1 > maxNinst) {`
Note: C++ uses `ninst_` counter; Java uses `prog.size() + 1` for single instruction.

#### Difference 14
C++:  
```
if (ninst_ + n > inst_.size()) {
  int cap = inst_.size();
  if (cap == 0)
    cap = 8;
  while (ninst_ + n > cap)
    cap *= 2;
  PODArray<Prog::Inst> inst(cap);
  if (inst_.data() != NULL)
    memmove(inst.data(), inst_.data(), ninst_*sizeof inst_[0]);
  memset(inst.data() + ninst_, 0, (cap - ninst_)*sizeof inst_[0]);
  inst_ = std::move(inst);
}
```
Java: `return prog.add(inst);`
Note: C++ manually manages array growth; Java delegates to Prog.add().

---

### Method: NoMatch / noMatch
C++ Location: compile.cc:272-274
Java Location: Compiler.java:370-373

No significant differences. Both return empty/default Frag.

---

### Method: IsNoMatch (static) / isNoMatch
C++ Location: compile.cc:277-279
Java Location: Compiler.java:375-378

No significant differences.

---

### Method: Cat / cat
C++ Location: compile.cc:282-304
Java Location: Compiler.java:380-402

#### Difference 15
C++:  `Prog::Inst* begin = &inst_[a.begin];`
Java: `Prog.Inst begin = prog.inst(a.begin);`
Note: Array access vs method call.

#### Difference 16
C++:  `if (begin->opcode() == kInstNop &&`
Java: `if (begin.opcode() == io.airlift.slice.re2.InstOp.NOP &&`
Note: Different enum naming conventions.

#### Difference 17
C++:  `PatchList::Patch(inst_.data(), a.end, b.begin);`
Java: `PatchList.patch(prog, a.end, b.begin);`
Note: C++ passes inst_ array directly; Java passes prog object.

---

### Method: Alt / alt
C++ Location: compile.cc:307-321
Java Location: Compiler.java:404-419

#### Difference 18
C++:  `inst_[id].InitAlt(a.begin, b.begin);`
Java: `int id = add(Prog.Inst.createAlt(a.begin, b.begin));`
Note: C++ initializes instruction after allocation; Java creates instruction in add call.

#### Difference 19
C++:  `return Frag(id, PatchList::Append(inst_.data(), a.end, b.end), a.nullable || b.nullable);`
Java: `return new Frag(id, PatchList.append(prog, a.end, b.end), a.nullable || b.nullable);`
Note: Java uses `new` keyword.

---

### Method: Plus / plus
C++ Location: compile.cc:331-345
Java Location: Compiler.java:421-441

#### Difference 20
C++:  `int id = AllocInst(1);`
Java: `int id = add(Prog.Inst.createAlt(0, 0));`
Note: C++ allocates then initializes; Java creates instruction inline.

#### Difference 21
C++:  `inst_[id].InitAlt(0, a.begin);`
Java: `prog.inst(id).setOut(0); prog.inst(id).setOut1(a.begin);`
Note: C++ uses InitAlt; Java sets fields separately after creation.

#### Difference 22
C++:  `PatchList::Patch(inst_.data(), a.end, id);`
Java: `PatchList.patch(prog, a.end, id);`
Note: Different parameter type.

---

### Method: Star / star
C++ Location: compile.cc:348-368
Java Location: Compiler.java:443-467

No significant logical differences beyond what was noted in Plus.

---

### Method: Quest / quest
C++ Location: compile.cc:371-386
Java Location: Compiler.java:469-492

No significant logical differences.

---

### Method: ByteRange / byteRange
C++ Location: compile.cc:389-395
Java Location: Compiler.java:539-546

#### Difference 23
C++:  `inst_[id].InitByteRange(lo, hi, foldcase, 0);`
Java: `int id = add(Prog.Inst.createByteRange(lo, hi, foldcase, 0));`
Note: C++ allocates then initializes; Java creates inline.

---

### Method: Nop / nop
C++ Location: compile.cc:398-404
Java Location: Compiler.java:521-528

No significant logical differences.

---

### Method: Match / match
C++ Location: compile.cc:407-413
Java Location: Compiler.java:512-519

#### Difference 24
C++:  `Frag Compiler::Match(int32_t match_id)`
Java: `private Frag match(int matchId)`
Note: C++ uses int32_t explicit type.

---

### Method: EmptyWidth / emptyWidth
C++ Location: compile.cc:416-422
Java Location: Compiler.java:530-537

No significant logical differences.

---

### Method: Capture / capture
C++ Location: compile.cc:425-436
Java Location: Compiler.java:494-510

#### Difference 25
C++:  `int id = AllocInst(2);`
Java: `if (failed || prog.size() + 2 > maxNinst) { failed = true; return noMatch(); } int id = prog.add(Prog.Inst.createCapture(2 * n, a.begin));`
Note: C++ allocates 2 instructions at once; Java adds them separately with explicit size check.

#### Difference 26
C++:  `inst_[id].InitCapture(2*n, a.begin); inst_[id+1].InitCapture(2*n+1, 0);`
Java: `int id = prog.add(Prog.Inst.createCapture(2 * n, a.begin)); int id2 = prog.add(Prog.Inst.createCapture(2 * n + 1, 0));`
Note: C++ uses contiguous allocation; Java adds separately.

#### Difference 27
C++:  `PatchList::Patch(inst_.data(), a.end, id+1);`
Java: `PatchList.patch(prog, a.end, id2);`
Note: C++ uses `id+1`; Java uses separate `id2` variable.

#### Difference 28
C++:  `return Frag(id, PatchList::Mk((id+1) << 1), a.nullable);`
Java: `return new Frag(id, PatchList.mk(id2 << 1), a.nullable);`
Note: C++ uses `(id+1)`; Java uses `id2`.

---

### Method: MaxRune / maxRune
C++ Location: compile.cc:440-447
Java Location: Compiler.java:973-984

#### Difference 29
C++:  `b = 8-(len+1) + 6*(len-1);`
Java: `b = 8 - (len + 1) + 6 * (len - 1);`
Note: Just spacing difference, functionally identical.

---

### Method: BeginRange / beginRange
C++ Location: compile.cc:457-461
Java Location: Compiler.java:772-778

#### Difference 30
C++:  `rune_range_.begin = 0; rune_range_.end = kNullPatchList;`
Java: `runeRange.begin = 0; runeRange.end = PatchList.NULL; runeRange.nullable = false;`
Note: Java explicitly sets nullable to false; C++ doesn't (relies on prior state or default).

---

### Method: UncachedRuneByteSuffix / uncachedRuneByteSuffix
C++ Location: compile.cc:463-472
Java Location: Compiler.java:788-801

#### Difference 31
C++:  `int Compiler::UncachedRuneByteSuffix(uint8_t lo, uint8_t hi, bool foldcase, int next)`
Java: `private int uncachedRuneByteSuffix(int lo, int hi, boolean foldcase, int next)`
Note: C++ uses uint8_t; Java uses int (caller converts).

#### Difference 32
C++:  `if (next != 0) { PatchList::Patch(inst_.data(), f.end, next); } else { rune_range_.end = PatchList::Append(inst_.data(), rune_range_.end, f.end); }`
Java: `if (next != 0) { PatchList.patch(prog, f.end, next); } else { runeRange.end = PatchList.append(prog, runeRange.end, f.end); }`
Note: Slightly different parameter passing, but same logic.

#### Difference 33
C++:  `return f.begin;`
Java: `if (isNoMatch(f)) { return 0; } ... return f.begin;`
Note: Java has explicit null check at start; C++ doesn't check.

---

### Method: MakeRuneCacheKey / makeRuneCacheKey
C++ Location: compile.cc:474-480
Java Location: Compiler.java:803-810

No significant differences.

---

### Method: CachedRuneByteSuffix / cachedRuneByteSuffix
C++ Location: compile.cc:482-491
Java Location: Compiler.java:812-822

#### Difference 34
C++:  `absl::flat_hash_map<uint64_t, int>::const_iterator it = rune_cache_.find(key); if (it != rune_cache_.end()) return it->second;`
Java: `Integer existing = runeCache.get(key); if (existing != null) { return existing; }`
Note: Different map API, functionally equivalent.

---

### Method: IsCachedRuneByteSuffix / isCachedRuneByteSuffix
C++ Location: compile.cc:493-501
Java Location: Compiler.java:824-829

#### Difference 35
C++:  `uint8_t lo = inst_[id].lo_; uint8_t hi = inst_[id].hi_;`
Java: `Prog.Inst inst = prog.inst(id); ... inst.lo(), inst.hi()`
Note: C++ accesses fields directly; Java uses accessor methods.

#### Difference 36
C++:  `bool foldcase = inst_[id].foldcase() != 0;`
Java: `inst.foldcase()`
Note: C++ compares to 0; Java relies on boolean return type.

---

### Method: AddSuffix / addSuffix
C++ Location: compile.cc:503-525
Java Location: Compiler.java:831-861

#### Difference 37
C++:  `if (rune_range_.begin == 0) { rune_range_.begin = id; return; }`
Java: `if (id == 0) { failed = true; return; } if (runeRange.begin == 0) { runeRange.begin = id; return; }`
Note: Java has explicit check for id == 0 before the main logic.

#### Difference 38
C++:  `if (encoding_ == kEncodingUTF8) { rune_range_.begin = AddSuffixRecursive(rune_range_.begin, id); return; }`
Java: `if (encoding == Encoding.UTF8) { runeRange.begin = addSuffixRecursive(runeRange.begin, id); if (runeRange.begin == 0) { failed = true; } return; }`
Note: Java has explicit failure check after recursive call.

#### Difference 39
C++:  `int alt = AllocInst(1); if (alt < 0) { rune_range_.begin = 0; return; } inst_[alt].InitAlt(rune_range_.begin, id); rune_range_.begin = alt;`
Java: `int alt = add(Prog.Inst.createAlt(runeRange.begin, id)); if (alt < 0) { runeRange.begin = 0; failed = true; return; } runeRange.begin = alt;`
Note: Java sets `failed = true` explicitly.

---

### Method: AddSuffixRecursive / addSuffixRecursive
C++ Location: compile.cc:527-583
Java Location: Compiler.java:910-971

#### Difference 40
C++:  `ABSL_DCHECK(inst_[root].opcode() == kInstAlt || inst_[root].opcode() == kInstByteRange);`
Java: `Prog.Inst rootInst = prog.inst(root); if (rootInst.opcode() != io.airlift.slice.re2.InstOp.ALT && rootInst.opcode() != io.airlift.slice.re2.InstOp.BYTE_RANGE) { failed = true; return 0; }`
Note: C++ uses debug assertion; Java returns failure.

#### Difference 41
C++:  `inst_[alt].InitAlt(root, id);`
Java: `int alt = add(Prog.Inst.createAlt(root, id));`
Note: Different allocation/init pattern.

#### Difference 42
C++:  `if (f.end.head&1) br = inst_[f.begin].out1(); else br = inst_[f.begin].out();`
Java: `if ((f.end.head & 1) != 0) { br = prog.inst(f.begin).out1(); } else { br = prog.inst(f.begin).out(); }`
Note: Java explicit comparison to 0.

#### Difference 43
C++:  `int byterange = AllocInst(1); ... inst_[byterange].InitByteRange(inst_[br].lo(), inst_[br].hi(), inst_[br].foldcase(), inst_[br].out());`
Java: `int clone = add(Prog.Inst.createByteRange(head.lo(), head.hi(), head.foldcase(), head.out()));`
Note: Variable named `byterange` in C++, `clone` in Java.

#### Difference 44
C++:  `inst_[f.begin].out1_ = br;`
Java: `prog.inst(f.begin).setOut1(br);`
Note: Direct field access vs setter.

#### Difference 45
C++:  `ABSL_DCHECK_EQ(id, ninst_-1); inst_[id].out_opcode_ = 0; inst_[id].out1_ = 0; ninst_--;`
Java: `prog.removeLastInst(id);`
Note: C++ manually clears and decrements; Java delegates to method.

---

### Method: ByteRangeEqual / byteRangeEqual
C++ Location: compile.cc:585-589
Java Location: Compiler.java:863-868

No significant differences.

---

### Method: FindByteRange / findByteRange
C++ Location: compile.cc:591-621
Java Location: Compiler.java:870-908

#### Difference 46
C++:  `if (inst_[root].opcode() == kInstByteRange) { if (ByteRangeEqual(root, id)) return Frag(root, kNullPatchList, false); else return NoMatch(); }`
Java: `if (rootInst.opcode() == io.airlift.slice.re2.InstOp.BYTE_RANGE) { if (byteRangeEqual(root, id)) { return new Frag(root, PatchList.NULL, false); } return noMatch(); }`
Note: Java uses explicit `new Frag`.

#### Difference 47
C++:  `ABSL_LOG(DFATAL) << "should never happen";`
Java: `failed = true;`
Note: C++ logs fatal error; Java sets failure flag.

---

### Method: EndRange / endRange
C++ Location: compile.cc:623-625
Java Location: Compiler.java:780-786

#### Difference 48
C++:  `return rune_range_;`
Java: `return new Frag(runeRange.begin, runeRange.end, runeRange.nullable);`
Note: Java creates a copy; C++ returns directly. The Java comment explains this is to prevent mutation.

---

### Method: AddRuneRange / addRuneRange
C++ Location: compile.cc:633-643
Java Location: Compiler.java:626-633

#### Difference 49
C++:  `switch (encoding_) { default: case kEncodingUTF8: AddRuneRangeUTF8(lo, hi, foldcase); break; case kEncodingLatin1: AddRuneRangeLatin1(lo, hi, foldcase); break; }`
Java: `if (encoding == Encoding.LATIN1) { addRuneRangeLatin1(lo, hi, foldcase); return; } addRuneRangeUtf8(lo, hi, foldcase);`
Note: C++ uses switch with default to UTF8; Java uses if-else with LATIN1 check first.

---

### Method: AddRuneRangeLatin1 / addRuneRangeLatin1
C++ Location: compile.cc:645-653
Java Location: Compiler.java:635-645

#### Difference 50
C++:  `AddSuffix(UncachedRuneByteSuffix(static_cast<uint8_t>(lo), static_cast<uint8_t>(hi), foldcase, 0));`
Java: `addSuffix(uncachedRuneByteSuffix(lo, hi, foldcase, 0));`
Note: C++ has explicit cast to uint8_t; Java passes int directly.

---

### Method: Add_80_10ffff / add80To10ffff
C++ Location: compile.cc:655-693
Java Location: Compiler.java:734-769

No significant logical differences, just naming conventions.

---

### Method: AddRuneRangeUTF8 / addRuneRangeUtf8  **(CRITICAL FUNCTION)**
C++ Location: compile.cc:695-791
Java Location: Compiler.java:648-731

#### Difference 51
C++:  `if (lo == 0x80 && hi == 0x10ffff) {`
Java: `if (lo == 0x80 && hi == Regexp.RUNEMAX) {`
Note: C++ uses literal `0x10ffff`; Java uses constant `Regexp.RUNEMAX`.

#### Difference 52
C++:  `for (int i = 1; i < UTFmax; i++) {`
Java: `for (int i = 1; i < UTF_MAX; i++) {`
Note: Constant name difference.

#### Difference 53
C++:  `Rune max = MaxRune(i);`
Java: `int max = maxRune(i);`
Note: C++ uses `Rune` type; Java uses `int`.

#### Difference 54
C++:  `if (hi < Runeself) {`
Java: `if (hi < RUNES_SELF) {`
Note: Constant name difference.

#### Difference 55
C++:  `uint32_t m = (1<<(6*i)) - 1;`
Java: `int m = (1 << (6 * i)) - 1;`
Note: C++ uses uint32_t; Java uses int.

#### Difference 56
C++:  `uint8_t ulo[UTFmax], uhi[UTFmax];`
Java: `byte[] ulo = encodeUtf8(lo); byte[] uhi = encodeUtf8(hi);`
Note: C++ allocates arrays then calls runetochar; Java calls encodeUtf8 which returns allocated array.

#### Difference 57
C++:  `int n = runetochar(reinterpret_cast<char*>(ulo), &lo); int m = runetochar(reinterpret_cast<char*>(uhi), &hi); (void)m; ABSL_DCHECK_EQ(n, m);`
Java: `if (ulo.length != uhi.length) { failed = true; return; }`
Note: C++ uses runetochar and asserts equality; Java uses encodeUtf8 and checks length with failure.

#### Difference 58
C++:  `if (i == 0 || (ulo[i] == uhi[i] && i != n-1))`
Java: `if (i == 0 || (blo == bhi && i != ulo.length - 1))`
Note: Java uses `blo == bhi` with extracted byte values; C++ directly compares array elements.

#### Difference 59
C++:  `id = CachedRuneByteSuffix(ulo[i], uhi[i], false, id);`
Java: `int blo = ulo[i] & 0xFF; int bhi = uhi[i] & 0xFF; ... id = cachedRuneByteSuffix(blo, bhi, false, id);`
Note: Java masks bytes with 0xFF to ensure unsigned values; C++ uint8_t is inherently unsigned.

#### Difference 60
C++:  `for (int i = n-1; i >= 0; i--) {`
Java: `for (int i = ulo.length - 1; i >= 0; i--) {`
Note: C++ uses `n`; Java uses `ulo.length`.

---

### Method: Literal / literal
C++ Location: compile.cc:817-836
Java Location: Compiler.java:548-564

#### Difference 61
C++:  `switch (encoding_) { default: return Frag();`
Java: `return switch (encoding) { case LATIN1 -> ...`
Note: C++ has default case returning empty Frag; Java switch expression has no default (exhaustive enum).

#### Difference 62
C++:  `if (r < Runeself)`
Java: `if (rune < 0x80)`
Note: Different constant usage.

#### Difference 63
C++:  `uint8_t buf[UTFmax]; int n = runetochar(reinterpret_cast<char*>(buf), &r);`
Java: `byte[] utf8 = encodeUtf8(rune);`
Note: Different encoding approach.

#### Difference 64
C++:  `Frag f = ByteRange((uint8_t)buf[0], buf[0], false);`
Java: `Frag f = byteRange(utf8[0] & 0xFF, utf8[0] & 0xFF, false);`
Note: Java masks with 0xFF for unsigned conversion.

---

### Method: PostVisit / postVisit
C++ Location: compile.cc:840-984
Java Location: Compiler.java:291-335

#### Difference 65
C++:  `case kRegexpRepeat: break;` (falls through to error)
Java: `case REPEAT -> { failed = true; yield noMatch(); }`
Note: Java handles REPEAT explicitly with failure.

#### Difference 66
C++:  
```
case kRegexpHaveMatch: {
  Frag f = Match(re->match_id());
  if (anchor_ == RE2::ANCHOR_BOTH) {
    f = Cat(EmptyWidth(kEmptyEndText), f);
  }
  return f;
}
```
Java: `case HAVE_MATCH -> match(re.matchId());`
Note: C++ has special handling for ANCHOR_BOTH anchor mode; Java does not.

#### Difference 67
C++:  `case kRegexpAnyChar: BeginRange(); AddRuneRange(0, Runemax, false); return EndRange();`
Java: 
```
case ANY_CHAR -> anyChar(re.parseFlags());
```
Note: Java delegates to anyChar method; C++ does inline.

#### Difference 68
C++:  
```
case kRegexpCharClass: {
  CharClass* cc = re->cc();
  if (cc->empty()) {
    failed_ = true;
    ABSL_LOG(DFATAL) << "No ranges in char class";
    return NoMatch();
  }
  ...
}
```
Java: 
```
case CHAR_CLASS -> charClass(re.charClass());
```
Note: Java delegates to charClass method.

#### Difference 69
C++:  
```
case kRegexpCapture:
  if (re->cap() < 0)
    return child_frags[0];
  return Capture(child_frags[0], re->cap());
```
Java: `case CAPTURE -> capture(childArgs.getFirst(), re.cap());`
Note: C++ has special handling for non-capturing groups (cap < 0); Java doesn't check.

#### Difference 70
C++:  `failed_ = true; ABSL_LOG(DFATAL) << "Missing case in Compiler: " << re->op();`
Java: (No default case in switch expression - exhaustive enum matching)
Note: C++ has error handling for missing cases; Java relies on exhaustive enum.

---

### Method: anyChar (Java only, extracted from C++ PostVisit)
C++ Location: compile.cc:911-914 (inline in PostVisit)
Java Location: Compiler.java:583-596

#### Difference 71
C++:  `BeginRange(); AddRuneRange(0, Runemax, false); return EndRange();`
Java:
```
if (((flags & Regexp.DOT_NL) != 0) && ((flags & Regexp.NEVER_NL) == 0)) {
    beginRange();
    addRuneRange(0, Regexp.RUNEMAX, false);
    return endRange();
}
CharClassBuilder cc = new CharClassBuilder();
cc.addRange('\n', '\n');
cc.negate();
cc.removeAbove(Regexp.maxRune(flags));
return charClass(cc.toCharClass());
```
Note: Java has special handling for DOT_NL and NEVER_NL flags that C++ does not have in this location. The C++ equivalent is in regexp.cc during parsing.

---

### Method: charClass (Java only, extracted from C++ PostVisit)
C++ Location: compile.cc:919-953 (inline in PostVisit)
Java Location: Compiler.java:598-624

No significant logical differences from the inline C++ code.

---

### Method: IsAnchorStart / stripAnchorStart
C++ Location: compile.cc:989-1031
Java Location: Compiler.java:170-202

#### Difference 72
C++:  `static bool IsAnchorStart(Regexp** pre, int depth)`
Java: `private static AnchorStripResult stripAnchorStart(Regexp re, int depth)`
Note: C++ modifies pointer; Java returns record with new regexp.

#### Difference 73
C++:  `if (re == NULL || depth >= 4)`
Java: `if (re == null || depth >= 4)`
Note: NULL vs null.

#### Difference 74
C++:  `sub = re->sub()[0]->Incref();`
Java: (Java doesn't use reference counting)
Note: C++ manages reference counts; Java relies on GC.

#### Difference 75
C++:  `*pre = Regexp::Concat(subcopy.data(), re->nsub(), re->parse_flags()); re->Decref();`
Java: `yield new AnchorStripResult(true, Regexp.concat(re.parseFlags(), subs));`
Note: C++ manual memory management; Java uses records.

#### Difference 76
C++:  `*pre = Regexp::LiteralString(NULL, 0, re->parse_flags());`
Java: `new AnchorStripResult(true, Regexp.literalString(re.parseFlags(), new int[0]))`
Note: C++ passes NULL; Java passes empty array.

---

### Method: IsAnchorEnd / stripAnchorEnd
C++ Location: compile.cc:1036-1078
Java Location: Compiler.java:204-237

Same differences as IsAnchorStart/stripAnchorStart.

---

### Method: Setup / setup
C++ Location: compile.cc:1080-1108
Java Location: Compiler.java:259-279

#### Difference 77
C++:  `if (flags & Regexp::Latin1)`
Java: `if ((flags & Regexp.LATIN1) != 0)`
Note: Java explicit comparison.

#### Difference 78
C++:  `if (max_mem <= 0) { max_ninst_ = 100000; }`
Java: `if (maxMem <= 0) { maxNinst = 100_000; return; }`
Note: Java has early return; C++ continues.

#### Difference 79
C++:  `if (static_cast<size_t>(max_mem) <= sizeof(Prog))`
Java: `if (maxMem <= CPP_PROG_OVERHEAD_BYTES)`
Note: C++ uses sizeof(Prog); Java uses constant CPP_PROG_OVERHEAD_BYTES (512).

#### Difference 80
C++:  `int64_t m = (max_mem - sizeof(Prog)) / sizeof(Prog::Inst);`
Java: `long m = (maxMem - CPP_PROG_OVERHEAD_BYTES) / CPP_INST_BYTES;`
Note: C++ uses sizeof; Java uses constants.

#### Difference 81
C++:  `if (m >= 1<<24) m = 1<<24;`
Java: `if (m >= MAX_NINST) { m = MAX_NINST; }`
Note: Java uses constant MAX_NINST.

#### Difference 82
C++:  `if (m > Prog::Inst::kMaxInst) m = Prog::Inst::kMaxInst;`
Java: (No equivalent check)
Note: C++ has additional limit from Inst; Java doesn't have this check.

#### Difference 83
C++:  `anchor_ = anchor;`
Java: (No anchor field)
Note: C++ stores anchor mode; Java doesn't have this field.

---

### Method: Compile (static) / compile (static)
C++ Location: compile.cc:1115-1161
Java Location: Compiler.java:39-163

#### Difference 84
C++:  `c.Setup(re->parse_flags(), max_mem, RE2::UNANCHORED /* unused */);`
Java: `c.setup(re.parseFlags(), maxMem);`
Note: C++ passes anchor parameter (unused); Java doesn't.

#### Difference 85
C++:  `Regexp* sre = re->Simplify(); if (sre == NULL) return NULL;`
Java: `Regexp sre = Simplifier.simplify(re);`
Note: C++ checks for null; Java doesn't (simplify doesn't return null).

#### Difference 86
C++:  `sre->Decref();`
Java: (No equivalent)
Note: C++ manual memory management.

---

### Method: Finish / finish
C++ Location: compile.cc:1163-1203
Java Location: Compiler.java:344-359

#### Difference 87
C++:  
```
if (prog_->start() == 0 && prog_->start_unanchored() == 0) {
  ninst_ = 1;
}
```
Java: 
```
if (prog.start() == 0 && prog.startUnanchored() == 0) {
  return new Prog();
}
```
Note: C++ keeps fail instruction; Java returns new empty Prog.

#### Difference 88
C++:  
```
prog_->inst_ = std::move(inst_);
prog_->size_ = ninst_;
```
Java: (No equivalent - Java adds instructions directly to prog)
Note: C++ moves array to prog; Java uses different architecture.

#### Difference 89
C++:  
```
if (!prog_->reversed()) {
  std::string prefix;
  bool prefix_foldcase;
  if (re->RequiredPrefixForAccel(&prefix, &prefix_foldcase))
    prog_->ConfigurePrefixAccel(prefix, prefix_foldcase);
}
```
Java: (This is done in compile method, not finish)
Note: Different organization of prefix acceleration setup.

#### Difference 90
C++:  
```
if (max_mem_ <= 0) {
  prog_->set_dfa_mem(1<<20);
} else {
  int64_t m = max_mem_ - sizeof(Prog);
  m -= prog_->size_*sizeof(Prog::Inst);
  if (prog_->CanBitState())
    m -= prog_->size_*sizeof(uint16_t);
  if (m < 0)
    m = 0;
  prog_->set_dfa_mem(m);
}
```
Java: (No equivalent)
Note: C++ calculates DFA memory budget; Java doesn't have this.

---

### Method: DotStar / dotStar
C++ Location: compile.cc:1214-1216
Java Location: Compiler.java:578-581

No significant differences.

---

### Method: Copy (C++ only)
C++ Location: compile.cc:794-799
Java Location: N/A

Note: C++ has Copy method that should not be called (sets failed_ and logs error). Java Walker implementation handles this differently - doesn't have Copy method.

---

### Method: ShortVisit / shortVisit
C++ Location: compile.cc:803-806
Java Location: Compiler.java:337-342

No significant differences.

---

### Method: PreVisit / preVisit
C++ Location: compile.cc:809-815
Java Location: Compiler.java:281-288

#### Difference 91
C++:  `Frag Compiler::PreVisit(Regexp* re, Frag, bool* stop)`
Java: `protected PreVisitResult<Frag> preVisit(Regexp re, Frag parentArg)`
Note: C++ uses out parameter for stop; Java returns PreVisitResult record.

#### Difference 92
C++:  `if (failed_) *stop = true;`
Java: `if (failed) { return new PreVisitResult<>(parentArg, true); }`
Note: Different mechanism for signaling stop.

---

### Method: literalString (Java only)
C++ Location: compile.cc:895-909 (inline in PostVisit)
Java Location: Compiler.java:566-576

#### Difference 93
C++:  
```
if (re->nrunes() == 0)
  return Nop();
Frag f;
for (int i = 0; i < re->nrunes(); i++) {
  Frag f1 = Literal(re->runes()[i], (re->parse_flags()&Regexp::FoldCase) != 0);
  if (i == 0)
    f = f1;
  else
    f = Cat(f, f1);
}
return f;
```
Java:
```
if (runes.length == 0) {
    return nop();
}
Frag f = literal(runes[0], foldcase);
for (int i = 1; i < runes.length; i++) {
    f = cat(f, literal(runes[i], foldcase));
}
return f;
```
Note: Java starts loop at 1 and initializes f outside; C++ checks i == 0 inside loop.

---

### Method: CompileSet (C++ only)
C++ Location: compile.cc:1219-1263
Java Location: N/A

Note: C++ has CompileSet for RE2::Set; Java doesn't implement this method.

---

### Method: encodeUtf8 (Java only)
C++ Location: N/A (uses runetochar from util/utf.h)
Java Location: Compiler.java:986-1013

Note: Java implements UTF-8 encoding inline; C++ uses external runetochar function.

---

### Summary for Compiler.java vs compile.cc
- Methods compared: 42
- Total differences found: 93
