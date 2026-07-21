I'll read all three files to perform a comprehensive line-by-line audit.
Let me read the full Java file since it was truncated.
Let me continue reading the Java file to see the remaining methods.
Now I have all the files. Let me perform a detailed line-by-line comparison for each critical method.

## File: Prog.java vs prog.cc

### Method: Prog Constructor
C++ Location: prog.cc:114-131
Java Location: Prog.java:355-359

#### Difference 1
C++:  `Prog::Prog() : anchor_start_(false), anchor_end_(false), reversed_(false), did_flatten_(false), did_onepass_(false), start_(0), start_unanchored_(0), size_(0), bytemap_range_(0), prefix_foldcase_(false), prefix_size_(0), list_count_(0), bit_state_text_max_size_(0), dfa_mem_(0), dfa_first_(NULL), dfa_longest_(NULL) {}`
Java: `public Prog() { add(Inst.createFail()); }`
Note: Java constructor adds a FAIL instruction at position 0 explicitly. C++ initializes all member variables to default values but does not add a FAIL instruction in constructor (size_ starts at 0).

---

### Method: optimize (C++: Optimize)
C++ Location: prog.cc:230-291
Java Location: Prog.java:1637-1693

#### Difference 1
C++:  `Workq q(size_);`
Java: `SparseSet q = new SparseSet(size() + 1);`
Note: Java adds +1 to the size when creating the SparseSet.

#### Difference 2
C++:  `Inst* jp; while (j != 0 && (jp=inst(j))->opcode() == kInstNop) { j = jp->out(); }`
Java: `while (j != 0 && inst(j).opcode() == InstOp.NOP) { j = inst(j).out(); }`
Note: C++ captures jp in the loop condition and reuses it; Java calls inst(j) twice per iteration.

#### Difference 3
C++:  `ip->set_out(j);`
Java: `ip.setOut(j);`
Note: Just naming convention difference (snake_case vs camelCase).

#### Difference 4
C++:  `ip->out1_ = j;` (direct field assignment)
Java: `ip.setOut1(j);` (setter method call)
Note: C++ directly assigns to out1_ field, Java uses setter.

---

### Method: isMatch (C++: IsMatch)
C++ Location: prog.cc:204-227
Java Location: Prog.java:2088-2101

#### Difference 1
C++:  `static bool IsMatch(Prog* prog, Prog::Inst* ip)`
Java: `private boolean isMatch(Inst ip)`
Note: C++ is a free static function taking Prog* parameter; Java is an instance method.

#### Difference 2
C++:  `default: ABSL_LOG(DFATAL) << "Unexpected opcode in IsMatch: " << ip->opcode(); return false;`
Java: (no default case)
Note: C++ has explicit default case with logging for unexpected opcodes; Java relies on exhaustive switch.

---

### Method: emptyFlags (C++: EmptyFlags)
C++ Location: prog.cc:293-325
Java Location: Prog.java:2028-2086

#### Difference 1
C++:  `uint32_t Prog::EmptyFlags(absl::string_view text, const char* p)`
Java: `public static int emptyFlags(ByteSlice text, int p)`
Note: C++ takes a pointer p into text; Java takes an integer offset p.

#### Difference 2
C++:  `if (p == text.data())`
Java: `if (p == begin)` where `int begin = text.byteArrayOffset();`
Note: Same logic, different representation.

#### Difference 3
C++:  `else if (p[-1] == '\n')`
Java: `else if (bytes[p - 1] == '\n')`
Note: C++ uses pointer arithmetic; Java uses array indexing with offset.

#### Difference 4
C++:  (No range checking)
Java: `if (p < begin || p > end) { throw new IllegalArgumentException("p out of range..."); }`
Note: Java adds explicit range validation that C++ does not have.

#### Difference 5
C++:  `if (IsWordChar(p[0]))` and `if (IsWordChar(p[-1]))`
Java: `if (isWordChar(bytes[p] & 0xFF))` and `if (isWordChar(bytes[p - 1] & 0xFF))`
Note: Java applies `& 0xFF` to convert signed byte to unsigned int. C++ IsWordChar takes uint8_t.

---

### Method: computeByteMap (C++: ComputeByteMap)
C++ Location: prog.cc:452-526
Java Location: Prog.java:1040-1114

#### Difference 1
C++:  `for (int id = 0; id < size(); id++)`
Java: `for (int id = 0; id < size(); id++)`
Note: Identical loop structure.

#### Difference 2
C++:  `if (!ip->last() && inst(id+1)->opcode() == kInstByteRange && ip->out() == inst(id+1)->out())`
Java: `if (!ip.last() && id + 1 < size() && inst(id + 1).opcode() == InstOp.BYTE_RANGE && ip.out() == inst(id + 1).out())`
Note: Java adds bounds check `id + 1 < size()` that C++ does not have.

#### Difference 3
C++:  `if (ip->empty() & (kEmptyBeginLine|kEmptyEndLine) && !marked_line_boundaries)`
Java: `if ((empty & (EmptyOp.EMPTY_BEGIN_LINE | EmptyOp.EMPTY_END_LINE)) != 0 && !markedLineBoundaries)`
Note: Java uses explicit `!= 0` comparison; C++ relies on implicit truthiness.

#### Difference 4
C++:  `Prog::IsWordChar(static_cast<uint8_t>(i))`
Java: `isWordChar(i)`
Note: C++ casts to uint8_t; Java does not cast (isWordChar masks with 0xFF internally).

#### Difference 5
C++:  `builder.Build(bytemap_, &bytemap_range_);`
Java: `int[] outRange = new int[1]; builder.build(bytemap, outRange); bytemapRange = outRange[0];`
Note: C++ passes pointer to member variable; Java uses output array pattern.

#### Difference 6
C++:  `if ((0)) { ABSL_LOG(ERROR) << "Using trivial bytemap."; for (int i = 0; i < 256; i++) bytemap_[i] = static_cast<uint8_t>(i); bytemap_range_ = 256; }`
Java: (not present)
Note: C++ has debug code block (disabled with `if ((0))`); Java omits this entirely.

---

### Method: flatten (C++: Flatten)
C++ Location: prog.cc:563-658
Java Location: Prog.java:1695-1780

#### Difference 1
C++:  `SparseSet reachable(size());`
Java: `SparseSet reachable = new SparseSet(size());`
Note: C++ stack allocation; Java heap allocation.

#### Difference 2
C++:  `std::vector<int> stk; stk.reserve(size());`
Java: `IntStack stk = new IntStack(size());`
Note: C++ uses std::vector; Java uses custom IntStack class.

#### Difference 3
C++:  `SparseArray<int> rootmap(size());`
Java: `SparseIntArray rootmap = new SparseIntArray(size());`
Note: Different class names for sparse array implementation.

#### Difference 4
C++:  `SparseArray<int> sorted(rootmap);`
Java: `SparseIntArray sorted = new SparseIntArray(rootmap);`
Note: C++ copy constructor; Java copy constructor.

#### Difference 5
C++:  `std::sort(sorted.begin(), sorted.end(), sorted.less);`
Java: `sorted.sortByIndex();`
Note: C++ uses std::sort with custom comparator; Java has dedicated method.

#### Difference 6
C++:  `for (SparseArray<int>::const_iterator i = sorted.end() - 1; i != sorted.begin(); --i)`
Java: `for (int pos = sorted.size() - 1; pos > 0; pos--)`
Note: C++ uses reverse iterator; Java uses integer index.

#### Difference 7
C++:  `if (i->index() != start_unanchored() && i->index() != start())`
Java: `if (id != startUnanchored() && id != start())`
Note: Same logic, different accessor style.

#### Difference 8
C++:  `std::vector<int> flatmap(rootmap.size());`
Java: `int[] flatmap = new int[rootmap.size()];`
Note: C++ uses std::vector; Java uses primitive array.

#### Difference 9
C++:  `flatmap[i->value()] = static_cast<int>(flat.size());`
Java: `flatmap[rootId] = flat.size();`
Note: C++ gets value from iterator; Java pre-extracts rootId.

#### Difference 10
C++:  `flat.back().set_last();`
Java: `flat.get(flat.size() - 1).setLast();`
Note: C++ uses back() method; Java uses get(size-1).

#### Difference 11
C++:  `ComputeHints(&flat, flatmap[i->value()], static_cast<int>(flat.size()));`
Java: `computeHints(flat, flatmap[rootId], flat.size());`
Note: Same logic, different syntax.

#### Difference 12
C++:  `list_count_ = static_cast<int>(flatmap.size());`
Java: `listCount = rootmap.size();`
Note: C++ uses flatmap.size(); Java uses rootmap.size(). These should be equal but the source is different.

#### Difference 13
C++:  `for (int i = 0; i < kNumInst; i++) inst_count_[i] = 0;`
Java: (not present)
Note: C++ initializes instruction counts array; Java does not have inst_count_ tracking.

#### Difference 14
C++:  `inst_count_[ip->opcode()]++;`
Java: (not present)
Note: C++ counts instructions by opcode in fourth pass; Java omits this.

#### Difference 15
C++:  `#if !defined(NDEBUG) ... ABSL_CHECK_EQ(total, flat.size()); #endif`
Java: (not present)
Note: C++ has debug assertion checking total instruction count; Java omits this.

#### Difference 16
C++:  `size_ = static_cast<int>(flat.size()); inst_ = PODArray<Inst>(size_); memmove(inst_.data(), flat.data(), size_*sizeof inst_[0]);`
Java: `insts.clear(); insts.addAll(flat);`
Note: C++ does low-level memory copy to PODArray; Java clears and adds to ArrayList.

#### Difference 17
C++:  `if (size_ <= 512) { list_heads_ = PODArray<uint16_t>(size_); memset(list_heads_.data(), 0xFF, size_*sizeof list_heads_[0]); for (int i = 0; i < list_count_; ++i) list_heads_[flatmap[i]] = i; }`
Java: `if (insts.size() <= 512) { short[] heads = new short[insts.size()]; java.util.Arrays.fill(heads, (short) -1); for (int i = 0; i < listCount; i++) { int headPc = flatmap[i]; if (headPc >= 0 && headPc < heads.length) { heads[headPc] = (short) i; } } listHeads = heads; }`
Note: Java adds bounds check `if (headPc >= 0 && headPc < heads.length)` that C++ does not have.

#### Difference 18
C++:  `const size_t kBitStateBitmapMaxSize = 256*1024;`
Java: `long maxBits = 256L * 1024L;`
Note: C++ uses size_t constant; Java uses long literal.

#### Difference 19
C++:  `bit_state_text_max_size_ = kBitStateBitmapMaxSize / list_count_ - 1;`
Java: `bitStateTextMaxSize = (int) (maxBits / Math.max(1, listCount) - 1);`
Note: Java adds `Math.max(1, listCount)` to prevent division by zero.

---

### Method: markSuccessors (C++: MarkSuccessors)
C++ Location: prog.cc:660-722
Java Location: Prog.java:2103-2161

#### Difference 1
C++:  `void Prog::MarkSuccessors(SparseArray<int>* rootmap, SparseArray<int>* predmap, std::vector<std::vector<int>>* predvec, SparseSet* reachable, std::vector<int>* stk)`
Java: `private void markSuccessors(SparseIntArray rootmap, SparseIntArray predmap, List<IntList> predvec, SparseSet reachable, IntStack stk)`
Note: C++ uses pointers; Java uses references (no explicit pointers).

#### Difference 2
C++:  `rootmap->set_new(0, rootmap->size());`
Java: `rootmap.setNew(0, rootmap.size());`
Note: Pointer dereference vs method call; same semantics.

#### Difference 3
C++:  `stk->push_back(start_unanchored());`
Java: `stk.push(startUnanchored());`
Note: C++ push_back vs Java push; same semantics.

#### Difference 4
C++:  `while (!stk->empty()) { int id = stk->back(); stk->pop_back(); Loop: ... goto Loop; }`
Java: `while (!stk.isEmpty()) { int id = stk.pop(); while (true) { ... break; } }`
Note: C++ uses goto Loop pattern; Java uses nested while(true) with break.

#### Difference 5
C++:  `default: ABSL_LOG(DFATAL) << "unhandled opcode: " << ip->opcode(); break;`
Java: (no default case)
Note: C++ has explicit default case with logging; Java relies on exhaustive switch.

#### Difference 6
C++:  `for (int out : {ip->out(), ip->out1()})`
Java: `for (int out : new int[] {ip.out(), ip.out1()})`
Note: C++ uses initializer list; Java creates array explicitly.

#### Difference 7
C++:  `predvec->emplace_back();`
Java: `predvec.add(new IntList());`
Note: C++ emplace_back creates default object; Java explicitly creates new IntList.

#### Difference 8
C++:  `(*predvec)[predmap->get_existing(out)].emplace_back(id);`
Java: `predvec.get(predmap.getExisting(out)).add(id);`
Note: C++ uses operator[] and emplace_back; Java uses get() and add().

#### Difference 9
C++:  `id = ip->out(); goto Loop;` (for BYTE_RANGE, CAPTURE, EMPTY_WIDTH)
Java: `id = ip.out();` (continues in while(true) loop)
Note: Different loop control structure but same semantics.

#### Difference 10
C++:  `case kInstMatch: case kInstFail: break;`
Java: `case MATCH, FAIL -> { id = 0; }`
Note: C++ just breaks (relies on not setting id, will exit via reachable check); Java explicitly sets id = 0 then checks `if (id == 0) break;`.

---

### Method: markDominator (C++: MarkDominator)
C++ Location: prog.cc:724-786
Java Location: Prog.java:2164-2219

#### Difference 1
C++:  `Loop: if (reachable->contains(id)) continue;`
Java: `while (true) { if (reachable.contains(id)) { break; } ... }`
Note: C++ uses goto Loop with continue; Java uses while(true) with break.

#### Difference 2
C++:  `default: ABSL_LOG(DFATAL) << "unhandled opcode: " << ip->opcode(); break;`
Java: (no default case)
Note: C++ has explicit default case with logging; Java relies on exhaustive switch.

#### Difference 3
C++:  `case kInstByteRange: case kInstCapture: case kInstEmptyWidth: break;`
Java: `case BYTE_RANGE, CAPTURE, EMPTY_WIDTH, MATCH, FAIL -> { id = 0; }`
Note: C++ breaks and falls through to next stk pop; Java sets id=0 and checks at end of loop.

#### Difference 4
C++:  `for (SparseSet::const_iterator i = reachable->begin(); i != reachable->end(); ++i) { int id = *i; ... }`
Java: `for (int it = 0; it < reachable.size(); it++) { int id = reachable.denseAt(it); ... }`
Note: C++ uses iterator; Java uses index-based access.

#### Difference 5
C++:  `for (int pred : (*predvec)[predmap->get_existing(id)])`
Java: `IntList preds = predvec.get(predmap.getExisting(id)); for (int p = 0; p < preds.size(); p++) { int pred = preds.get(p); ... }`
Note: C++ uses range-for; Java uses index-based loop.

---

### Method: emitList (C++: EmitList)
C++ Location: prog.cc:788-848
Java Location: Prog.java:2222-2281

#### Difference 1
C++:  `flat->emplace_back(); flat->back().set_opcode(kInstNop); flat->back().set_out(rootmap->get_existing(id));`
Java: `Inst nop = new Inst(InstOp.NOP); nop.setOut(rootmap.getExisting(id)); flat.add(nop);`
Note: C++ uses emplace_back then modifies; Java creates object then adds.

#### Difference 2
C++:  `flat->back().set_opcode(kInstAltMatch);` (line 819)
Java: `Inst altMatch = new Inst(InstOp.ALT_MATCH);` (constructor sets opcode)
Note: C++ sets opcode after emplacing; Java sets in constructor.

#### Difference 3
C++:  `flat->back().set_out(static_cast<int>(flat->size()));`
Java: `int next = flat.size(); altMatch.setOut(next);`
Note: C++ casts size() to int inline; Java stores in variable.

#### Difference 4
C++:  `flat->back().out1_ = static_cast<uint32_t>(flat->size())+1;`
Java: `altMatch.setOut1(next + 1);`
Note: C++ directly assigns out1_; Java uses setter.

#### Difference 5
C++:  `[[fallthrough]];` (after ALT_MATCH case)
Java: (explicit handling of both cases in ALT_MATCH case, then falls through to ALT-like behavior)
Note: C++ uses fallthrough attribute; Java explicitly duplicates the stk.push/id= logic in ALT_MATCH case.

#### Difference 6
C++:  `memmove(&flat->back(), ip, sizeof *ip);`
Java: `Inst copy = ip.copy();`
Note: C++ uses memmove to copy instruction data; Java uses explicit copy method.

---

### Method: computeHints (C++: ComputeHints)
C++ Location: prog.cc:858-933
Java Location: Prog.java:2283-2357

#### Difference 1
C++:  `void Prog::ComputeHints(std::vector<Inst>* flat, int begin, int end)`
Java: `private static void computeHints(List<Inst> flat, int begin, int end)`
Note: C++ is instance method taking pointer; Java is static method taking List.

#### Difference 2
C++:  `Bitmap256 splits; int colors[256];`
Java: `Bitmap256 splits = new Bitmap256(); int[] colors = new int[256];`
Note: C++ stack-allocates; Java heap-allocates.

#### Difference 3
C++:  `for (int id = end; id >= begin; --id)`
Java: `for (int id = end; id >= begin; id--)`
Note: Identical semantics, just pre- vs post-decrement style.

#### Difference 4
C++:  `if (id == end || (*flat)[id].opcode() != kInstByteRange)`
Java: `if (id == end || flat.get(id).opcode() != InstOp.BYTE_RANGE)`
Note: C++ uses operator[]; Java uses get().

#### Difference 5
C++:  `splits.Clear();`
Java: `splits.clear();`
Note: Method naming convention difference (Clear vs clear).

#### Difference 6
C++:  `auto Recolor = [&](int lo, int hi) { ... };`
Java: `first = recolorHints(splits, colors, id, lo, hi, first);`
Note: C++ uses inline lambda; Java extracts to separate method.

#### Difference 7
C++:  `first = std::min(first, colors[next]);`
Java: `first = Math.min(first, colors[next]);`
Note: C++ uses std::min; Java uses Math.min.

#### Difference 8
C++:  `uint16_t hint = static_cast<uint16_t>(std::min(first - id, 32767)); ip->hint_foldcase_ |= hint<<1;`
Java: `int hint = Math.min(first - id, 32767); ip.hintFoldcase |= hint << 1;`
Note: C++ casts to uint16_t; Java keeps as int. Both shift left by 1.

---

### Class: ByteMapBuilder
C++ Location: prog.cc:343-450
Java Location: Prog.java:2359-2465

#### Difference 1
C++:  `ByteMapBuilder() { splits_.Set(255); colors_[255] = 256; nextcolor_ = 257; }`
Java: `ByteMapBuilder() { splits.set(255); colors[255] = 256; }`
Note: Java initializes nextColor=257 at field declaration level.

#### Difference 2
C++:  `void Mark(int lo, int hi) { ABSL_DCHECK_GE(lo, 0); ABSL_DCHECK_GE(hi, 0); ABSL_DCHECK_LE(lo, 255); ABSL_DCHECK_LE(hi, 255); ABSL_DCHECK_LE(lo, hi); ... }`
Java: `void mark(int lo, int hi) { if (lo < 0 || lo > 255 || hi < 0 || hi > 255 || lo > hi) { throw new IllegalArgumentException(...); } ... }`
Note: C++ uses debug checks (DCHECK); Java throws exception for validation.

#### Difference 3
C++:  `ranges_.emplace_back(lo, hi);`
Java: `ranges.add(new IntPair(lo, hi));`
Note: C++ uses emplace_back with pair; Java creates IntPair record explicitly.

#### Difference 4
C++:  `for (std::vector<std::pair<int, int>>::const_iterator it = ranges_.begin(); it != ranges_.end(); ++it) { int lo = it->first-1; int hi = it->second; ... }`
Java: `for (IntPair range : ranges) { int lo = range.a() - 1; int hi = range.b(); ... }`
Note: C++ uses iterator with first/second; Java uses range-for with record accessors.

#### Difference 5
C++:  `void Build(uint8_t* bytemap, int* bytemap_range)`
Java: `void build(byte[] bytemap, int[] bytemapRangeOut)`
Note: C++ takes pointers; Java takes arrays. C++ bytemap is uint8_t*; Java is byte[].

#### Difference 6
C++:  `uint8_t b = static_cast<uint8_t>(Recolor(colors_[next]));`
Java: `int b = recolor(colors[next]);`
Note: C++ casts to uint8_t; Java keeps as int.

#### Difference 7
C++:  `bytemap[c] = b;`
Java: `bytemap[c] = (byte) b;`
Note: C++ assigns uint8_t directly; Java casts int to byte.

#### Difference 8
C++:  `*bytemap_range = nextcolor_;`
Java: `bytemapRangeOut[0] = nextColor;`
Note: C++ dereferences pointer; Java assigns to array element.

#### Difference 9
C++:  `std::vector<std::pair<int, int>>::const_iterator it = std::find_if(colormap_.begin(), colormap_.end(), [=](const std::pair<int, int>& kv) -> bool { return kv.first == oldcolor || kv.second == oldcolor; });`
Java: `for (IntPair kv : colorMap) { if (kv.a() == oldColor || kv.b() == oldColor) { return kv.b(); } }`
Note: C++ uses find_if with lambda; Java uses explicit for loop.

---

### Class: Inst
C++ Location: prog.h:65-194
Java Location: Prog.java:53-290

#### Difference 1
C++:  `Inst() = default;` (default constructor)
Java: `private Inst(InstOp opcode) { this.opcode = requireNonNull(opcode, "opcode is null"); }`
Note: C++ has default constructor; Java requires opcode in constructor.

#### Difference 2
C++:  Union structure with `out1_`, `cap_`, `match_id_`, `struct { lo_; hi_; hint_foldcase_; }`, `empty_`
Java: Separate fields for all: `out1`, `cap`, `matchId`, `lo`, `hi`, `hintFoldcase`, `empty`
Note: C++ uses union to save memory; Java uses separate fields for each.

#### Difference 3
C++:  `int out() { return out_opcode_ >> 4; }`
Java: `public int out() { return out; }`
Note: C++ extracts out from packed out_opcode_ field; Java has separate field.

#### Difference 4
C++:  `int last() { return (out_opcode_ >> 3) & 1; }` (returns int)
Java: `public boolean last() { return last; }` (returns boolean)
Note: C++ returns int (0 or 1); Java returns boolean.

#### Difference 5
C++:  `inline bool Matches(int c) { ... }` (no bounds check for c)
Java: `public boolean matches(int c) { if (c < 0 || c > 0xFF) { return false; } ... }`
Note: Java adds explicit bounds checking for c outside [0, 255] range.

#### Difference 6
C++:  `static const int kMaxInst = (1<<28) - 1;`
Java: (not present)
Note: C++ defines maximum instruction constant; Java does not have this constant.

---

### Method: isWordChar (C++: IsWordChar)
C++ Location: prog.h:288-293
Java Location: Prog.java:2018-2025

#### Difference 1
C++:  `static bool IsWordChar(uint8_t c)`
Java: `private static boolean isWordChar(int c) { int b = c & 0xFF; ... }`
Note: C++ takes uint8_t directly; Java takes int and masks with 0xFF.

---

### Method: ConfigurePrefixAccel (C++: ConfigurePrefixAccel)
C++ Location: prog.cc:1020-1037
Java Location: Prog.java:559-578

#### Difference 1
C++:  `void Prog::ConfigurePrefixAccel(const std::string& prefix, bool prefix_foldcase)`
Java: `public void configurePrefixAccel(ByteSlice prefix, boolean foldcase)`
Note: C++ takes std::string; Java takes ByteSlice.

#### Difference 2
C++:  `if (prefix_foldcase_) { prefix_size_ = std::min(prefix_size_, kShiftDFAFinal); prefix_dfa_ = BuildShiftDFA(prefix.substr(0, prefix_size_)); }`
Java: (no ShiftDFA implementation)
Note: C++ implements ShiftDFA for foldcase prefix search; Java does not implement this optimization.

#### Difference 3
C++:  `else if (prefix_size_ != 1) { prefix_front_ = prefix.front(); prefix_back_ = prefix.back(); }`
Java: (stores full prefix array and pre-computes broadcast mask)
Note: C++ stores only front/back bytes; Java stores full prefix and SWAR broadcast mask.

#### Difference 4
C++:  `else { prefix_front_ = prefix.front(); }` (for single-byte prefix, uses memchr)
Java: (same logic but using indexOf with SWAR optimization)
Note: C++ relies on memchr; Java implements custom indexOf with SWAR.

---

### Method: PrefixAccel (C++: PrefixAccel)
C++ Location: prog.h:250-259
Java Location: Prog.java:591-636

#### Difference 1
C++:  `const void* PrefixAccel(const void* data, size_t size)`
Java: `public int prefixAccel(byte[] data, int offset, int length)`
Note: C++ takes void pointer and returns void pointer; Java takes byte array with offset/length and returns index.

#### Difference 2
C++:  `if (prefix_foldcase_) { return PrefixAccel_ShiftDFA(data, size); }`
Java: (no ShiftDFA path; handles foldcase via indexOfFoldcase)
Note: C++ has ShiftDFA acceleration for foldcase; Java uses simpler byte-by-byte for foldcase.

#### Difference 3
C++:  `else if (prefix_size_ != 1) { return PrefixAccel_FrontAndBack(data, size); }`
Java: FrontAndBack-like logic inline in prefixAccel method
Note: C++ calls separate method; Java inlines the logic.

#### Difference 4
C++:  `else { return memchr(data, prefix_front_, size); }`
Java: `return indexOf(data, offset, length, prefix[0], prefixFoldcase);`
Note: C++ uses memchr; Java uses custom indexOf with SWAR optimization.

---

### Method: PrefixAccel_FrontAndBack (C++: PrefixAccel_FrontAndBack)
C++ Location: prog.cc:1136-1179
Java Location: Prog.java:614-635 (inline in prefixAccel)

#### Difference 1
C++:  Uses AVX2 SIMD when available (`#if defined(__AVX2__)`)
Java: Uses SWAR (SIMD Within A Register) or Vector API based on USE_VECTOR_API flag
Note: C++ has conditional AVX2 SIMD; Java uses software SWAR or Java Vector API.

#### Difference 2
C++:  `ABSL_DCHECK_GE(prefix_size_, size_t{2});`
Java: (no explicit check; handles single-byte case separately)
Note: C++ asserts prefix_size >= 2; Java branches on prefixSize == 1 earlier.

---

### Summary for Prog.java vs prog.cc
- Methods compared: 14 (Prog constructor, optimize, isMatch, emptyFlags, computeByteMap, flatten, markSuccessors, markDominator, emitList, computeHints, ByteMapBuilder class, Inst class, isWordChar, configurePrefixAccel/prefixAccel)
- Total differences found: 80+

### Critical Behavioral Differences Summary

1. **Java adds instruction 0 in constructor** - Java explicitly adds FAIL at position 0; C++ starts with size_=0
2. **Missing inst_count_ tracking** - Java does not track instruction counts by opcode in flatten()
3. **Missing debug assertions** - Java omits C++ debug checks (ABSL_LOG(DFATAL), #ifndef NDEBUG blocks)
4. **Bounds checks added in Java** - Java adds extra bounds checks in computeByteMap, flatten
5. **Division by zero protection** - Java adds Math.max(1, listCount) in bitStateTextMaxSize calculation
6. **Inst union vs separate fields** - Java uses separate fields instead of C++ union (memory trade-off)
7. **Missing ShiftDFA for foldcase prefix** - Java does not implement the ShiftDFA optimization for case-insensitive prefix search
8. **Java Inst.matches() adds bounds check** - Returns false for c outside [0, 255], C++ has undefined behavior
9. **Loop control structure differences** - C++ uses goto Loop; Java uses while(true) with break
