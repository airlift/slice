I'll read both files to perform the line-by-line audit.
## File: OnePass.java vs onepass.cc

### Method: Constants
C++ Location: onepass.cc:170-182
Java Location: OnePass.java:27-43

#### Difference 1
C++:  `static const uint32_t kImpossible = kEmptyWordBoundary | kEmptyNonWordBoundary;`
Java: `private static final int IMPOSSIBLE = EmptyOp.EMPTY_WORD_BOUNDARY | EmptyOp.EMPTY_NO_WORD_BOUNDARY;`
Note: Java references EmptyOp constants, C++ uses local constants

#### Difference 2
C++:  No explicit EMPTY_ALL_FLAGS constant (defined in prog.h)
Java: `private static final int EMPTY_ALL_FLAGS = (1 << EMPTY_SHIFT) - 1;`
Note: Java defines EMPTY_ALL_FLAGS locally, C++ imports kEmptyAllFlags from prog.h

#### Difference 3
C++:  `static const uint32_t` types for flags
Java: `private static final int` types for all constants
Note: C++ uses explicit unsigned types, Java uses signed int

---

### Method: Satisfy (C++) vs satisfy (Java)
C++ Location: onepass.cc:194-199
Java Location: OnePass.java:219-224

#### Difference 4
C++:  `static bool Satisfy(uint32_t cond, absl::string_view context, const char* p)`
Java: `private static boolean satisfy(int cond, ByteSlice context, int p)`
Note: Different parameter types - C++ uses pointer p, Java uses int index p

#### Difference 5
C++:  
```cpp
if (cond & kEmptyAllFlags & ~satisfied)
    return false;
  return true;
```
Java: 
```java
int need = cond & EMPTY_ALL_FLAGS;
return (need & ~satisfied) == 0;
```
Note: Java introduces intermediate variable `need`, C++ uses direct expression. Logic is equivalent.

---

### Method: ApplyCaptures (C++) vs applyCaptures (Java)
C++ Location: onepass.cc:203-208
Java Location: OnePass.java:226-233

#### Difference 6
C++:  `static void ApplyCaptures(uint32_t cond, const char* p, const char** cap, int ncap)`
Java: `private static void applyCaptures(int cond, int p, int[] cap, int ncap)`
Note: C++ uses pointer array for captures, Java uses int array

#### Difference 7
C++:  `if (cond & (1 << kCapShift << i))`
Java: `if ((cond & (1 << (CAP_SHIFT + i))) != 0)`
Note: Java uses explicit parentheses and `!= 0` check. Mathematically equivalent.

---

### Method: SearchOnePass (C++) vs search (Java)
C++ Location: onepass.cc:216-346
Java Location: OnePass.java:45-212

#### Difference 8
C++:  
```cpp
bool Prog::SearchOnePass(absl::string_view text, absl::string_view context,
                         Anchor anchor, MatchKind kind,
                         absl::string_view* match, int nmatch)
```
Java: 
```java
public static boolean search(Prog prog, ByteSlice text, ByteSlice context, boolean anchored, boolean longest, int[] submatch)
```
Note: C++ is a member function of Prog, Java is a static function taking Prog. C++ uses enum types for anchor/kind, Java uses booleans.

#### Difference 9
C++:  `if (anchor != kAnchored && kind != kFullMatch)`
Java: `if (!anchored)`
Note: Java checks only anchored flag, C++ has conditional allowing unanchored with kFullMatch

#### Difference 10
C++:  
```cpp
ABSL_LOG(DFATAL) << "Cannot use SearchOnePass for unanchored matches.";
return false;
```
Java: `return false;`
Note: C++ logs DFATAL before returning false, Java silently returns false

#### Difference 11
C++:  No explicit isOnePass() check in SearchOnePass
Java: 
```java
if (!prog.isOnePass()) {
    return false;
}
```
Note: Java explicitly checks if program is onepass, C++ assumes caller checked

#### Difference 12
C++:  No validation that submatch length is even
Java: 
```java
if (submatch != null && (submatch.length % 2) != 0) {
    throw new IllegalArgumentException("submatch length must be even: " + submatch.length);
}
```
Note: Java validates submatch array has even length

#### Difference 13
C++:  No validation of text within context bounds
Java: 
```java
if (text.byteArray() != context.byteArray()) {
    return false;
}
int ctxBegin = context.byteArrayOffset();
int ctxEnd = ctxBegin + context.length();
int textBegin = text.byteArrayOffset();
int textEnd = textBegin + text.length();
if (textBegin < ctxBegin || textEnd > ctxEnd) {
    return false;
}
```
Note: Java has extensive validation that text lies within context, C++ does not

#### Difference 14
C++:  
```cpp
int ncap = 2*nmatch;
if (ncap < 2)
    ncap = 2;
```
Java: 
```java
int nmatch = (submatch == null) ? 0 : (submatch.length / 2);
int ncap = 2 * nmatch;
if (ncap < 2) {
    ncap = 2;
}
```
Note: Java computes nmatch from submatch array length, C++ receives nmatch as parameter

#### Difference 15
C++:  No explicit check for ncap > MAX_CAP
Java: 
```java
if (ncap > MAX_CAP) {
    return false;
}
```
Note: Java adds explicit bounds check on ncap

#### Difference 16
C++:  
```cpp
const char* cap[kMaxCap];
for (int i = 0; i < ncap; i++)
    cap[i] = NULL;
```
Java: 
```java
int[] cap = new int[ncap];
Arrays.fill(cap, -1);
```
Note: C++ uses fixed-size array with NULL pointers, Java dynamically allocates with -1

#### Difference 17
C++:  No prefill of match[] output array
Java: 
```java
if (submatch != null) {
    Arrays.fill(submatch, -1);
}
```
Note: Java prefills output array with -1

#### Difference 18
C++:  
```cpp
uint8_t* nodes = onepass_nodes_.data();
int statesize = sizeof(OneState) + bytemap_range()*sizeof(uint32_t);
OneState* state = IndexToNode(nodes, statesize, 0);
```
Java: 
```java
int[] stateMatchCond = prog.onePassMatchCond();
int[] stateAction = prog.onePassAction();
int state = 0;
int stateOffset = 0;
```
Note: C++ uses raw byte array with calculated offsets, Java uses separate arrays for matchcond and action

#### Difference 19
C++:  `int c = bytemap[*p & 0xFF];`
Java: `int cls = bytemap[bytes[p] & 0xFF] & 0xFF;`
Note: Java applies additional `& 0xFF` mask on the bytemap result

#### Difference 20
C++:  `uint32_t cond = state->action[c];`
Java: `int cond = stateAction[stateOffset + cls];`
Note: C++ accesses action via struct member, Java uses flat array with offset

#### Difference 21
C++:  `uint32_t nextindex = cond >> kIndexShift;`
Java: `state = cond >>> INDEX_SHIFT;`
Note: C++ uses signed right shift on uint32_t, Java uses unsigned right shift (`>>>`)

#### Difference 22
C++:  
```cpp
state = IndexToNode(nodes, statesize, nextindex);
nextmatchcond = state->matchcond;
```
Java: 
```java
state = cond >>> INDEX_SHIFT;
stateOffset = state * range;
nextMatchCond = stateMatchCond[state];
```
Note: C++ calculates node pointer, Java maintains separate state index and offset

#### Difference 23
C++:  `state = NULL;`
Java: `state = -1;`
Note: C++ uses NULL pointer for invalid state, Java uses -1 sentinel value

#### Difference 24
C++:  
```cpp
if (kind == kFullMatch)
    goto skipmatch;

if (matchcond == kImpossible)
    goto skipmatch;

if ((cond & kMatchWins) == 0 && (nextmatchcond & kEmptyAllFlags) == 0)
    goto skipmatch;
```
Java: 
```java
if (kind == KIND_FULL_MATCH) {
    // skip
}
else if (matchCond != IMPOSSIBLE) {
    if ((cond & MATCH_WINS) != 0 || (nextMatchCond & EMPTY_ALL_FLAGS) != 0) {
```
Note: C++ uses goto for control flow, Java uses nested if-else structure

#### Difference 25
C++:  `for (int i = 2; i < 2*nmatch; i++)`
Java: `for (int i = 2; i < ncap; i++)`
Note: C++ uses 2*nmatch as loop bound, Java uses ncap. When nmatch==0, ncap==2 but 2*nmatch==0.

#### Difference 26
C++:  `if (nmatch > 1 && (matchcond & kCapMask))`
Java: `if (nmatch > 1 && (matchCond & CAP_MASK) != 0)`
Note: Java uses explicit `!= 0` comparison

#### Difference 27
C++:  
```cpp
if (kind == kFirstMatch && (cond & kMatchWins))
    goto done;
```
Java: 
```java
if (kind == KIND_FIRST_MATCH && (cond & MATCH_WINS) != 0) {
    break;
}
```
Note: C++ uses goto done, Java uses break with explicit `!= 0`

#### Difference 28
C++:  `if (state == NULL) goto done;`
Java: `if (state < 0) { break; }`
Note: C++ checks NULL pointer, Java checks negative index

#### Difference 29
C++:  `if ((cond & kCapMask) && nmatch > 1)`
Java: `if ((cond & CAP_MASK) != 0 && nmatch > 1)`
Note: Operand order differs; Java has explicit `!= 0`

#### Difference 30
C++:  
```cpp
// Look for match at end of input.
{
    uint32_t matchcond = state->matchcond;
```
Java: 
```java
// Look for match at end of input.
if (state >= 0) {
    int matchCond = stateMatchCond[state];
```
Note: Java guards end-of-input match check with `state >= 0`, C++ block executes unconditionally (relies on prior goto)

#### Difference 31
C++:  
```cpp
if (nmatch > 1 && (matchcond & kCapMask))
    ApplyCaptures(matchcond, p, cap, ncap);
for (int i = 2; i < ncap; i++)
    matchcap[i] = cap[i];
```
Java: 
```java
if (nmatch > 1 && (matchCond & CAP_MASK) != 0) {
    applyCaptures(matchCond, p, cap, ncap);
}
for (int i = 2; i < ncap; i++) {
    matchcap[i] = cap[i];
}
```
Note: Order is same, but Java adds explicit `!= 0` and braces

#### Difference 32
C++:  
```cpp
for (int i = 0; i < nmatch; i++)
    match[i] = absl::string_view(
        matchcap[2 * i],
        static_cast<size_t>(matchcap[2 * i + 1] - matchcap[2 * i]));
```
Java: 
```java
if (submatch != null) {
    for (int i = 0; i < nmatch; i++) {
        int a = matchcap[2 * i];
        int b = matchcap[2 * i + 1];
        int o = 2 * i;
        if (a < 0 || b < 0) {
            submatch[o] = -1;
            submatch[o + 1] = -1;
        }
        else {
            submatch[o] = a - textBegin;
            submatch[o + 1] = b - textBegin;
        }
    }
}
```
Note: C++ creates string_view directly from pointers. Java checks for null submatch, converts from absolute to relative offsets, and handles -1 sentinel values.

#### Difference 33
C++:  No overloaded convenience method
Java: 
```java
public static boolean search(Prog prog, ByteSlice text, boolean anchored, boolean longest, int[] submatch)
{
    return search(prog, text, text, anchored, longest, submatch);
}
```
Note: Java provides convenience overload that defaults context to text

---

### Method: IsOnePass (C++)
C++ Location: onepass.cc:383-621
Java Location: Not in OnePass.java

#### Difference 34
C++:  Contains full `IsOnePass()` implementation (lines 383-621)
Java: Not present in OnePass.java
Note: The IsOnePass analysis functionality is not in this Java file - would be in Prog.java

---

### Method: IndexToNode (C++)
C++ Location: onepass.cc:211-214
Java Location: Not in OnePass.java

#### Difference 35
C++:  
```cpp
static inline OneState* IndexToNode(uint8_t* nodes, int statesize,
                                    int nodeindex) {
  return reinterpret_cast<OneState*>(nodes + statesize*nodeindex);
}
```
Java: Not present
Note: Java uses separate arrays (stateMatchCond, stateAction) instead of pointer arithmetic

---

### Method: AddQ (C++)
C++ Location: onepass.cc:354-361
Java Location: Not in OnePass.java

#### Difference 36
C++:  
```cpp
static bool AddQ(Instq *q, int id) {
  if (id == 0)
    return true;
  if (q->contains(id))
    return false;
  q->insert(id);
  return true;
}
```
Java: Not present
Note: Helper for IsOnePass, would be in Prog.java if ported

---

### Method: OnePass_Checks (C++)
C++ Location: onepass.cc:186-192
Java Location: Not in OnePass.java

#### Difference 37
C++:  
```cpp
void OnePass_Checks() {
  static_assert((1<<kEmptyShift)-1 == kEmptyAllFlags,
                "kEmptyShift disagrees with kEmptyAllFlags");
  static_assert(kMaxCap == Prog::kMaxOnePassCapture*2,
                "kMaxCap disagrees with kMaxOnePassCapture");
}
```
Java: Not present
Note: C++ compile-time assertions not ported to Java

---

### Summary for OnePass.java vs onepass.cc
- Methods compared: 4 main functions (constants, Satisfy/satisfy, ApplyCaptures/applyCaptures, SearchOnePass/search)
- Total differences found: 37

Key structural differences:
1. Java is a standalone utility class with static methods; C++ is integrated into Prog class
2. Java uses int arrays for captures with -1 sentinels; C++ uses pointer arrays with NULL
3. Java uses separate arrays for state data (stateMatchCond, stateAction); C++ uses a single byte array with pointer arithmetic via OneState struct
4. Java converts absolute byte positions to relative offsets in output; C++ returns string_views directly
5. Java adds extensive input validation not present in C++
6. C++ IsOnePass analysis function (~240 lines) not in this Java file
