/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice.re2;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstration tests for reverse DFA usage patterns and edge cases.
 * <p>
 * This test file serves as <b>executable documentation</b> for the reverse DFA feature.
 * Each test demonstrates correct usage, edge cases, or common mistakes with extensive comments.
 * <p>
 * <b>Purpose:</b>
 * <ul>
 * <li>Show the canonical two-phase search pattern
 * <li>Verify position correctness across various scenarios
 * <li>Document edge cases (UTF-8, word boundaries, zero-length matches)
 * <li>Demonstrate anchor flag swapping behavior
 * <li>Show what NOT to do (common mistakes)
 * </ul>
 * <p>
 * For complete documentation, see {@code REVERSE_DFA.md}.
 *
 * @see DfaReversePositionTest for focused position correctness tests
 */
class TestDfaReverseDemonstration
{
    private static final long MAX_MEM = 1_000_000;

    /**
     * CANONICAL EXAMPLE: Two-Phase Search Pattern
     * <p>
     * This is the standard pattern for finding complete match positions (both start and end).
     * <p>
     * Pattern: {@code \d+} (one or more digits)
     * Text: {@code "abc123xyz"}
     * <p>
     * <b>Phase 1 (Forward DFA):</b>
     * <ul>
     * <li>Scans left-to-right through entire text
     * <li>Finds match end at position 6 (after "123")
     * <li>Does NOT track where match started
     * </ul>
     * <p>
     * <b>Phase 2 (Reverse DFA):</b>
     * <ul>
     * <li>Scans right-to-left through prefix [0, 6) = "abc123"
     * <li>Finds match start at position 3 (before "123")
     * <li>Returns start position despite method named "end()"
     * </ul>
     * <p>
     * Result: Complete match is text[3:6] = "123"
     */
    @Test
    void testCanonicalTwoPhasePattern()
    {
        // Setup: parse pattern and prepare text
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("\\d+".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("abc123xyz".getBytes(UTF_8));

        // PHASE 1: Forward DFA finds match END
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM * 2 / 3);
        assertThat(forwardProg).isNotNull(); // Compilation succeeded

        long forwardResult = Dfa.search(
                forwardProg,
                text,      // Search entire text
                false,     // Not anchored (can match anywhere)
                Prog.MatchKind.FIRST_MATCH,
                true);     // Return match boundary

        // Forward phase found match ending at position 6
        assertThat(forwardResult).isGreaterThanOrEqualTo(0);
        assertThat((int) forwardResult).isEqualTo(6); // Position after "123"

        // PHASE 2: Reverse DFA finds match START
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM / 3);
        assertThat(reverseProg).isNotNull();

        // CRITICAL: Search only the prefix [0, matchEnd), not the full text
        int matchEnd = (int) forwardResult;
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, matchEnd); // "abc123"

        long reverseResult = Dfa.search(
                reverseProg,
                text,      // Context is still full text
                0,
                prefix.length(), // Search prefix, not full text!
                true,      // Anchored to prefix end (required for correctness)
                Prog.MatchKind.LONGEST_MATCH,
                true);     // Return match boundary

        // Reverse phase found match starting at position 3
        assertThat(reverseResult).isGreaterThanOrEqualTo(0);
        assertThat((int) reverseResult).isEqualTo(3); // Returns START position (confusing name)

        // Verification: Complete match is text[3:6] = "123"
        int matchStart = (int) reverseResult;
        String matched = new String(text.byteArray(), matchStart, matchEnd - matchStart, UTF_8);
        assertThat(matched).isEqualTo("123");
    }

    /**
     * UTF-8 Multi-Byte Character Boundaries
     * <p>
     * RE2 works on byte sequences, not Unicode strings. Positions are BYTE offsets.
     * <p>
     * Text: {@code "xxcaféyy"} where 'é' is 2 bytes (U+00E9 = 0xC3 0xA9)
     * Pattern: {@code café} (4 Unicode codepoints, 5 bytes)
     * <p>
     * <b>Expected positions:</b>
     * <ul>
     * <li>Match start: byte 2 (before 'c')
     * <li>Match end: byte 7 (after 'é', which spans bytes 5-6)
     * <li>Match is 5 bytes: c(1) a(1) f(1) é(2)
     * </ul>
     */
    @Test
    void testUtf8MultiByteCharacterBoundaries()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("café".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xxcaféyy".getBytes(UTF_8));

        // Verify text encoding: "xxcaféyy" = 8 bytes (not 8 chars)
        // xx = 2, c = 1, a = 1, f = 1, é = 2, yy = 2 → total 9 bytes
        assertThat(text.length()).isEqualTo(9);

        // Phase 1: Forward finds end
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(7); // Byte offset after "café"

        // Phase 2: Reverse finds start
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(2); // Byte offset before "café"

        // Verification: Match is 5 bytes
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("café");
        assertThat((int) fwd - (int) rev).isEqualTo(5); // 5 bytes, not 4 chars
    }

    /**
     * Word Boundary Behavior (\b and \B)
     * <p>
     * Word boundaries work in reverse direction by checking byte transitions.
     * <p>
     * Pattern: {@code \bword\b} (word boundaries on both sides)
     * Text: {@code "xx word yy"}
     * <p>
     * Forward finds end at byte 7 (after "word"), reverse finds start at byte 3 (before "word").
     */
    @Test
    void testWordBoundaries()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("\\bword\\b".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xx word yy".getBytes(UTF_8));

        // Phase 1: Forward
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(7); // After "word"

        // Phase 2: Reverse
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(3); // Before "word"

        // Verification
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("word");
    }

    /**
     * Zero-Length Match at Start-of-Text Anchor (^)
     * <p>
     * Pattern: {@code ^} (start-of-text anchor, matches zero-length position)
     * <p>
     * <b>Forward phase:</b> Matches at position 0, returns end=0
     * <b>Reverse phase:</b> Matches at position 0, returns end=0
     * <p>
     * <b>Anchor swapping:</b> Pattern "^" compiles to {@code anchorEnd=true} in reverse mode!
     * This is correct because when scanning backward, start-of-text is reached at the END of the scan.
     */
    @Test
    void testZeroLengthMatchStartAnchor()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("^".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("hello".getBytes(UTF_8));

        // Phase 1: Forward
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(0); // Zero-length match at position 0

        // Phase 2: Reverse
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);

        // IMPORTANT: Anchor flags are SWAPPED in reverse program
        assertThat(reverseProg.anchorStart()).isFalse(); // Surprising!
        assertThat(reverseProg.anchorEnd()).isTrue();    // "^" becomes end anchor

        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(0); // Zero-length match
    }

    /**
     * Zero-Length Match at End-of-Text Anchor ($)
     * <p>
     * Pattern: {@code $} (end-of-text anchor)
     * <p>
     * <b>Forward phase:</b> Matches at end of text (position 5 for "hello")
     * <b>Reverse phase:</b> Matches at same position
     * <p>
     * <b>Anchor swapping:</b> Pattern "$" compiles to {@code anchorStart=true} in reverse mode!
     */
    @Test
    void testZeroLengthMatchEndAnchor()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("$".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("hello".getBytes(UTF_8));

        // Phase 1: Forward finds end-of-text position
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(5); // End of "hello"

        // Phase 2: Reverse
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);

        // IMPORTANT: Anchor flags are SWAPPED
        assertThat(reverseProg.anchorStart()).isTrue();  // "$" becomes start anchor
        assertThat(reverseProg.anchorEnd()).isFalse();

        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(5); // Same position
    }

    /**
     * Anchor Inversion: Pattern with Both Anchors (^...$)
     * <p>
     * Pattern: {@code ^abc$} (fully anchored)
     * <p>
     * <b>Forward compilation:</b> {@code anchorStart=true, anchorEnd=true}
     * <b>Reverse compilation:</b> BOTH SWAPPED → {@code anchorStart=true, anchorEnd=true}
     * (but semantics are inverted during search)
     */
    @Test
    void testAnchorInversionBothAnchors()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("^abc$".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("abc".getBytes(UTF_8));

        // Forward program has both anchors
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        assertThat(forwardProg.anchorStart()).isTrue();
        assertThat(forwardProg.anchorEnd()).isTrue();

        // Reverse program ALSO has both anchors (swapped then swapped = both)
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        assertThat(reverseProg.anchorStart()).isTrue();
        assertThat(reverseProg.anchorEnd()).isTrue();

        // Phase 1: Forward
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);
        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(3);

        // Phase 2: Reverse
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);
        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(0);
    }

    /**
     * COMMON MISTAKE #1: Using Reverse DFA Alone (Without Forward Phase)
     * <p>
     * This demonstrates what happens if you try to use reverse DFA without the forward phase.
     * The result is semantically incorrect - you get a "start" position with no corresponding "end".
     * <p>
     * Pattern: {@code \d+}
     * Text: {@code "abc123xyz"}
     * <p>
     * <b>WRONG:</b> Skip forward phase, use reverse alone
     * <b>Result:</b> Confusing output that doesn't represent a complete match
     * <p>
     * <b>NEVER DO THIS.</b> Always use the two-phase pattern.
     */
    @Test
    void testCommonMistakeReverseAlone()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("\\d+".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("abc123xyz".getBytes(UTF_8));

        // WRONG: Using reverse DFA alone without forward phase
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        long wrongResult = Dfa.search(reverseProg, text, false, Prog.MatchKind.LONGEST_MATCH, true);

        // You get a match...
        assertThat(wrongResult).isGreaterThanOrEqualTo(0);

        // ...but what does end=0 mean here? It's the START of "123" (position 3)
        // But this is scanning the wrong direction on the wrong text!
        // The result is semantically meaningless.

        // Don't rely on this output - it's not the correct two-phase pattern
        // This test exists only to show the mistake, not to validate behavior
    }

    /**
     * COMMON MISTAKE #2: Wrong Prefix Slice Bounds
     * <p>
     * Reverse DFA must search only the prefix [0, matchEnd), not the full text.
     * <p>
     * Pattern: {@code \d+}
     * Text: {@code "abc123xyz456"}
     * <p>
     * If forward finds first match "123" ending at position 6, reverse must search
     * prefix "abc123" to find start. If you search full text, reverse might find
     * the start of "456" instead!
     */
    @Test
    void testCommonMistakeWrongPrefixBounds()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("\\d+".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("abc123xyz456".getBytes(UTF_8));

        // Phase 1: Forward finds first match "123" ending at 6
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(6); // After "123"

        // CORRECT: Use prefix [0, matchEnd)
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice correctPrefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long correctRev = Dfa.search(reverseProg, text, 0, correctPrefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(correctRev).isGreaterThanOrEqualTo(0);
        assertThat((int) correctRev).isEqualTo(3); // Correct start of "123"

        // WRONG: Using full text in reverse phase
        long wrongRev = Dfa.search(reverseProg, text, false, Prog.MatchKind.LONGEST_MATCH, true);

        // This might match, but the position is meaningless in this context
        // (depends on pattern and text, but generally incorrect)
        // Don't do this!
    }

    /**
     * Complex Pattern: Alternation
     * <p>
     * Pattern: {@code (abc|def|ghi)} (alternation with three options)
     * Text: {@code "xxdefyy"}
     * <p>
     * Tests that reverse DFA handles alternation correctly, finding the start
     * of whichever branch matched in the forward phase.
     */
    @Test
    void testComplexPatternAlternation()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("(abc|def|ghi)".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xxdefyy".getBytes(UTF_8));

        // Phase 1: Forward
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(5); // After "def"

        // Phase 2: Reverse
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(2); // Before "def"

        // Verification
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("def");
    }

    /**
     * Complex Pattern: Quantifier Range
     * <p>
     * Pattern: {@code a{2,5}} (between 2 and 5 'a's)
     * Text: {@code "xxaaaaayy"}
     * <p>
     * Tests quantifier handling in reverse direction.
     */
    @Test
    void testComplexPatternQuantifierRange()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("a{2,5}".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xxaaaaayy".getBytes(UTF_8));

        // Phase 1: Forward finds maximal match (5 a's)
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(7); // After "aaaaa"

        // Phase 2: Reverse finds start
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(2); // Before "aaaaa"

        // Verification: 5 a's
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("aaaaa");
    }

    /**
     * Complex Pattern: Nested Repetition
     * <p>
     * Pattern: {@code ((a+)b+)+} (nested plus quantifiers)
     * Text: {@code "xxaaabbbaabyy"}
     * <p>
     * Tests that reverse DFA handles complex nested structures.
     */
    @Test
    void testComplexPatternNestedRepetition()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("((a+)b+)+".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xxaaabbbaabyy".getBytes(UTF_8));

        // Phase 1: Forward
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(11); // After "aaabbbaab"

        // Phase 2: Reverse
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(2); // Before "aaabbbaab"

        // Verification
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("aaabbbaab");
    }

    /**
     * Edge Case: Empty Pattern Match
     * <p>
     * Pattern: {@code a*} (zero or more 'a's, can match empty string)
     * Text: {@code "xyz"}
     * <p>
     * Forward and reverse both match zero-length at position 0.
     */
    @Test
    void testEdgeCaseEmptyMatch()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer("a*".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("xyz".getBytes(UTF_8));

        // Phase 1: Forward matches zero-length at position 0
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(0); // Zero-length match

        // Phase 2: Reverse also matches zero-length at position 0
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(0); // Zero-length match

        // Match is empty string at position [0:0)
        assertThat((int) fwd - (int) rev).isEqualTo(0);
    }

    /**
     * Edge Case: Full Text Match
     * <p>
     * Pattern: {@code .*} (match entire text)
     * Text: {@code "hello"}
     * <p>
     * Forward matches to end (position 5), reverse finds start (position 0).
     */
    @Test
    void testEdgeCaseFullTextMatch()
    {
        Regexp regexp = RegexpParser.parse(Slices.wrappedBuffer(".*".getBytes(UTF_8)), Regexp.LIKE_PERL | Regexp.LATIN1).regexp();
        Slice text = Slices.wrappedBuffer("hello".getBytes(UTF_8));

        // Phase 1: Forward matches entire text
        Prog forwardProg = Compiler.compile(regexp, false, MAX_MEM);
        long fwd = Dfa.search(forwardProg, text, false, Prog.MatchKind.FIRST_MATCH, true);

        assertThat(fwd).isGreaterThanOrEqualTo(0);
        assertThat((int) fwd).isEqualTo(5); // End of text

        // Phase 2: Reverse finds start
        Prog reverseProg = Compiler.compile(regexp, true, MAX_MEM);
        Slice prefix = Slices.wrappedBuffer(text.byteArray(), 0, (int) fwd);
        long rev = Dfa.search(reverseProg, text, 0, prefix.length(), true, Prog.MatchKind.LONGEST_MATCH, true);

        assertThat(rev).isGreaterThanOrEqualTo(0);
        assertThat((int) rev).isEqualTo(0); // Start of text

        // Match is full text
        String matched = new String(text.byteArray(), (int) rev, (int) fwd - (int) rev, UTF_8);
        assertThat(matched).isEqualTo("hello");
    }
}
