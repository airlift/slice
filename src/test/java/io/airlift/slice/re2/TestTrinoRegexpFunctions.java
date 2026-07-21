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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRegexpFunctions
{
    @ParameterizedTest(name = "{0}")
    @MethodSource("functionCases")
    public void testFunctionCorpus(FunctionCase functionCase)
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8(decode(functionCase.pattern())));
        Slice source = utf8(decode(functionCase.source()));
        switch (functionCase.operation()) {
            case CONTAINS -> assertThat(regexp.contains(source)).isEqualTo(Boolean.parseBoolean(functionCase.expected()));
            case COUNT -> assertThat(regexp.count(source)).isEqualTo(Long.parseLong(functionCase.expected()));
            case POSITION -> {
                String[] arguments = functionCase.argument().split(",", -1);
                assertThat(regexp.position(source, Long.parseLong(arguments[0]), Long.parseLong(arguments[1])))
                        .isEqualTo(Long.parseLong(functionCase.expected()));
            }
            case EXTRACT -> assertThat(string(regexp.extract(source, Integer.parseInt(functionCase.argument()))))
                    .isEqualTo(decodeNullable(functionCase.expected()));
            case EXTRACT_ALL -> assertThat(strings(regexp.extractAll(source, Integer.parseInt(functionCase.argument()))))
                    .containsExactlyElementsOf(expectedList(functionCase.expected()));
            case SPLIT -> assertThat(strings(regexp.split(source)))
                    .containsExactlyElementsOf(expectedList(functionCase.expected()));
            case REPLACE -> assertThat(regexp.replace(source, utf8(decode(functionCase.argument()))).toStringUtf8())
                    .isEqualTo(decode(functionCase.expected()));
        }
    }

    @Test
    public void testLambdaReplacement()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("(a)|(b)"));

        assertThat(regexp.replace(utf8("ab"), groups -> {
            if (groups.get(0) != null) {
                return utf8("A");
            }
            return utf8("B");
        }).toStringUtf8()).isEqualTo("AB");

        assertThat(regexp.replace(utf8("a"), groups -> null)).isNull();
    }

    @Test
    public void testSingleByteRepeatOperations()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("x*"));
        Slice source = utf8("axxa💰");

        assertThat(regexp.count(source)).isEqualTo(5);
        assertThat(strings(regexp.extractAll(source))).containsExactly("", "xx", "", "", "");
        assertThat(strings(regexp.split(source))).containsExactly("", "a", "", "a", "💰", "");
        assertThat(regexp.replace(source, utf8("_")).toStringUtf8()).isEqualTo("_a__a_💰_");
        assertThat(regexp.replace(source, utf8("$0")).toStringUtf8()).isEqualTo("axxa💰");
        assertThat(regexp.replace(source, groups -> {
            assertThat(groups).isEmpty();
            return utf8("_");
        }).toStringUtf8()).isEqualTo("_a__a_💰_");

        assertThat(regexp.position(source, 1, 1)).isEqualTo(1);
        assertThat(regexp.position(source, 1, 2)).isEqualTo(2);
        assertThat(regexp.position(source, 1, 3)).isEqualTo(4);
        assertThat(regexp.position(source, 1, 4)).isEqualTo(5);
        assertThat(regexp.position(source, 1, 5)).isEqualTo(6);
        assertThat(regexp.position(source, 1, 6)).isEqualTo(-1);
        assertThat(regexp.position(source, Long.MAX_VALUE, 1)).isEqualTo(-1);

        assertThatThrownBy(() -> regexp.replace(source, utf8("$1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown group");
        assertThatThrownBy(() -> regexp.replace(source, utf8("${missing}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown named group");

        TrinoRegexp captured = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(strings(captured.extractAll(source, 1))).containsExactly("", "xx", "", "", "");
    }

    @Test
    public void testSingleByteRepeatAnalysisEligibility()
    {
        TrinoRegexp nonNullable = TrinoRegexp.compile(utf8("[a-z]+"));
        assertThat(nonNullable.isSingleByteRepeatMatcherComputed()).isFalse();
        assertThat(nonNullable.count(utf8("abc"))).isEqualTo(1);
        assertThat(nonNullable.isSingleByteRepeatMatcherComputed()).isFalse();

        TrinoRegexp nullableRepeat = TrinoRegexp.compile(utf8("x*"));
        assertThat(nullableRepeat.isSingleByteRepeatMatcherComputed()).isFalse();
        assertThat(nullableRepeat.count(utf8("ax"))).isEqualTo(3);
        assertThat(nullableRepeat.isSingleByteRepeatMatcherComputed()).isTrue();
    }

    @Test
    public void testFullCaptureMatcherDoesNotAnalyzeSingleBytePattern()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("(a)"));
        assertThat(regexp.isSingleByteMatcherComputed()).isFalse();

        assertThat(regexp.extract(utf8("a"), 1)).isEqualTo(utf8("a"));
        assertThat(regexp.isSingleByteMatcherComputed()).isFalse();
    }

    @Test
    public void testContainsDoesNotInitializeSingleByteMatcher()
    {
        TrinoRegexp delimiter = TrinoRegexp.compile(utf8("[,;]"));
        assertThat(delimiter.isSingleByteMatcherComputed()).isFalse();
        assertThat(delimiter.contains(utf8("abc;def"))).isTrue();
        assertThat(delimiter.isSingleByteMatcherComputed()).isFalse();
        assertThat(delimiter.contains(Slices.wrappedBuffer(utf8("xxabc;defyy").getBytes(), 2, 7))).isTrue();
        assertThat(delimiter.contains(utf8("abcdef"))).isFalse();
        assertThatThrownBy(() -> delimiter.contains(null)).isInstanceOf(NullPointerException.class);

        TrinoRegexp captures = TrinoRegexp.compile(utf8("([a-z]+)-([0-9]+)"));
        assertThat(captures.contains(utf8("abc-123"))).isTrue();
        assertThat(captures.isSingleByteMatcherComputed()).isFalse();
        assertThatThrownBy(() -> captures.contains(null)).isInstanceOf(NullPointerException.class);

        for (String pattern : new String[] {"a$", "\\ba\\b"}) {
            TrinoRegexp unsupported = TrinoRegexp.compile(utf8(pattern));
            assertThat(unsupported.contains(utf8("a"))).isTrue();
            assertThat(unsupported.contains(utf8("bbb"))).isFalse();
            assertThat(unsupported.isSingleByteMatcherComputed()).isFalse();
        }
    }

    @Test
    public void testCapturedSingleByteRepeatUsesBoundaryMatcherWhenCapturesAreNotNeeded()
    {
        Slice source = utf8("axx");

        TrinoRegexp position = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(position.isSingleByteRepeatMatcherComputed()).isFalse();
        assertThat(position.position(source, 1, 2)).isEqualTo(2);
        assertThat(position.isSingleByteRepeatMatcherComputed()).isTrue();

        TrinoRegexp groupZero = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(strings(groupZero.extractAll(source, 0))).containsExactly("", "xx", "");
        assertThat(groupZero.isSingleByteRepeatMatcherComputed()).isTrue();

        TrinoRegexp capturedGroup = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(strings(capturedGroup.extractAll(source, 1))).containsExactly("", "xx", "");
        assertThat(capturedGroup.isSingleByteRepeatMatcherComputed()).isFalse();

        TrinoRegexp split = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(strings(split.split(source))).containsExactly("", "a", "", "");
        assertThat(split.isSingleByteRepeatMatcherComputed()).isTrue();

        TrinoRegexp groupZeroReplacement = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(groupZeroReplacement.replace(source, utf8("$0"))).isEqualTo(source);
        assertThat(groupZeroReplacement.isSingleByteRepeatMatcherComputed()).isTrue();

        TrinoRegexp capturedReplacement = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(capturedReplacement.replace(source, utf8("$1"))).isEqualTo(source);
        assertThat(capturedReplacement.isSingleByteRepeatMatcherComputed()).isFalse();

        TrinoRegexp lambdaReplacement = TrinoRegexp.compile(utf8("(x*)"));
        assertThat(lambdaReplacement.replace(source, groups -> utf8("_"))).isEqualTo(utf8("_a__"));
        assertThat(lambdaReplacement.isSingleByteRepeatMatcherComputed()).isFalse();
    }

    @Test
    public void testSingleByteRepeatOperationsWithMalformedUtf8Latin1AndSliceOffset()
    {
        byte[] malformedBacking = {
                '!', 'a', (byte) 0xED, (byte) 0xA0, (byte) 0x80, 'x', 'x', (byte) 0xFF, '?'};
        assertSingleByteRepeatOperationsMatchGeneralMatcher(
                utf8("x*"),
                Slices.wrappedBuffer(malformedBacking, 1, malformedBacking.length - 2));

        byte[] latin1Pattern = {(byte) 0xE9, '*'};
        byte[] latin1Backing = {'!', 'a', (byte) 0xE9, (byte) 0xE9, (byte) 0xFF, 'b', '?'};
        assertSingleByteRepeatOperationsMatchGeneralMatcher(
                Slices.wrappedBuffer(latin1Pattern),
                Slices.wrappedBuffer(latin1Backing, 1, latin1Backing.length - 2));
    }

    @Test
    public void testReplacementValidation()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("(?<name>x)"));

        assertThatThrownBy(() -> regexp.replace(utf8("x"), utf8("\\")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> regexp.replace(utf8("x"), utf8("$")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> regexp.replace(utf8("x"), utf8("${missing}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown named group");
    }

    @Test
    public void testReplacementCapturingGroupRequirement()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("(?<name>x)"));

        assertThat(regexp.replacementNeedsCapturingGroups(utf8("_"))).isFalse();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("$0"))).isFalse();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("$00"))).isFalse();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("\\$1"))).isFalse();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("$1"))).isTrue();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("$01"))).isTrue();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("${name}"))).isTrue();
        assertThat(regexp.replacementNeedsCapturingGroups(utf8("$"))).isTrue();
    }

    @Test
    public void testGroupValidationAndNonzeroSliceOffset()
    {
        TrinoRegexp regexp = TrinoRegexp.compile(utf8("(abc)"));
        byte[] bytes = utf8("xxabcxx").getBytes();
        Slice source = Slices.wrappedBuffer(bytes, 2, 3);

        assertThat(regexp.extract(source, 1).toStringUtf8()).isEqualTo("abc");
        assertThatThrownBy(() -> regexp.extract(source, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> regexp.extract(source, 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMalformedUtf8Terminates()
    {
        Slice malformed = Slices.wrappedBuffer(new byte[] {'A', (byte) 0xED, (byte) 0xA0, (byte) 0x80, 'B'});

        assertThat(TrinoRegexp.compile(malformed).contains(malformed)).isTrue();
        assertThat(TrinoRegexp.compile(utf8("")).count(malformed)).isGreaterThan(0);
    }

    @Test
    public void testForwardOnlyCountEligibility()
    {
        Re2 delimiter = TrinoRegexp.compile(utf8(",")).pattern();
        assertThat(delimiter.countMatches(utf8("a,b,,c"))).isEqualTo(3);

        Re2 anchored = TrinoRegexp.compile(utf8("^a")).pattern();
        assertThat(anchored.countMatches(utf8("aa"))).isEqualTo(1);

        Re2 nullable = TrinoRegexp.compile(utf8("x*")).pattern();
        assertThat(nullable.countMatches(utf8("xxx"))).isEqualTo(-1);
    }

    @Test
    public void testSingleByteCountEligibility()
    {
        assertThat(countSingleByteMatches("[,;]", utf8("a,b;c;;"))).isEqualTo(4);
        assertThat(countSingleByteMatches("([,;])", utf8("a,b;c;;"))).isEqualTo(4);
        assertThat(countSingleByteMatches("(?i)a", utf8("aAbA"))).isEqualTo(3);
        assertThat(countSingleByteMatches("\\C", Slices.wrappedBuffer(new byte[] {'a', (byte) 0xFF, 'b'}))).isEqualTo(3);

        assertThat(countSingleByteMatches("a+", utf8("aaa"))).isEqualTo(-1);
        assertThat(countSingleByteMatches(".", utf8("a💰"))).isEqualTo(-1);
        assertThat(countSingleByteMatches("^a", utf8("aa"))).isEqualTo(-1);
        assertThat(countSingleByteMatches("^ab[,;]", utf8("ab,xx;"))).isEqualTo(-1);
        assertThat(TrinoRegexp.compile(utf8("^ab[,;]")).count(utf8("ab,xx;"))).isEqualTo(1);

        SingleByteMatcher matcher = Re2.compile(utf8("[,;]")).createSingleByteMatcher();
        Slice slice = Slices.wrappedBuffer(utf8("xxa,b;cyy").getBytes(), 2, 5);
        assertThat(matcher.find(slice, 0)).isEqualTo(1);
        assertThat(matcher.find(slice, 2)).isEqualTo(3);
        assertThat(matcher.find(slice, 4)).isEqualTo(-1);
    }

    @Test
    public void testSingleByteCountAnalysisIsStackSafe()
            throws InterruptedException
    {
        int captureCount = 10_000;
        String pattern = "(".repeat(captureCount) + "a" + ")".repeat(captureCount);
        AtomicReference<Long> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread compilerThread = new Thread(null, () -> {
            try {
                SingleByteMatcher matcher = Re2.compile(utf8(pattern)).createSingleByteMatcher();
                result.set(matcher == null ? -1 : matcher.count(utf8("aba")));
            }
            catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "single-byte-matcher-small-stack", 256 * 1024);

        compilerThread.start();
        compilerThread.join();

        assertThat(failure.get()).isNull();
        assertThat(result.get()).isEqualTo(2);
    }

    private static long countSingleByteMatches(String pattern, Slice source)
    {
        SingleByteMatcher matcher = Re2.compile(utf8(pattern)).createSingleByteMatcher();
        return matcher == null ? -1 : matcher.count(source);
    }

    private static void assertSingleByteRepeatOperationsMatchGeneralMatcher(Slice pattern, Slice source)
    {
        TrinoRegexp regexp = TrinoRegexp.compile(pattern);
        Re2Matcher matcher = regexp.pattern().matcher(source);
        List<MatchBoundary> matches = new ArrayList<>();
        List<Slice> groups = new ArrayList<>();
        while (matcher.find()) {
            matches.add(new MatchBoundary(matcher.start(), matcher.end()));
            groups.add(matcher.group());
        }

        assertThat(regexp.count(source)).isEqualTo(matches.size());
        for (int occurrence = 0; occurrence < matches.size(); occurrence++) {
            assertThat(regexp.position(source, 1, occurrence + 1L))
                    .isEqualTo(SliceUtf8.countCodePoints(source, 0, matches.get(occurrence).start()) + 1L);
        }
        assertThat(regexp.position(source, 1, matches.size() + 1L)).isEqualTo(-1);

        int byteStart = SliceUtf8.offsetOfCodePoint(source, 1);
        Re2Matcher offsetMatcher = regexp.pattern().matcher(source);
        long expectedOffsetPosition = offsetMatcher.find(byteStart)
                ? SliceUtf8.countCodePoints(source, 0, offsetMatcher.start()) + 1L
                : -1;
        assertThat(regexp.position(source, 2, 1)).isEqualTo(expectedOffsetPosition);
        assertThat(regexp.extractAll(source)).containsExactlyElementsOf(groups);

        List<Slice> expectedParts = new ArrayList<>();
        DynamicSliceOutput expectedReplacement = new DynamicSliceOutput(source.length() + matches.size());
        int previousEnd = 0;
        for (MatchBoundary match : matches) {
            expectedParts.add(source.slice(previousEnd, match.start() - previousEnd));
            expectedReplacement.writeBytes(source, previousEnd, match.start() - previousEnd);
            expectedReplacement.writeByte('_');
            previousEnd = match.end();
        }
        expectedParts.add(source.slice(previousEnd, source.length() - previousEnd));
        expectedReplacement.writeBytes(source, previousEnd, source.length() - previousEnd);

        assertThat(regexp.split(source)).containsExactlyElementsOf(expectedParts);
        assertThat(regexp.replace(source, utf8("_"))).isEqualTo(expectedReplacement.slice());
        assertThat(regexp.replace(source, ignored -> utf8("_"))).isEqualTo(expectedReplacement.slice());
        assertThat(regexp.isSingleByteRepeatMatcherComputed()).isTrue();
    }

    @Test
    public void testForwardOnlyCountMatchesMatcherIteration()
    {
        List<String> patterns = List.of(
                "a",
                "a+",
                "ab|cd",
                "(a)b",
                "[a-z]+",
                "^a",
                "a$",
                "^a$",
                "a.*?b",
                "(?i)abc",
                "\\bword\\b",
                "世+");
        List<String> sources = List.of(
                "",
                "a",
                "aaabacda",
                "abc ABC abc",
                "word sword word",
                "世界世世");

        for (String pattern : patterns) {
            Re2 re2 = Re2.compile(utf8(pattern));
            assertThat(re2.canMatchEmpty()).as("pattern %s", pattern).isFalse();
            for (String source : sources) {
                Slice input = utf8(source);
                long expectedCount = 0;
                Re2Matcher matcher = re2.matcher(input);
                while (matcher.find()) {
                    expectedCount++;
                }
                assertThat(re2.countMatches(input))
                        .as("pattern %s, source %s", pattern, source)
                        .isEqualTo(expectedCount);
            }
        }
    }

    private static Stream<FunctionCase> functionCases()
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                TestTrinoRegexpFunctions.class.getResourceAsStream("/io/airlift/slice/re2/trino-function-cases.tsv"),
                StandardCharsets.UTF_8));
        return reader.lines()
                .filter(line -> !line.isBlank() && !line.startsWith("#"))
                .map(line -> line.split("\\t", -1))
                .map(parts -> new FunctionCase(Operation.valueOf(parts[0]), parts[1], parts[2], parts[3], parts[4]));
    }

    private static List<String> expectedList(String value)
    {
        return Stream.of(value.split("\\|", -1))
                .map(TestTrinoRegexpFunctions::decodeNullable)
                .toList();
    }

    private static List<String> strings(List<Slice> values)
    {
        return values.stream()
                .map(TestTrinoRegexpFunctions::string)
                .toList();
    }

    private static String string(Slice value)
    {
        return value == null ? null : value.toStringUtf8();
    }

    private static String decodeNullable(String value)
    {
        return value.equals("<null>") ? null : decode(value);
    }

    private static String decode(String value)
    {
        if (value.equals("<empty>")) {
            return "";
        }
        return value.replace("<newline>", "\n");
    }

    private static Slice utf8(String value)
    {
        return Slices.utf8Slice(value);
    }

    private enum Operation
    {
        CONTAINS,
        COUNT,
        POSITION,
        EXTRACT,
        EXTRACT_ALL,
        SPLIT,
        REPLACE,
    }

    private record FunctionCase(Operation operation, String pattern, String source, String argument, String expected) {}

    private record MatchBoundary(int start, int end) {}
}
