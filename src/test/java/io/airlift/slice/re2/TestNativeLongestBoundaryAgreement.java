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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNativeLongestBoundaryAgreement
{
    private static final String RESOURCE = "io/airlift/slice/re2/testing/native_longest_boundary.jsonl";
    private static final int CASE_COUNT = 148;
    private static final HexFormat HEX_FORMAT = HexFormat.of();
    private static final List<GoldenJsonl.JsonObject> TEST_CASES = GoldenJsonl.readObjects(RESOURCE);

    @Test
    public void testCorpusCoverage()
    {
        assertThat(TEST_CASES).hasSize(CASE_COUNT);
        assertThat(TEST_CASES.stream().filter(testCase -> testCase.getString("id").startsWith("long-")).count()).isEqualTo(40);
        assertThat(TEST_CASES.stream().filter(testCase -> testCase.getString("id").startsWith("anchor-")).count()).isEqualTo(48);
        assertThat(TEST_CASES.stream().filter(testCase -> testCase.getString("id").startsWith("byte-")).count()).isEqualTo(60);
        assertThat(TEST_CASES.stream().filter(testCase -> testCase.getString("encoding").equals("utf8")).count()).isEqualTo(CASE_COUNT / 2);
        assertThat(TEST_CASES.stream().filter(testCase -> testCase.getString("encoding").equals("latin1")).count()).isEqualTo(CASE_COUNT / 2);

        Map<String, GoldenJsonl.JsonObject> firstCases = new HashMap<>();
        int differingPairs = 0;
        for (GoldenJsonl.JsonObject testCase : TEST_CASES) {
            String id = testCase.getString("id");
            if (id.startsWith("long-") && id.endsWith("-first")) {
                firstCases.put(id.substring(0, id.length() - "-first".length()), testCase);
            }
            if (id.startsWith("long-") && id.endsWith("-longest")) {
                String pairId = id.substring(0, id.length() - "-longest".length());
                GoldenJsonl.JsonObject firstCase = firstCases.get(pairId);
                assertThat(firstCase).as("first case for %s", pairId).isNotNull();
                if (firstCase.getBoolean("matched") != testCase.getBoolean("matched") ||
                        !firstCase.getString("groups").equals(testCase.getString("groups"))) {
                    differingPairs++;
                }
            }
        }
        assertThat(differingPairs).isEqualTo(18);
    }

    @TestFactory
    public List<DynamicTest> testNativeCases()
    {
        return TEST_CASES.stream()
                .map(testCase -> DynamicTest.dynamicTest(testCase.getString("id"), () -> assertNativeCase(testCase)))
                .toList();
    }

    private static void assertNativeCase(GoldenJsonl.JsonObject testCase)
    {
        assertThat(testCase.getBoolean("ok")).as("native compilation").isTrue();

        Re2.Options options = switch (testCase.getString("encoding")) {
            case "utf8" -> Re2.Options.defaults();
            case "latin1" -> Re2.Options.latin1();
            default -> throw new IllegalArgumentException("unknown encoding: " + testCase.getString("encoding"));
        };
        options.setLongestMatch(testCase.getBoolean("longest"));

        Re2 regexp = Re2.compile(decodeHex(testCase.getString("patternHex")), options);
        Slice text = decodeHex(testCase.getString("textHex"));
        int start = Integer.parseInt(testCase.getString("start"));
        int end = Integer.parseInt(testCase.getString("end"));
        Re2.Anchor anchor = parseAnchor(testCase.getString("anchor"));
        boolean expectedMatched = testCase.getBoolean("matched");
        int[] expectedGroups = expectedMatched ? parseGroups(testCase.getString("groups")) : null;

        int groupCount = Integer.parseInt(testCase.getString("groupCount"));
        int[] actualGroups = new int[groupCount * 2];
        boolean actualMatched = regexp.matchInto(text, start, end, anchor, actualGroups);
        assertThat(actualMatched).as("matchInto matched").isEqualTo(expectedMatched);
        if (expectedMatched) {
            assertThat(actualGroups).as("matchInto groups").containsExactly(expectedGroups);
        }

        MatchResult result = regexp.matchResult(text, start, end, anchor);
        assertResult(result, expectedMatched, expectedGroups);

        if (start == 0 && end == text.length()) {
            int[] wholeInputGroups = new int[groupCount * 2];
            assertThat(regexp.matchInto(text, anchor, wholeInputGroups)).as("whole-input matchInto matched").isEqualTo(expectedMatched);
            if (expectedMatched) {
                assertThat(wholeInputGroups).as("whole-input matchInto groups").containsExactly(expectedGroups);
            }

            switch (anchor) {
                case UNANCHORED -> {
                    assertThat(regexp.partialMatch(text)).as("partialMatch").isEqualTo(expectedMatched);
                    assertResult(regexp.partialMatchResult(text), expectedMatched, expectedGroups);
                }
                case ANCHOR_BOTH -> {
                    assertThat(regexp.fullMatch(text)).as("fullMatch").isEqualTo(expectedMatched);
                    assertResult(regexp.fullMatchResult(text), expectedMatched, expectedGroups);
                }
                case ANCHOR_START -> {}
            }
        }
    }

    private static void assertResult(MatchResult result, boolean expectedMatched, int[] expectedGroups)
    {
        if (!expectedMatched) {
            assertThat(result).isNull();
            return;
        }

        assertThat(result).isNotNull();
        assertThat(result.groupCount()).isEqualTo((expectedGroups.length / 2) - 1);
        for (int group = 0; group < expectedGroups.length / 2; group++) {
            assertThat(result.start(group)).as("group %s start", group).isEqualTo(expectedGroups[group * 2]);
            assertThat(result.end(group)).as("group %s end", group).isEqualTo(expectedGroups[(group * 2) + 1]);
        }
    }

    private static Re2.Anchor parseAnchor(String anchor)
    {
        return switch (anchor) {
            case "unanchored" -> Re2.Anchor.UNANCHORED;
            case "start" -> Re2.Anchor.ANCHOR_START;
            case "both" -> Re2.Anchor.ANCHOR_BOTH;
            default -> throw new IllegalArgumentException("unknown anchor: " + anchor);
        };
    }

    private static Slice decodeHex(String value)
    {
        return Slices.wrappedBuffer(value.equals("-") ? new byte[0] : HEX_FORMAT.parseHex(value));
    }

    private static int[] parseGroups(String value)
    {
        if (value.isEmpty()) {
            return new int[0];
        }
        return Arrays.stream(value.split(","))
                .flatMapToInt(range -> Arrays.stream(range.split(":"))
                        .mapToInt(Integer::parseInt))
                .toArray();
    }
}
