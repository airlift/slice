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
import org.junit.jupiter.api.TestFactory;

import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUpstreamPublicMatch
{
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    @TestFactory
    public List<DynamicTest> testPublicMatchCases()
    {
        return GoldenJsonl.readObjects("io/airlift/slice/re2/upstream_public_match.jsonl").stream()
                .map(testCase -> DynamicTest.dynamicTest(testCase.getString("id"), () -> assertMatch(testCase)))
                .toList();
    }

    private static void assertMatch(GoldenJsonl.JsonObject testCase)
    {
        assertThat(testCase.getBoolean("ok"))
                .as("upstream compiled case")
                .isTrue();

        Re2.Options options = switch (testCase.getString("encoding")) {
            case "utf8" -> Re2.Options.defaults();
            case "latin1" -> Re2.Options.latin1();
            default -> throw new IllegalArgumentException("unknown encoding: " + testCase.getString("encoding"));
        };
        options.setLongestMatch(testCase.getBoolean("longest"));

        Re2 regexp = Re2.compile(decodeHex(testCase.getString("patternHex")), options);
        Slice text = decodeHex(testCase.getString("textHex"));
        Re2.Anchor anchor = switch (testCase.getString("anchor")) {
            case "unanchored" -> Re2.Anchor.UNANCHORED;
            case "start" -> Re2.Anchor.ANCHOR_START;
            case "both" -> Re2.Anchor.ANCHOR_BOTH;
            default -> throw new IllegalArgumentException("unknown anchor: " + testCase.getString("anchor"));
        };

        int groupCount = Integer.parseInt(testCase.getString("groupCount"));
        int[] groups = groupCount == 0 ? null : new int[groupCount * 2];
        boolean matched = regexp.matchInto(
                text,
                Integer.parseInt(testCase.getString("start")),
                Integer.parseInt(testCase.getString("end")),
                anchor,
                groups);

        assertThat(matched).as("matched").isEqualTo(testCase.getBoolean("matched"));
        if (matched && groups != null) {
            assertThat(groups).as("groups").containsExactly(parseGroups(testCase.getString("groups")));
        }
    }

    private static Slice decodeHex(String value)
    {
        return Slices.wrappedBuffer(value.equals("-") ? new byte[0] : HEX_FORMAT.parseHex(value));
    }

    private static int[] parseGroups(String value)
    {
        String[] ranges = value.split(",");
        int[] groups = new int[ranges.length * 2];
        for (int group = 0; group < ranges.length; group++) {
            String[] offsets = ranges[group].split(":");
            groups[group * 2] = Integer.parseInt(offsets[0]);
            groups[(group * 2) + 1] = Integer.parseInt(offsets[1]);
        }
        return groups;
    }
}
