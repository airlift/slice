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

import com.sun.management.ThreadMXBean;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRe2Allocations
{
    @Test
    public void testBooleanDfaSearchDoesNotAllocate()
    {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        threadBean.setThreadAllocatedMemoryEnabled(true);

        Re2 pattern = Re2.compile(Slices.utf8Slice("([a-z]+)-([0-9]+)"));
        Slice source = Slices.utf8Slice("................................................................");

        for (int iteration = 0; iteration < 20_000; iteration++) {
            assertThat(pattern.partialMatch(source)).isFalse();
        }

        long threadId = Thread.currentThread().threadId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);
        int matchCount = 0;
        for (int iteration = 0; iteration < 10_000; iteration++) {
            if (pattern.partialMatch(source)) {
                matchCount++;
            }
        }
        long allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - allocatedBefore;

        assertThat(matchCount).isZero();
        assertThat(allocatedBytes).isZero();
    }

    @Test
    public void testCallerBufferCaptureDoesNotAllocate()
    {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        threadBean.setThreadAllocatedMemoryEnabled(true);

        Re2 pattern = Re2.compile(Slices.utf8Slice("([a-z]+)-([0-9]+)"));
        byte[] sourceBytes = new byte[32_768];
        Arrays.fill(sourceBytes, (byte) '.');
        byte[] match = Slices.utf8Slice("abc-123").getBytes();
        System.arraycopy(match, 0, sourceBytes, sourceBytes.length / 2, match.length);
        Slice source = Slices.wrappedBuffer(sourceBytes);
        int[] groups = new int[6];

        for (int iteration = 0; iteration < 20_000; iteration++) {
            assertThat(pattern.matchInto(source, Re2.Anchor.UNANCHORED, groups)).isTrue();
        }

        long threadId = Thread.currentThread().threadId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);
        int matchCount = 0;
        for (int iteration = 0; iteration < 10_000; iteration++) {
            if (pattern.matchInto(source, Re2.Anchor.UNANCHORED, groups)) {
                matchCount++;
            }
        }
        long allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - allocatedBefore;

        assertThat(matchCount).isEqualTo(10_000);
        assertThat(groups).containsExactly(16_384, 16_391, 16_384, 16_387, 16_388, 16_391);
        assertThat(allocatedBytes).isZero();
    }

    @Test
    public void testTinyCallerBufferCaptureDoesNotAllocate()
    {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        threadBean.setThreadAllocatedMemoryEnabled(true);

        Re2 pattern = Re2.compile(Slices.utf8Slice("([a-z]+)-([0-9]+)"));
        Slice source = Slices.utf8Slice("................abc-123.........................................");
        int[] groups = new int[6];

        for (int iteration = 0; iteration < 20_000; iteration++) {
            assertThat(pattern.matchInto(source, Re2.Anchor.UNANCHORED, groups)).isTrue();
        }

        long threadId = Thread.currentThread().threadId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);
        int matchCount = 0;
        for (int iteration = 0; iteration < 10_000; iteration++) {
            if (pattern.matchInto(source, Re2.Anchor.UNANCHORED, groups)) {
                matchCount++;
            }
        }
        long allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - allocatedBefore;

        assertThat(matchCount).isEqualTo(10_000);
        assertThat(groups).containsExactly(16, 23, 16, 19, 20, 23);
        assertThat(allocatedBytes).isZero();
    }

    @Test
    public void testMatcherBitStateCaptureAllocatesOnlyInvocationState()
    {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        threadBean.setThreadAllocatedMemoryEnabled(true);

        Re2 pattern = Re2.compile(Slices.utf8Slice("(a)(b)(c)(d)(e)"));
        Slice source = Slices.utf8Slice("abcde");
        Re2Matcher matcher = pattern.matcher(source);

        for (int iteration = 0; iteration < 20_000; iteration++) {
            matcher.reset(source);
            assertThat(matcher.matches()).isTrue();
        }

        long threadId = Thread.currentThread().threadId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);
        int matchCount = 0;
        for (int iteration = 0; iteration < 10_000; iteration++) {
            matcher.reset(source);
            if (matcher.matches()) {
                matchCount++;
            }
        }
        long allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - allocatedBefore;

        assertThat(matchCount).isEqualTo(10_000);
        assertThat(allocatedBytes).isLessThanOrEqualTo(128L * 10_000);
    }

    @Test
    public void testMatcherNfaCaptureAllocatesOnlyInvocationState()
    {
        ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        threadBean.setThreadAllocatedMemoryEnabled(true);

        Re2 pattern = Re2.compile(Slices.utf8Slice("(a{100})(b{100})(c{100})(d{100})(e{100})(f{100})"));
        Slice source = Slices.utf8Slice(
                "a".repeat(100) +
                        "b".repeat(100) +
                        "c".repeat(100) +
                        "d".repeat(100) +
                        "e".repeat(100) +
                        "f".repeat(100));
        Re2Matcher matcher = pattern.matcher(source);

        for (int iteration = 0; iteration < 20_000; iteration++) {
            matcher.reset(source);
            assertThat(matcher.matches()).isTrue();
        }

        long threadId = Thread.currentThread().threadId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);
        int matchCount = 0;
        for (int iteration = 0; iteration < 10_000; iteration++) {
            matcher.reset(source);
            if (matcher.matches()) {
                matchCount++;
            }
        }
        long allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - allocatedBefore;

        assertThat(matchCount).isEqualTo(10_000);
        assertThat(allocatedBytes).isLessThanOrEqualTo(128L * 10_000);
    }
}
