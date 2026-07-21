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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRegexpWalker
{
    @Test
    public void testCopyCalledForAdjacentDuplicateChildren()
    {
        Regexp leaf = Regexp.emptyMatch(0);
        Regexp concat = Regexp.concat(0, List.of(leaf, leaf));

        AtomicInteger copies = new AtomicInteger();
        RegexpWalker<Integer> walker = new RegexpWalker<>()
        {
            @Override
            protected PreVisitResult<Integer> preVisit(Regexp re, Integer parentArg)
            {
                return new PreVisitResult<>(parentArg + 1, false);
            }

            @Override
            protected Integer postVisit(Regexp re, Integer parentArg, Integer preArg, List<Integer> childArgs)
            {
                return preArg;
            }

            @Override
            protected Integer copy(Integer arg)
            {
                copies.incrementAndGet();
                return arg;
            }

            @Override
            protected Integer shortVisit(Regexp re, Integer parentArg)
            {
                throw new AssertionError("unexpected shortVisit");
            }
        };

        walker.walk(concat, 0);
        assertThat(copies.get()).isEqualTo(1);
    }

    @Test
    public void testExponentialVisitLimitCountsNodes()
    {
        Regexp leaf = Regexp.emptyMatch(0);
        Regexp concat = Regexp.concat(0, List.of(leaf, leaf, leaf));
        AtomicInteger visits = new AtomicInteger();
        AtomicInteger shortVisits = new AtomicInteger();

        RegexpWalker<Integer> walker = new RegexpWalker<>()
        {
            @Override
            protected PreVisitResult<Integer> preVisit(Regexp regexp, Integer parentArg)
            {
                visits.incrementAndGet();
                return new PreVisitResult<>(parentArg, false);
            }

            @Override
            protected Integer postVisit(Regexp regexp, Integer parentArg, Integer preArg, List<Integer> childArgs)
            {
                return preArg;
            }

            @Override
            protected Integer shortVisit(Regexp regexp, Integer parentArg)
            {
                shortVisits.incrementAndGet();
                return parentArg;
            }
        };

        walker.walkExponential(concat, 0, 3);
        assertThat(visits).hasValue(3);
        assertThat(shortVisits).hasValue(1);
    }
}
