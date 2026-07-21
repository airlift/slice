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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSparseSet
{
    @Test
    public void testInsertAndContains()
    {
        SparseSet set = new SparseSet(10);
        assertThat(set.isEmpty()).isTrue();

        set.insert(3);
        set.insert(7);
        set.insert(3); // duplicate

        assertThat(set.size()).isEqualTo(2);
        assertThat(set.contains(3)).isTrue();
        assertThat(set.contains(7)).isTrue();
        assertThat(set.contains(0)).isFalse();
        assertThat(set.contains(-1)).isFalse();
        assertThat(set.contains(10)).isFalse();

        assertThat(set.denseAt(0)).isEqualTo(3);
        assertThat(set.denseAt(1)).isEqualTo(7);
    }

    @Test
    public void testIterationWhileInserting()
    {
        SparseSet q = new SparseSet(10);
        q.insert(1);

        for (int it = 0; it < q.size(); it++) {
            int v = q.denseAt(it);
            if (v < 5) {
                q.insert(v + 1);
            }
        }

        assertThat(q.size()).isEqualTo(5);
        assertThat(q.denseAt(0)).isEqualTo(1);
        assertThat(q.denseAt(1)).isEqualTo(2);
        assertThat(q.denseAt(2)).isEqualTo(3);
        assertThat(q.denseAt(3)).isEqualTo(4);
        assertThat(q.denseAt(4)).isEqualTo(5);
    }

    @Test
    public void testClear()
    {
        SparseSet set = new SparseSet(3);
        set.insert(2);
        set.insert(1);
        assertThat(set.size()).isEqualTo(2);

        set.clear();
        assertThat(set.isEmpty()).isTrue();

        set.insert(0);
        assertThat(set.size()).isEqualTo(1);
        assertThat(set.denseAt(0)).isEqualTo(0);
    }

    @Test
    public void testResize()
    {
        SparseSet set = new SparseSet(2);
        set.insert(0);
        set.insert(1);

        set.resize(5);
        assertThat(set.size()).isEqualTo(2);
        assertThat(set.maxSize()).isEqualTo(5);
        assertThat(set.contains(0)).isTrue();
        assertThat(set.contains(1)).isTrue();

        set.resize(1);
        assertThat(set.size()).isEqualTo(1);
        assertThat(set.maxSize()).isEqualTo(5); // we only grow capacity, matching upstream semantics
        assertThat(set.contains(0)).isTrue();
    }

    @Test
    public void testRangeChecks()
    {
        SparseSet set = new SparseSet(2);
        assertThrows(IllegalArgumentException.class, () -> set.insert(-1));
        assertThrows(IllegalArgumentException.class, () -> set.insert(2));
        assertThrows(IllegalArgumentException.class, () -> set.insertNew(2));
        assertThrows(IllegalArgumentException.class, () -> set.denseAt(0));
    }
}
