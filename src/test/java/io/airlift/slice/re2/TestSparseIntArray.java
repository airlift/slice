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

public class TestSparseIntArray
{
    @Test
    public void testSetAndGet()
    {
        SparseIntArray a = new SparseIntArray(10);
        assertThat(a.isEmpty()).isTrue();

        a.set(3, 100);
        a.set(7, 200);
        a.set(3, 101);

        assertThat(a.size()).isEqualTo(2);
        assertThat(a.hasIndex(3)).isTrue();
        assertThat(a.hasIndex(7)).isTrue();
        assertThat(a.hasIndex(0)).isFalse();
        assertThat(a.hasIndex(-1)).isFalse();
        assertThat(a.hasIndex(10)).isFalse();

        assertThat(a.getExisting(3)).isEqualTo(101);
        assertThat(a.getExisting(7)).isEqualTo(200);

        assertThat(a.denseIndexAt(0)).isEqualTo(3);
        assertThat(a.denseValueAt(0)).isEqualTo(101);
        assertThat(a.denseIndexAt(1)).isEqualTo(7);
        assertThat(a.denseValueAt(1)).isEqualTo(200);
    }

    @Test
    public void testIterationWhileAdding()
    {
        SparseIntArray q = new SparseIntArray(10);
        q.set(1, 10);

        for (int it = 0; it < q.size(); it++) {
            int placeholderIndex = q.denseIndexAt(it);
            int value = q.denseValueAt(it);
            if (placeholderIndex < 5) {
                q.set(placeholderIndex + 1, value + 1);
            }
        }

        assertThat(q.size()).isEqualTo(5);
        assertThat(q.getExisting(5)).isEqualTo(14);
    }

    @Test
    public void testClear()
    {
        SparseIntArray a = new SparseIntArray(3);
        a.set(2, 20);
        a.set(1, 10);
        assertThat(a.size()).isEqualTo(2);

        a.clear();
        assertThat(a.isEmpty()).isTrue();

        a.set(0, 1);
        assertThat(a.size()).isEqualTo(1);
        assertThat(a.getExisting(0)).isEqualTo(1);
    }

    @Test
    public void testResize()
    {
        SparseIntArray a = new SparseIntArray(2);
        a.set(0, 0);
        a.set(1, 1);

        a.resize(5);
        assertThat(a.size()).isEqualTo(2);
        assertThat(a.maxSize()).isEqualTo(5);
        assertThat(a.getExisting(1)).isEqualTo(1);

        a.resize(1);
        assertThat(a.size()).isEqualTo(1);
        assertThat(a.maxSize()).isEqualTo(5);
        assertThat(a.hasIndex(0)).isTrue();
    }

    @Test
    public void testCopy()
    {
        SparseIntArray a = new SparseIntArray(3);
        a.set(2, 20);
        SparseIntArray b = new SparseIntArray(a);

        assertThat(b.size()).isEqualTo(1);
        assertThat(b.hasIndex(2)).isTrue();
        assertThat(b.getExisting(2)).isEqualTo(20);
    }

    @Test
    public void testSortByIndex()
    {
        SparseIntArray a = new SparseIntArray(10);
        a.set(7, 70);
        a.set(3, 30);
        a.set(9, 90);
        assertThat(a.denseIndexAt(0)).isEqualTo(7);

        a.sortByIndex();

        assertThat(a.denseIndexAt(0)).isEqualTo(3);
        assertThat(a.denseIndexAt(1)).isEqualTo(7);
        assertThat(a.denseIndexAt(2)).isEqualTo(9);

        assertThat(a.getExisting(3)).isEqualTo(30);
        assertThat(a.getExisting(7)).isEqualTo(70);
        assertThat(a.getExisting(9)).isEqualTo(90);
    }

    @Test
    public void testRangeChecks()
    {
        SparseIntArray a = new SparseIntArray(2);
        assertThrows(IllegalArgumentException.class, () -> a.set(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> a.set(2, 0));
        assertThrows(IllegalArgumentException.class, () -> a.setNew(2, 0));
        assertThrows(IllegalArgumentException.class, () -> a.getExisting(0));
        assertThrows(IllegalArgumentException.class, () -> a.denseIndexAt(0));
    }
}
