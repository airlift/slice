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

import java.util.Arrays;

final class SparseIntArray
{
    private int size;
    private int[] sparse;
    private int[] denseIndex;
    private int[] denseValue;

    public SparseIntArray()
    {
        this(0);
    }

    public SparseIntArray(int maxSize)
    {
        resize(maxSize);
    }

    public SparseIntArray(SparseIntArray source)
    {
        if (source == null) {
            throw new IllegalArgumentException("source is null");
        }
        this.size = source.size;
        this.sparse = Arrays.copyOf(source.sparse, source.sparse.length);
        this.denseIndex = Arrays.copyOf(source.denseIndex, source.denseIndex.length);
        this.denseValue = Arrays.copyOf(source.denseValue, source.denseValue.length);
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int maxSize()
    {
        return denseIndex.length;
    }

    /**
     * Clears the array in O(1).
     */
    public void clear()
    {
        size = 0;
    }

    public void resize(int newMaxSize)
    {
        if (newMaxSize < 0) {
            throw new IllegalArgumentException("newMaxSize must be >= 0: " + newMaxSize);
        }

        if (sparse == null) {
            sparse = new int[newMaxSize];
            denseIndex = new int[newMaxSize];
            denseValue = new int[newMaxSize];
            size = 0;
            return;
        }

        if (newMaxSize > maxSize()) {
            sparse = Arrays.copyOf(sparse, newMaxSize);
            denseIndex = Arrays.copyOf(denseIndex, newMaxSize);
            denseValue = Arrays.copyOf(denseValue, newMaxSize);
        }

        size = Math.min(size, newMaxSize);
    }

    public boolean hasIndex(int indexValue)
    {
        if (indexValue < 0 || indexValue >= maxSize()) {
            return false;
        }

        int densePosition = sparse[indexValue];
        return densePosition >= 0 && densePosition < size && denseIndex[densePosition] == indexValue;
    }

    public void set(int indexValue, int value)
    {
        checkIndex(indexValue);
        if (!hasIndex(indexValue)) {
            createIndex(indexValue);
        }
        denseValue[sparse[indexValue]] = value;
    }

    public void setNew(int indexValue, int value)
    {
        checkIndex(indexValue);
        if (hasIndex(indexValue)) {
            throw new IllegalArgumentException("already present: " + indexValue);
        }
        createIndex(indexValue);
        denseValue[sparse[indexValue]] = value;
    }

    public void setExisting(int indexValue, int value)
    {
        checkIndex(indexValue);
        if (!hasIndex(indexValue)) {
            throw new IllegalArgumentException("not present: " + indexValue);
        }
        denseValue[sparse[indexValue]] = value;
    }

    public int getExisting(int indexValue)
    {
        checkIndex(indexValue);
        if (!hasIndex(indexValue)) {
            throw new IllegalArgumentException("not present: " + indexValue);
        }
        return denseValue[sparse[indexValue]];
    }

    public int denseIndexAt(int position)
    {
        if (position < 0 || position >= size) {
            throw new IllegalArgumentException("position out of range: " + position);
        }
        return denseIndex[position];
    }

    public int denseValueAt(int position)
    {
        if (position < 0 || position >= size) {
            throw new IllegalArgumentException("position out of range: " + position);
        }
        return denseValue[position];
    }

    /**
     * Sorts entries by increasing index, updating the internal sparse mapping accordingly.
     */
    public void sortByIndex()
    {
        // Simple insertion-sort; size is typically small in RE2 uses (and <= maxSize <= 1<<24).
        for (int densePosition = 1; densePosition < size; densePosition++) {
            int keyIndex = denseIndex[densePosition];
            int keyValue = denseValue[densePosition];

            int insertionPosition = densePosition - 1;
            while (insertionPosition >= 0 && denseIndex[insertionPosition] > keyIndex) {
                denseIndex[insertionPosition + 1] = denseIndex[insertionPosition];
                denseValue[insertionPosition + 1] = denseValue[insertionPosition];
                insertionPosition--;
            }
            denseIndex[insertionPosition + 1] = keyIndex;
            denseValue[insertionPosition + 1] = keyValue;
        }

        // Rebuild sparse mapping for entries in [0,size).
        for (int position = 0; position < size; position++) {
            sparse[denseIndex[position]] = position;
        }
    }

    private void createIndex(int indexValue)
    {
        if (size >= maxSize()) {
            throw new IllegalStateException("array is full (maxSize=" + maxSize() + ")");
        }
        sparse[indexValue] = size;
        denseIndex[size] = indexValue;
        size++;
    }

    private void checkIndex(int indexValue)
    {
        if (indexValue < 0 || indexValue >= maxSize()) {
            throw new IllegalArgumentException("illegal index: " + indexValue);
        }
    }
}
