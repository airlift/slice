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

final class SparseSet
{
    private int size;
    private int[] sparse;
    private int[] dense;

    public SparseSet()
    {
        this(0);
    }

    public SparseSet(int maxSize)
    {
        resize(maxSize);
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
        return dense.length;
    }

    /**
     * Clears the set in O(1).
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
            dense = new int[newMaxSize];
            size = 0;
            return;
        }

        if (newMaxSize > maxSize()) {
            int oldMaxSize = maxSize();
            int[] newSparse = new int[newMaxSize];
            int[] newDense = new int[newMaxSize];
            System.arraycopy(sparse, 0, newSparse, 0, oldMaxSize);
            System.arraycopy(dense, 0, newDense, 0, oldMaxSize);
            sparse = newSparse;
            dense = newDense;
        }

        size = Math.min(size, newMaxSize);
    }

    public boolean contains(int indexValue)
    {
        if (indexValue < 0 || indexValue >= maxSize()) {
            return false;
        }

        int densePosition = sparse[indexValue];
        return densePosition >= 0 && densePosition < size && dense[densePosition] == indexValue;
    }

    public void insert(int indexValue)
    {
        checkIndex(indexValue);
        if (!contains(indexValue)) {
            createIndex(indexValue);
        }
    }

    /**
     * Like {@link #insert(int)}, but only use this if {@link #contains(int)} is known to be false.
     */
    public void insertNew(int indexValue)
    {
        checkIndex(indexValue);
        if (contains(indexValue)) {
            throw new IllegalArgumentException("already present: " + indexValue);
        }
        createIndex(indexValue);
    }

    /**
     * Dense list element at the given index, suitable for iteration like:
     * <pre>{@code
     * for (int it = 0; it < set.size(); it++) {
     *     int value = set.denseAt(it);
     *     ...
     *     set.insert(...); // safe during iteration
     * }
     * }</pre>
     */
    public int denseAt(int index)
    {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("index out of range: " + index);
        }
        return dense[index];
    }

    private void createIndex(int indexValue)
    {
        if (size >= maxSize()) {
            throw new IllegalStateException("set is full (maxSize=" + maxSize() + ")");
        }
        sparse[indexValue] = size;
        dense[size] = indexValue;
        size++;
    }

    private void checkIndex(int indexValue)
    {
        if (indexValue < 0 || indexValue >= maxSize()) {
            throw new IllegalArgumentException("illegal index: " + indexValue);
        }
    }
}
