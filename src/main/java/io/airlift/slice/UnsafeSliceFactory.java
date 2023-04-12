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
package io.airlift.slice;

/**
 * A slice factory for creating unsafe slices
 */
public class UnsafeSliceFactory
{
    private static final UnsafeSliceFactory INSTANCE = new UnsafeSliceFactory();

    /**
     * Get a factory for creating "unsafe" slices that can reference
     * arbitrary memory addresses.
     *
     * @return an unsafe slice factory
     */
    public static UnsafeSliceFactory getInstance()
    {
        return INSTANCE;
    }

    private UnsafeSliceFactory() {}

    /**
     * Creates a slice for directly a raw memory address. This is
     * inherently unsafe as it may be used to access arbitrary memory.
     *
     * @param address the raw memory address base
     * @param size the size of the slice
     * @return the unsafe slice
     */
    public Slice newSlice(long address, int size)
    {
        if (address <= 0) {
            throw new IllegalArgumentException("Invalid address: " + address);
        }
        if (size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(null, address, size, 0, null);
    }

    /**
     * Creates a slice for directly a raw memory address. This is
     * inherently unsafe as it may be used to access arbitrary memory.
     * The slice will hold the specified object reference to prevent the
     * garbage collector from freeing it while it is in use by the slice.
     *
     * @param address the raw memory address base
     * @param size the size of the slice
     * @param reference the object reference
     * @return the unsafe slice
     */
    public Slice newSlice(long address, int size, Object reference)
    {
        if (address <= 0) {
            throw new IllegalArgumentException("Invalid address: " + address);
        }
        if (reference == null) {
            throw new NullPointerException("Object reference is null");
        }
        if (size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(null, address, size, size, reference);
    }

    public Slice newSlice(Object base, long address, int size)
    {
        return new Slice(base, address, size, size, null);
    }
}
