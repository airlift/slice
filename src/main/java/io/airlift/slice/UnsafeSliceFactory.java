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

import javax.annotation.Nullable;

/**
 * A slice factory for creating unsafe slices
 */
public class UnsafeSliceFactory {
    /**
     * Accessible only to the privileged code
     */
    static UnsafeSliceFactory instance = new UnsafeSliceFactory();

    /**
     * Get an instance of this factory
     * @return an instnace of this factory
     */
    public static UnsafeSliceFactory getInstance() { return instance; }

    /**
     * Hidden constructor
     */
    UnsafeSliceFactory() {}

    /**
     * Creates a slice for directly accessing the base object.
     */
    public Slice newSlice(@Nullable Object base, long address, int size, @Nullable Object reference)
    {
        if(size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(base, address, size, reference);
    }
}
