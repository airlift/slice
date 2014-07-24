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

import java.lang.reflect.Field;
import java.lang.reflect.ReflectPermission;
import java.security.Permission;

import static io.airlift.slice.JvmUtils.unsafe;

/**
 * A slice factory for creating unsafe slices
 */
public class UnsafeSliceFactory
{
    /**
     * The Permission object that is used to check whether a client has
     * sufficient privilege to defeat Java language access control checks.
     * This is the same permission used by {@link java.lang.reflect.AccessibleObject}.
     */
    private static final Permission ACCESS_PERMISSION = new ReflectPermission("suppressAccessChecks");

    /**
     * Accessible only to the privileged code.
     */
    private static final UnsafeSliceFactory INSTANCE = new UnsafeSliceFactory();

    /**
     * Get a factory for creating "unsafe" slices that can reference
     * arbitrary memory addresses. If there is a security manager, its
     * {@code checkPermission} method is called with a
     * {@code ReflectPermission("suppressAccessChecks")} permission.
     *
     * @return an unsafe slice factory
     */
    @SuppressWarnings("JavaDoc") // IDEA-81310
    public static UnsafeSliceFactory getInstance()
    {
        // see setAccessible() in AccessibleObject
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(ACCESS_PERMISSION);
        }
        return INSTANCE;
    }

    private UnsafeSliceFactory() {}

    /**
     * Creates a slice for directly accessing a base object.
     * This is inherently unsafe as it may be used to update arbitrary
     * memory within the object (including object references).
     *
     * @param base the base object
     * @param offset the offset from the start of the object
     * @param size the size of the slice (should be no larger than the object)
     * @return the unsafe slice
     */
    public Slice newSlice(Object base, long offset, int size)
    {
        if (size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(base, offset, size, null);
    }

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
        return new Slice(null, address, size, null);
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
        return new Slice(null, address, size, reference);
    }

    /**
     * Get the offset of a field within an object.
     *
     * @param field the field reference
     * @return the offset for use with an unsafe slice
     */
    public int objectFieldOffset(Field field)
    {
        long offset = unsafe.objectFieldOffset(field);
        if (offset < 0) {
            throw new IllegalArgumentException("Field offset is negative: " + offset);
        }
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Field offset is larger than an int: " + offset);
        }
        return (int) offset;
    }
}
