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

import com.google.common.primitives.Ints;
import org.testng.annotations.Test;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.security.Permission;

import static io.airlift.slice.JvmUtils.unsafe;
import static java.lang.Math.max;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUnsafeSliceFactory
{
    @SuppressWarnings("PublicField")
    private static class Foo
    {
        public int x;
        public int y;
    }

    @Test
    public void testBaseObject()
            throws Exception
    {
        Foo foo = new Foo();
        foo.x = 42;
        foo.y = 0xDEADBEEF;

        UnsafeSliceFactory factory = UnsafeSliceFactory.getInstance();

        int offsetX = factory.objectFieldOffset((Foo.class.getField("x")));
        int offsetY = factory.objectFieldOffset((Foo.class.getField("y")));
        int size = max(offsetX, offsetY) + 4;

        Slice slice = factory.newSlice(foo, 0, size);

        assertEquals(slice.getInt(offsetX), 42);
        assertEquals(slice.getInt(offsetY), 0xDEADBEEF);
    }

    @Test
    public void testRawAddress()
            throws Exception
    {
        UnsafeSliceFactory factory = UnsafeSliceFactory.getInstance();

        int size = 100;
        long address = unsafe.allocateMemory(size);
        try {
            Slice slice = factory.newSlice(address, size);
            for (int i = 0; i < size; i += Ints.BYTES) {
                slice.setInt(i, i);
            }
            for (int i = 0; i < size; i += Ints.BYTES) {
                assertEquals(slice.getInt(i), i);
            }
        }
        finally {
            unsafe.freeMemory(address);
        }
    }

    @Test
    public void testRawAddressWithReference()
            throws Exception
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        assertTrue(buffer instanceof DirectBuffer);
        long address = ((DirectBuffer) buffer).address();

        UnsafeSliceFactory factory = UnsafeSliceFactory.getInstance();

        Slice slice = factory.newSlice(address, buffer.capacity(), buffer);

        slice.setInt(32, 0xDEADBEEF);
        assertEquals(slice.getInt(32), 0xDEADBEEF);
    }

    @Test(expectedExceptions = SecurityException.class)
    public void testSecurity()
            throws Exception
    {
        SecurityManager saved = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager()
        {
            @Override
            public void checkPermission(Permission perm)
            {
                if (perm.getName().equals("suppressAccessChecks")) {
                    throw new SecurityException();
                }
            }
        });

        try {
            UnsafeSliceFactory.getInstance();
            fail("expected SecurityException");
        }
        finally {
            System.setSecurityManager(saved);
        }
    }
}
