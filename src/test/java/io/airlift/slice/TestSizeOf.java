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

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSizeOf
{
    @Test
    public void test()
    {
        // Generally, instance size delegates to JOL
        assertThat(instanceSize(BasicClass.class)).isEqualTo(toIntExact(ClassLayout.parseClass(BasicClass.class).instanceSize()));

        // JOL can not calculate the size of a record or a hidden class
        // If this changes, we may be able to remove the fallback code
        assertThatThrownBy(() -> ClassLayout.parseClass(BasicRecord.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Cannot get the field offset, try with -Djol.magicFieldOffset=true");

        // instanceSize estimates instance size when JOL fails
        assertThat(instanceSize(BasicRecord.class)).isEqualTo(instanceSize(BasicClass.class));
    }

    @SuppressWarnings("unused")
    private static class BasicClass
    {
        private String a;
        private int b;
        private long c;
        private boolean d;
    }

    @SuppressWarnings("unused")
    private record BasicRecord(String a, int b, long c, boolean d) {}
}
