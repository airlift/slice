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

import java.lang.instrument.Instrumentation;

public final class ObjectSizeAgent
{
    private static Instrumentation instrumentation;

    private ObjectSizeAgent() {}

    public static void premain(String agentArguments, Instrumentation instrumentation)
    {
        ObjectSizeAgent.instrumentation = instrumentation;
    }

    static long getObjectSize(Object object)
    {
        if (instrumentation == null) {
            throw new IllegalStateException("Object size agent is not loaded; run tests with Maven or configure -javaagent");
        }

        // This API measures shallow size and specifies an implementation-specific approximation.
        // https://docs.oracle.com/en/java/javase/25/docs/api/java.instrument/java/lang/instrument/Instrumentation.html#getObjectSize(java.lang.Object)
        // In HotSpot, libinstrument delegates to JVMTI GetObjectSize, which returns mirror->size() * wordSize.
        // https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.instrument/share/native/libinstrument/JPLISAgent.c#L1456-L1470
        // https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/hotspot/share/prims/jvmtiEnv.cpp#L508-L515
        return instrumentation.getObjectSize(object);
    }
}
