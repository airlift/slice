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

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Thread)
public class BenchmarkData
{
    private byte[] bytes;
    private Slice slice;
    private long value;

    @Setup
    public void setup()
    {
        bytes = new byte[1024 * 1024]; // 1 MB
        ThreadLocalRandom.current().nextBytes(bytes);
        slice = Slices.wrappedBuffer(bytes);

        value = ThreadLocalRandom.current().nextLong();
    }

    public Slice getSlice()
    {
        return slice;
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    public long getLong()
    {
        return value;
    }
}
