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

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Thread)
public class BenchmarkData
{
    @Param({
            "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072", "262144", "524288", "1048576",
            "3", "7", "13", "29", "127", "251", "509", "997", "4211", "8081", "16411", "32779" // prime numbers
    })
    public int size;

    private byte[] bytes;
    private Slice slice;

    @Setup
    public void setup()
    {
        bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        slice = Slices.wrappedBuffer(bytes);
    }

    public Slice getSlice()
    {
        return slice;
    }

    public byte[] getBytes()
    {
        return bytes;
    }
}
