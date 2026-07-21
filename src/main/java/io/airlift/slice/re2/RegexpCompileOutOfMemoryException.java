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

public class RegexpCompileOutOfMemoryException
        extends RegexpCompileException
{
    private final long maxMemoryBytes;

    public RegexpCompileOutOfMemoryException(long maxMemoryBytes)
    {
        super(buildMessage(maxMemoryBytes));
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public long maxMemoryBytes()
    {
        return maxMemoryBytes;
    }

    private static String buildMessage(long maxMemoryBytes)
    {
        if (maxMemoryBytes > 0) {
            return "PATTERN_TOO_LARGE for maxMemory=" + maxMemoryBytes;
        }
        return "PATTERN_TOO_LARGE";
    }
}
