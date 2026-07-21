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

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public class RegexpParseException
        extends IllegalArgumentException
{
    private final RegexpStatusCode statusCode;
    private final Slice errorArgument;
    private final int byteOffset;

    public RegexpParseException(RegexpStatusCode statusCode, Slice errorArgument, int byteOffset)
    {
        super(buildMessage(statusCode, byteOffset));
        this.statusCode = requireNonNull(statusCode, "statusCode is null");
        this.errorArgument = errorArgument;
        this.byteOffset = byteOffset;
    }

    public RegexpStatusCode statusCode()
    {
        return statusCode;
    }

    public Slice errorArgument()
    {
        return errorArgument;
    }

    public int byteOffset()
    {
        return byteOffset;
    }

    private static String buildMessage(RegexpStatusCode statusCode, int byteOffset)
    {
        String message = requireNonNull(statusCode, "statusCode is null").name();
        if (byteOffset >= 0) {
            return message + " at byte offset " + byteOffset;
        }
        return message;
    }
}
