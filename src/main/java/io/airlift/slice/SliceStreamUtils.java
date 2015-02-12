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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class SliceStreamUtils
{
    private SliceStreamUtils()
    {
    }

    public static void copyStreamFully(InputStream in, OutputStream out, int length)
            throws IOException
    {
        byte[] bytes = new byte[4096];
        while (length > 0) {
            int newBytes = in.read(bytes, 0, Math.min(bytes.length, length));
            if (newBytes < 0) {
                throw new EOFException();
            }
            out.write(bytes, 0, newBytes);
            length -= newBytes;
        }
    }
}
