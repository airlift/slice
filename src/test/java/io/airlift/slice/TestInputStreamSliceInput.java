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

import com.google.common.base.Charsets;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TestInputStreamSliceInput
{
    @Test
    public void testReadBytes()
            throws Exception
    {
        byte[] testBytes = "This is a test".getBytes(Charsets.UTF_8);
        InputStreamSliceInput in = new InputStreamSliceInput(new ByteArrayInputStream(testBytes));

        byte[] buffer = new byte[testBytes.length + 20];
        in.readBytes(buffer, 10, testBytes.length);

        assertEquals(Arrays.copyOfRange(buffer, 10, 10 + testBytes.length), testBytes);
    }
}
