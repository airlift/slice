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
