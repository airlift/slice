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
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestCompilerDotRegression
{
    @Test
    public void testCompilingRepeatedUtf8DotIsLinear()
    {
        // This used to hang while compiling "^.........$" (nine dots) in UTF-8 mode.
        assertCompilesQuickly("^.$", Regexp.LIKE_PERL, Duration.ofSeconds(1));
        assertCompilesQuickly("^..$", Regexp.LIKE_PERL, Duration.ofSeconds(1));
        assertCompilesQuickly("^...$", Regexp.LIKE_PERL, Duration.ofSeconds(1));
        assertCompilesQuickly("^....$", Regexp.LIKE_PERL, Duration.ofSeconds(1));
        assertCompilesQuickly("^.........$", Regexp.LIKE_PERL, Duration.ofSeconds(1));
    }

    private static void assertCompilesQuickly(String regexp, int parseFlags, Duration timeout)
    {
        Slice bytes = Slices.wrappedBuffer(regexp.getBytes(UTF_8));
        ParseResult parsed = RegexpParser.parse(bytes, parseFlags);

        Prog p = compileWithTimeout(parsed.regexp(), false, 0, timeout);
        assertThat(p).isNotNull();
        assertThat(p.size()).isGreaterThan(0);
    }

    private static Prog compileWithTimeout(Regexp re, boolean reversed, long maxMemory, Duration timeout)
    {
        final Prog[] result = new Prog[1];
        final Throwable[] error = new Throwable[1];

        Thread t = new Thread(() -> {
            try {
                result[0] = Compiler.compile(re, reversed, maxMemory);
            }
            catch (Throwable e) {
                error[0] = e;
            }
        }, "re2-compile-test");
        t.setDaemon(true);
        t.start();

        try {
            t.join(timeout.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        if (t.isAlive()) {
            StackTraceElement[] trace = t.getStackTrace();
            StringBuilder builder = new StringBuilder();
            builder.append("compile timed out after ").append(timeout).append('\n');
            for (StackTraceElement el : trace) {
                builder.append("  at ").append(el).append('\n');
            }
            fail(builder.toString());
        }
        if (error[0] != null) {
            throw new RuntimeException(error[0]);
        }
        return result[0];
    }
}
