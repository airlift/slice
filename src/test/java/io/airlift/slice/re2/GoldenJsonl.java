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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GoldenJsonl
{
    private GoldenJsonl() {}

    public static List<JsonObject> readObjects(String resourcePath)
    {
        requireNonNull(resourcePath, "resourcePath is null");
        InputStream stream = GoldenJsonl.class.getClassLoader().getResourceAsStream(resourcePath);
        if (stream == null) {
            throw new IllegalArgumentException("resource not found: " + resourcePath);
        }

        List<JsonObject> objects = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                objects.add(parseLine(resourcePath, lineNo, line));
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("failed to read resource: " + resourcePath, e);
        }
        return objects;
    }

    private static JsonObject parseLine(String resourcePath, int lineNo, String line)
    {
        int index = skipWhitespace(line, 0);
        if (index >= line.length() || line.charAt(index) != '{') {
            throw error(resourcePath, lineNo, "expected '{'", line);
        }
        index++;

        Map<String, Object> values = new LinkedHashMap<>();
        while (true) {
            index = skipWhitespace(line, index);
            if (index >= line.length()) {
                throw error(resourcePath, lineNo, "unexpected end of line", line);
            }
            if (line.charAt(index) == '}') {
                index++;
                break;
            }

            ParsedString key = parseString(line, index, resourcePath, lineNo);
            index = skipWhitespace(line, key.nextIndex());
            if (index >= line.length() || line.charAt(index) != ':') {
                throw error(resourcePath, lineNo, "expected ':'", line);
            }
            index++;
            index = skipWhitespace(line, index);

            ParsedValue value = parseValue(line, index, resourcePath, lineNo);
            values.put(key.value(), value.value());
            index = skipWhitespace(line, value.nextIndex());

            if (index >= line.length()) {
                throw error(resourcePath, lineNo, "unexpected end of line", line);
            }
            char c = line.charAt(index);
            if (c == ',') {
                index++;
                continue;
            }
            if (c == '}') {
                index++;
                break;
            }
            throw error(resourcePath, lineNo, "expected ',' or '}'", line);
        }

        index = skipWhitespace(line, index);
        if (index != line.length()) {
            throw error(resourcePath, lineNo, "unexpected trailing content", line);
        }

        return new JsonObject(values);
    }

    private static ParsedValue parseValue(String line, int index, String resourcePath, int lineNo)
    {
        if (index >= line.length()) {
            throw error(resourcePath, lineNo, "unexpected end of line", line);
        }
        char c = line.charAt(index);
        if (c == '"') {
            ParsedString parsed = parseString(line, index, resourcePath, lineNo);
            return new ParsedValue(parsed.value(), parsed.nextIndex());
        }
        if (line.startsWith("true", index)) {
            return new ParsedValue(Boolean.TRUE, index + 4);
        }
        if (line.startsWith("false", index)) {
            return new ParsedValue(Boolean.FALSE, index + 5);
        }
        if (line.startsWith("null", index)) {
            return new ParsedValue(null, index + 4);
        }

        throw error(resourcePath, lineNo, "unexpected value", line);
    }

    private static ParsedString parseString(String line, int index, String resourcePath, int lineNo)
    {
        if (index >= line.length() || line.charAt(index) != '"') {
            throw error(resourcePath, lineNo, "expected string", line);
        }
        index++;

        StringBuilder out = new StringBuilder();
        while (index < line.length()) {
            char c = line.charAt(index++);
            if (c == '"') {
                return new ParsedString(out.toString(), index);
            }
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (index >= line.length()) {
                throw error(resourcePath, lineNo, "unterminated escape", line);
            }
            char esc = line.charAt(index++);
            switch (esc) {
                case '"' -> out.append('"');
                case '\\' -> out.append('\\');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (index + 4 > line.length()) {
                        throw error(resourcePath, lineNo, "invalid unicode escape", line);
                    }
                    int code = parseHex(line.substring(index, index + 4), resourcePath, lineNo, line);
                    out.append((char) code);
                    index += 4;
                }
                default -> throw error(resourcePath, lineNo, "unsupported escape", line);
            }
        }

        throw error(resourcePath, lineNo, "unterminated string", line);
    }

    private static int parseHex(String text, String resourcePath, int lineNo, String line)
    {
        try {
            return Integer.parseInt(text, 16);
        }
        catch (NumberFormatException e) {
            throw error(resourcePath, lineNo, "invalid hex escape", line);
        }
    }

    private static int skipWhitespace(String line, int index)
    {
        while (index < line.length()) {
            char c = line.charAt(index);
            if (c != ' ' && c != '\t') {
                break;
            }
            index++;
        }
        return index;
    }

    private static IllegalArgumentException error(String resourcePath, int lineNo, String message, String line)
    {
        return new IllegalArgumentException(resourcePath + ":" + lineNo + ": " + message + " -> " + line);
    }

    public static final class JsonObject
    {
        private final Map<String, Object> values;

        private JsonObject(Map<String, Object> values)
        {
            this.values = Collections.unmodifiableMap(new LinkedHashMap<>(requireNonNull(values, "values is null")));
        }

        public String getString(String key)
        {
            Object value = values.get(key);
            if (value == null) {
                return null;
            }
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("expected string for " + key + ": " + value);
            }
            return (String) value;
        }

        public boolean getBoolean(String key)
        {
            Object value = values.get(key);
            if (!(value instanceof Boolean)) {
                throw new IllegalArgumentException("expected boolean for " + key + ": " + value);
            }
            return (Boolean) value;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof JsonObject)) {
                return false;
            }
            JsonObject that = (JsonObject) other;
            return values.equals(that.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(values);
        }

        @Override
        public String toString()
        {
            return values.toString();
        }
    }

    private record ParsedString(String value, int nextIndex) {}

    private record ParsedValue(Object value, int nextIndex) {}
}
