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

import java.util.stream.Stream;

import static java.lang.Math.toIntExact;
import static org.openjdk.jol.info.ClassLayout.parseClass;

public final class InstanceSize
{
    private InstanceSize() {}

    public static int instanceSize(Class<?>... classes)
    {
        return Stream.of(classes)
                .mapToInt(clazz -> toIntExact(parseClass(clazz).instanceSize()))
                .reduce(0, Math::addExact);
    }
}
