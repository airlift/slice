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

import java.util.Random;

final class Re2Random
{
    private final Random random;

    Re2Random(int seed)
    {
        this.random = new Random(seed);
    }

    void reset(int seed)
    {
        random.setSeed(seed);
    }

    int uniform(int bound)
    {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive");
        }
        return random.nextInt(bound);
    }

    @Override
    public String toString()
    {
        return random.toString();
    }
}
