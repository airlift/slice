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

final class EmptyOp
{
    public static final int EMPTY_BEGIN_LINE = 1 << 0;
    public static final int EMPTY_END_LINE = 1 << 1;
    public static final int EMPTY_BEGIN_TEXT = 1 << 2;
    public static final int EMPTY_END_TEXT = 1 << 3;
    public static final int EMPTY_WORD_BOUNDARY = 1 << 4;
    public static final int EMPTY_NO_WORD_BOUNDARY = 1 << 5;

    private EmptyOp() {}
}
