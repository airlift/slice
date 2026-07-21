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
enum RegexpOp
{
    NO_MATCH,
    EMPTY_MATCH,
    LITERAL,
    LITERAL_STRING,
    CONCAT,
    ALTERNATE,
    STAR,
    PLUS,
    QUEST,
    REPEAT,
    CAPTURE,
    ANY_CHAR,
    ANY_BYTE,
    BEGIN_LINE,
    END_LINE,
    WORD_BOUNDARY,
    NO_WORD_BOUNDARY,
    BEGIN_TEXT,
    END_TEXT,
    CHAR_CLASS,
    HAVE_MATCH,
}
