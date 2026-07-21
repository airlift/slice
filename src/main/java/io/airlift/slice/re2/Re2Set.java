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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Searches for multiple patterns simultaneously with one many-match DFA traversal.
 */
public final class Re2Set
{
    private final Re2.Options options;
    private final Re2.Anchor anchor;
    private final List<Regexp> elements = new ArrayList<>();
    private boolean compiled;
    private int size;
    private Prog program;

    public Re2Set(Re2.Options options, Re2.Anchor anchor)
    {
        this.options = requireNonNull(options, "options is null")
                .copy()
                .setNeverCapture(true);
        this.anchor = requireNonNull(anchor, "anchor is null");
    }

    public int add(Slice pattern)
    {
        requireNonNull(pattern, "pattern is null");
        if (compiled) {
            throw new IllegalStateException("Re2Set.add() called after compiling");
        }

        int parseFlags = options.parseFlags();
        Slice copiedPattern = pattern.copy();
        ParseResult result = RegexpParser.parse(copiedPattern, parseFlags);

        int regexpIndex = elements.size();
        Regexp regexp = result.regexp();
        Regexp matchMarkerRegexp = Regexp.haveMatch(parseFlags, regexpIndex);

        Regexp combined;
        if (regexp.op() == RegexpOp.CONCAT) {
            List<Regexp> subexpressions = new ArrayList<>(regexp.subCount() + 1);
            for (int i = 0; i < regexp.subCount(); i++) {
                subexpressions.add(regexp.sub(i));
            }
            subexpressions.add(matchMarkerRegexp);
            combined = Regexp.concat(parseFlags, subexpressions);
        }
        else {
            combined = Regexp.concat(parseFlags, List.of(regexp, matchMarkerRegexp));
        }

        elements.add(combined);
        return regexpIndex;
    }

    public int size()
    {
        if (!compiled) {
            return elements.size();
        }
        return size;
    }

    public void compile()
    {
        if (compiled) {
            throw new IllegalStateException("Re2Set.compile() called more than once");
        }
        compiled = true;
        size = elements.size();

        if (size == 0) {
            program = null;
            elements.clear();
            return;
        }

        int parseFlags = options.parseFlags();
        Regexp regexp;
        if (size == 1) {
            regexp = elements.getFirst();
        }
        else {
            regexp = Regexp.alternate(parseFlags, elements);
        }
        elements.clear();

        program = Compiler.compileSet(
                regexp,
                anchor == Re2.Anchor.UNANCHORED,
                anchor == Re2.Anchor.ANCHOR_BOTH,
                options.maxMemory());
        if (program.getCachedDfa(Dfa.DfaInstance.Kind.MANY_MATCH) == null) {
            throw new RegexpCompileOutOfMemoryException(options.maxMemory());
        }
    }

    public boolean match(Slice text)
    {
        return match(text, null);
    }

    public boolean match(Slice text, List<Integer> matches)
    {
        requireNonNull(text, "text is null");
        if (!compiled) {
            throw new IllegalStateException("Re2Set.match() called before compiling");
        }

        if (matches != null) {
            matches.clear();
        }

        if (program == null) {
            return false;
        }

        SparseSet matchSet = (matches != null) ? new SparseSet(size) : null;
        boolean matched = Dfa.searchMany(program, text, matchSet);

        if (!matched) {
            return false;
        }

        if (matches != null && matchSet != null) {
            if (matchSet.isEmpty()) {
                return false;
            }
            for (int i = 0; i < matchSet.size(); i++) {
                matches.add(matchSet.denseAt(i));
            }
        }

        return true;
    }
}
