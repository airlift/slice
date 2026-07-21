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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static java.util.Objects.requireNonNull;

abstract class RegexpWalker<T>
{
    private static final int DEFAULT_MAX_VISITS = 1_000_000;

    /**
     * Called before children are visited. May signal {@code stop=true} to bypass recursion.
     */
    protected abstract PreVisitResult<T> preVisit(Regexp regexp, T parentValue);

    /**
     * Called after children are visited (or immediately if {@code stop=true}).
     */
    protected abstract T postVisit(Regexp regexp, T parentValue, T preValue, List<T> childValues);

    protected T copy(T value)
    {
        return value;
    }

    /**
     * Called when max visit count is exceeded in {@link #walkExponential}.
     */
    protected abstract T shortVisit(Regexp regexp, T parentValue);

    public final T walk(Regexp regexp, T initialValue)
    {
        return walkInternal(regexp, initialValue, true, false, DEFAULT_MAX_VISITS);
    }

    public final T walkExponential(Regexp regexp, T initialValue, int maxVisits)
    {
        if (maxVisits <= 0) {
            throw new IllegalArgumentException("maxVisits must be > 0");
        }
        return walkInternal(regexp, initialValue, false, true, maxVisits);
    }

    private T walkInternal(Regexp regexp, T initialValue, boolean copyDuplicateSiblings, boolean allowVisitLimitFallback, int maxVisits)
    {
        requireNonNull(regexp, "regexp is null");

        int visitCount = 0;
        Deque<State<T>> stack = new ArrayDeque<>();
        stack.addLast(new State<>(regexp, initialValue));

        while (!stack.isEmpty()) {
            State<T> state = stack.getLast();
            if (!state.preVisitCompleted) {
                if (++visitCount > maxVisits) {
                    if (!allowVisitLimitFallback) {
                        throw new IllegalStateException("RegexpWalker exceeded maxVisits: " + maxVisits);
                    }
                    state.result = shortVisit(state.regexp, state.parentValue);
                    stack.removeLast();
                    if (stack.isEmpty()) {
                        return state.result;
                    }
                    stack.getLast().childValues.set(state.childIndexInParent, state.result);
                    continue;
                }

                state.preVisitCompleted = true;
                PreVisitResult<T> preVisitResult = preVisit(state.regexp, state.parentValue);
                state.preVisitValue = requireNonNull(preVisitResult, "preVisit result is null").value();
                state.skipChildren = preVisitResult.stop();

                if (state.skipChildren || state.regexp.subCount() == 0) {
                    state.result = postVisit(state.regexp, state.parentValue, state.preVisitValue, List.of());
                    stack.removeLast();
                    if (stack.isEmpty()) {
                        return state.result;
                    }
                    stack.getLast().childValues.set(state.childIndexInParent, state.result);
                    continue;
                }

                state.childValues = new ArrayList<>(state.regexp.subCount());
                for (int childIndex = 0; childIndex < state.regexp.subCount(); childIndex++) {
                    state.childValues.add(null);
                }
            }

            int childIndex = state.nextChildIndex;
            if (childIndex >= state.regexp.subCount()) {
                state.result = postVisit(state.regexp, state.parentValue, state.preVisitValue, state.childValues);
                stack.removeLast();
                if (stack.isEmpty()) {
                    return state.result;
                }
                stack.getLast().childValues.set(state.childIndexInParent, state.result);
                continue;
            }

            Regexp child = state.regexp.sub(childIndex);
            T childValue = state.preVisitValue;
            if (copyDuplicateSiblings && childIndex > 0 && child == state.regexp.sub(childIndex - 1)) {
                childValue = copy(childValue);
            }

            state.nextChildIndex++;

            State<T> childState = new State<>(child, childValue);
            childState.childIndexInParent = childIndex;
            stack.addLast(childState);
        }

        throw new IllegalStateException("unreachable");
    }

    public record PreVisitResult<T>(T value, boolean stop) {}

    private static final class State<T>
    {
        final Regexp regexp;
        final T parentValue;

        boolean preVisitCompleted;
        boolean skipChildren;
        T preVisitValue;

        int nextChildIndex;
        int childIndexInParent;

        List<T> childValues;
        T result;

        State(Regexp regexp, T parentValue)
        {
            this.regexp = regexp;
            this.parentValue = parentValue;
        }
    }
}
