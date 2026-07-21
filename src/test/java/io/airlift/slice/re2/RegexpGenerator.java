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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class RegexpGenerator
{
    private static final Slice WRAP_PREFIX = Slices.wrappedBuffer("(?:".getBytes(UTF_8));
    private static final Slice WRAP_SUFFIX = Slices.wrappedBuffer(")".getBytes(UTF_8));
    private static final byte[] TEMPLATE_PLACEHOLDER = new byte[] {'%', 's'};

    private static final Slice ANCHOR_PREFIX = Slices.wrappedBuffer("^(?:".getBytes(UTF_8));
    private static final Slice ANCHOR_SUFFIX = Slices.wrappedBuffer(")$".getBytes(UTF_8));
    private static final Slice ANCHOR_START_PREFIX = Slices.wrappedBuffer("^(?:".getBytes(UTF_8));
    private static final Slice ANCHOR_START_SUFFIX = Slices.wrappedBuffer(")".getBytes(UTF_8));
    private static final Slice ANCHOR_END_PREFIX = Slices.wrappedBuffer("(?:".getBytes(UTF_8));
    private static final Slice ANCHOR_END_SUFFIX = Slices.wrappedBuffer(")$".getBytes(UTF_8));

    private static final Slice SPACE = Slices.wrappedBuffer(" ".getBytes(UTF_8));

    private final List<Slice> atoms;
    private final List<Slice> ops;
    private final int maxAtoms;
    private final int maxOps;

    private final Re2Random random = new Re2Random(0);

    protected RegexpGenerator(String atomsUtf8, String opsUtf8, int maxAtoms, int maxOps)
    {
        requireNonNull(atomsUtf8, "atomsUtf8 is null");
        requireNonNull(opsUtf8, "opsUtf8 is null");
        this.atoms = split(StringGenerator.explodeUtf8(Slices.wrappedBuffer(atomsUtf8.getBytes(UTF_8))), SPACE);
        this.ops = split(StringGenerator.explodeUtf8(Slices.wrappedBuffer(opsUtf8.getBytes(UTF_8))), SPACE);
        this.maxAtoms = maxAtoms;
        this.maxOps = maxOps;
    }

    protected RegexpGenerator(List<Slice> atoms, List<Slice> ops, int maxAtoms, int maxOps)
    {
        this.atoms = List.copyOf(requireNonNull(atoms, "atoms is null"));
        this.ops = List.copyOf(requireNonNull(ops, "ops is null"));
        this.maxAtoms = maxAtoms;
        this.maxOps = maxOps;
    }

    public static List<Slice> egrepOps()
    {
        // Parens are added around operator results.
        String raw = "%s%s %s|%s %s* %s+ %s? %s\\\\C*";
        List<Slice> exploded = StringGenerator.explodeUtf8(Slices.wrappedBuffer(raw.getBytes(UTF_8)));
        return split(exploded, SPACE);
    }

    public void generate()
    {
        generatePostfix(new ArrayList<>(), 0, 0, 0);
    }

    public void generateRandom(int seed, int count)
    {
        random.reset(seed);
        for (int i = 0; i < count; i++) {
            List<Slice> postfix = new ArrayList<>();
            generateRandomPostfix(postfix, 0, 0, 0);
        }
    }

    protected abstract void handleRegexp(Slice regexp);

    private void generatePostfix(List<Slice> postfix, int nstk, int atoms, int ops)
    {
        if (nstk == 1) {
            runPostfix(postfix);
        }

        if (atoms < maxAtoms) {
            for (Slice atom : this.atoms) {
                postfix.add(atom);
                generatePostfix(postfix, nstk + 1, atoms + 1, ops);
                postfix.removeLast();
            }
        }

        if (nstk >= 1 && ops < maxOps) {
            for (Slice op : this.ops) {
                int args = countArgs(op);
                if (args <= nstk) {
                    postfix.add(op);
                    generatePostfix(postfix, nstk - args + 1, atoms, ops + 1);
                    postfix.removeLast();
                }
            }
        }
    }

    private void generateRandomPostfix(List<Slice> postfix, int nstk, int atoms, int opsCount)
    {
        if (nstk == 1 && random.uniform(maxAtoms + 1 - atoms) == 0) {
            runPostfix(postfix);
            return;
        }

        boolean canAtom = atoms < maxAtoms;
        boolean canOp = nstk >= 1 && opsCount < maxOps;
        if (!canAtom && !canOp) {
            // Give up early; no valid expression.
            return;
        }

        if (canAtom && (!canOp || random.uniform(2) == 0)) {
            Slice atom = this.atoms.get(random.uniform(this.atoms.size()));
            postfix.add(atom);
            generateRandomPostfix(postfix, nstk + 1, atoms + 1, opsCount);
            postfix.removeLast();
            return;
        }

        // Operator.
        Slice op = this.ops.get(random.uniform(this.ops.size()));
        int args = countArgs(op);
        if (args > nstk) {
            // Not enough stack arguments; try again.
            generateRandomPostfix(postfix, nstk, atoms, opsCount);
            return;
        }

        postfix.add(op);
        generateRandomPostfix(postfix, nstk - args + 1, atoms, opsCount + 1);
        postfix.removeLast();
    }

    private void runPostfix(List<Slice> postfix)
    {
        Deque<Slice> stack = new ArrayDeque<>();
        for (Slice token : postfix) {
            int args = countArgs(token);
            if (args == 0) {
                stack.addLast(token);
                continue;
            }

            if (args == 1) {
                Slice operand = stack.removeLast();
                byte[] formatted = applyTemplate(token, List.of(operand));
                stack.addLast(Slices.wrappedBuffer(ByteArrays.concatAll(List.of(WRAP_PREFIX, Slices.wrappedBuffer(formatted), WRAP_SUFFIX))));
                continue;
            }

            if (args == 2) {
                Slice rightOperand = stack.removeLast();
                Slice leftOperand = stack.removeLast();
                byte[] formatted = applyTemplate(token, List.of(leftOperand, rightOperand));
                stack.addLast(Slices.wrappedBuffer(ByteArrays.concatAll(List.of(WRAP_PREFIX, Slices.wrappedBuffer(formatted), WRAP_SUFFIX))));
                continue;
            }

            throw new IllegalStateException("unsupported arg count: " + args);
        }

        if (stack.size() != 1) {
            throw new IllegalStateException("bad postfix expression, stack size: " + stack.size());
        }

        Slice expr = stack.removeLast();
        handleRegexp(expr);
        handleRegexp(Slices.wrappedBuffer(ByteArrays.concat(ANCHOR_PREFIX, expr, ANCHOR_SUFFIX)));
        handleRegexp(Slices.wrappedBuffer(ByteArrays.concat(ANCHOR_START_PREFIX, expr, ANCHOR_START_SUFFIX)));
        handleRegexp(Slices.wrappedBuffer(ByteArrays.concat(ANCHOR_END_PREFIX, expr, ANCHOR_END_SUFFIX)));
    }

    private static int countArgs(Slice template)
    {
        byte[] bytes = template.byteArray();
        int offset = template.byteArrayOffset();
        int end = offset + template.length();
        int count = 0;
        for (int i = offset; i + 1 < end; i++) {
            if (bytes[i] == TEMPLATE_PLACEHOLDER[0] && bytes[i + 1] == TEMPLATE_PLACEHOLDER[1]) {
                count++;
            }
        }
        return count;
    }

    private static byte[] applyTemplate(Slice template, List<Slice> args)
    {
        int expectedArgs = countArgs(template);
        if (expectedArgs != args.size()) {
            throw new IllegalStateException("expected " + expectedArgs + " args, got " + args.size());
        }

        byte[] t = template.byteArray();
        int off = template.byteArrayOffset();
        int end = off + template.length();

        int size = 0;
        int argIndex = 0;
        for (int i = off; i < end; i++) {
            if (i + 1 < end && t[i] == TEMPLATE_PLACEHOLDER[0] && t[i + 1] == TEMPLATE_PLACEHOLDER[1]) {
                size += args.get(argIndex++).length();
                i++;
            }
            else {
                size++;
            }
        }

        byte[] out = new byte[size];
        int outputPosition = 0;
        argIndex = 0;
        for (int i = off; i < end; i++) {
            if (i + 1 < end && t[i] == TEMPLATE_PLACEHOLDER[0] && t[i + 1] == TEMPLATE_PLACEHOLDER[1]) {
                Slice arg = args.get(argIndex++);
                System.arraycopy(arg.byteArray(), arg.byteArrayOffset(), out, outputPosition, arg.length());
                outputPosition += arg.length();
                i++;
            }
            else {
                out[outputPosition++] = t[i];
            }
        }
        return out;
    }

    private static List<Slice> split(List<Slice> utf8Chars, Slice separator)
    {
        requireNonNull(utf8Chars, "utf8Chars is null");
        requireNonNull(separator, "separator is null");

        if (utf8Chars.isEmpty()) {
            return List.of();
        }

        // Implemented over an exploded list to make separator comparisons easy.
        List<Slice> out = new ArrayList<>();
        int i = 0;
        while (i < utf8Chars.size()) {
            // Skip repeated separators.
            while (i < utf8Chars.size() && utf8Chars.get(i).equals(separator)) {
                i++;
            }
            if (i >= utf8Chars.size()) {
                break;
            }

            int start = i;
            while (i < utf8Chars.size() && !utf8Chars.get(i).equals(separator)) {
                i++;
            }
            out.add(Slices.wrappedBuffer(ByteArrays.concatAll(utf8Chars.subList(start, i))));
        }

        return out;
    }
}
