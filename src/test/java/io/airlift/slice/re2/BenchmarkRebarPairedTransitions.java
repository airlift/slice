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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static java.lang.Math.toIntExact;

/** Measures repeated searches over exact Rebar inputs around the paired-table eligibility boundary. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRebarPairedTransitions
{
    private static final int MAX_SEARCH_CALLS = 100_000;
    private static final long FORWARD_MEMORY = (Re2.Options.DEFAULT_MAX_MEMORY / 3L) * 2;

    @Param({"LEIPZIG", "KEYWORDS", "URL", "NO_QUADRATIC"})
    String workload;

    @Param("false")
    boolean expectPairedTable;

    private Prog program;
    private Slice haystack;
    private boolean unicode;
    private long expectedResult;

    @Setup(Level.Trial)
    public void setup()
            throws IOException
    {
        Path corpusDirectory = Path.of(System.getProperty("re2.rebar.corpus", "target/rebar-selected"));
        WorkloadDefinition definition = switch (workload) {
            case "LEIPZIG" -> new WorkloadDefinition("imported_leipzig_tom-sawyer-huckle-fin-prefix-long.klv.gz", false);
            case "KEYWORDS" -> new WorkloadDefinition("reported_i787-keywords_ascii.klv.gz", false);
            case "URL" -> new WorkloadDefinition("wild_url_search.klv.gz", false);
            case "NO_QUADRATIC" -> new WorkloadDefinition("opt_reverse-inner_no-quadratic-forward.klv.gz", false);
            case "WORD_BOUNDARY" -> new WorkloadDefinition("unicode_word_boundary-any-english.klv.gz", true, true);
            case "WORD_ENDING" -> new WorkloadDefinition("imported_leipzig_word-ending-nn.klv.gz", true, false);
            case "ANY_CODE_POINT" -> new WorkloadDefinition("unicode_codepoints_any-one.klv.gz", true, true);
            case "BOUNDED_ENDING" -> new WorkloadDefinition("imported_leipzig_bounded-strings-ending-z.klv.gz", true, false);
            case "AROUND_HOLMES" -> new WorkloadDefinition("unicode_word_around-holmes-english.klv.gz", true, false);
            case "LINE_BOUNDARY" -> new WorkloadDefinition("imported_sherlock_line-boundary-sherlock-holmes.klv.gz", true, false);
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };
        Path inputFile = corpusDirectory.resolve(definition.fileName());

        RebarDfaCorpusAnalyzer.RebarBenchmark benchmark;
        try (InputStream input = new GZIPInputStream(Files.newInputStream(inputFile))) {
            benchmark = RebarDfaCorpusAnalyzer.readKlv(input);
        }
        if (benchmark.patterns().size() != 1) {
            throw new IllegalArgumentException("Benchmark requires exactly one pattern");
        }

        int parseFlags = Regexp.LIKE_PERL;
        if (!benchmark.unicode()) {
            parseFlags |= Regexp.LATIN1;
        }
        if (benchmark.caseInsensitive()) {
            parseFlags |= Regexp.FOLD_CASE;
        }
        ParseResult parsed = RegexpParser.parse(Slices.utf8Slice(benchmark.patterns().getFirst()), parseFlags);
        if (parsed.regexp().requiredPrefix() != null) {
            throw new IllegalArgumentException("Benchmark unexpectedly has a required prefix");
        }

        program = Compiler.compile(parsed.regexp(), false, FORWARD_MEMORY);
        haystack = Slices.wrappedBuffer(benchmark.haystack());
        unicode = benchmark.unicode();
        expectedResult = searchAll();

        Dfa.DfaInstance.Kind dfaKind = program.anchorEnd()
                ? Dfa.DfaInstance.Kind.LONGEST_MATCH
                : Dfa.DfaInstance.Kind.FIRST_MATCH;
        Dfa.DfaInstance dfa = program.getCachedDfa(dfaKind);
        if (!definition.completeTable() && dfa.estimatedPairedTransitionMemory() <= Dfa.MAX_PAIRED_TRANSITION_MEMORY) {
            throw new IllegalStateException("Benchmark DFA does not exceed the complete-table limit");
        }
        if (definition.completeTable() && dfa.estimatedPairedTransitionMemory() > Dfa.MAX_PAIRED_TRANSITION_MEMORY) {
            throw new IllegalStateException("Benchmark DFA exceeds the complete-table limit");
        }
        if (expectPairedTable) {
            if (definition.expectAdaptiveDisable()) {
                if (!dfa.pairedTransitionsDisabled() || dfa.pairedTransitionRowCount() != 0) {
                    throw new IllegalStateException("Benchmark did not disable an unproductive paired table");
                }
            }
            else if (dfa.pairedTransitionRowCount() != dfa.stateCount || dfa.pairedTransitionCount() == 0) {
                throw new IllegalStateException("Benchmark did not construct a usable paired table for " + workload);
            }
        }
        else if (dfa.pairedTransitionRowCount() != 0) {
            throw new IllegalStateException("Control unexpectedly constructed a paired table");
        }
    }

    @Benchmark
    public long searchAll()
    {
        int start = 0;
        int calls = 0;
        int matches = 0;
        while (start <= haystack.length() && calls < MAX_SEARCH_CALLS) {
            long matchEnd = Dfa.search(program, haystack, start, haystack.length(), false, Prog.MatchKind.FIRST_MATCH, true);
            calls++;
            if (matchEnd == Dfa.SEARCH_FAILED) {
                throw new IllegalStateException("DFA search failed");
            }
            if (matchEnd == Dfa.SEARCH_NO_MATCH) {
                break;
            }
            matches++;
            if (matchEnd > 0) {
                start += toIntExact(matchEnd);
            }
            else if (start < haystack.length()) {
                start = nextCodePointBoundary(start);
            }
            else {
                break;
            }
        }
        return ((long) calls << 32) | (matches & 0xFFFF_FFFFL);
    }

    private int nextCodePointBoundary(int position)
    {
        if (!unicode) {
            return position + 1;
        }
        int firstByte = haystack.getUnsignedByte(position);
        int codePointBytes;
        if (firstByte < 0x80) {
            codePointBytes = 1;
        }
        else if (firstByte < 0xE0) {
            codePointBytes = 2;
        }
        else if (firstByte < 0xF0) {
            codePointBytes = 3;
        }
        else {
            codePointBytes = 4;
        }
        return Math.min(position + codePointBytes, haystack.length());
    }

    long expectedResult()
    {
        return expectedResult;
    }

    private record WorkloadDefinition(String fileName, boolean completeTable, boolean expectAdaptiveDisable)
    {
        private WorkloadDefinition(String fileName, boolean completeTable)
        {
            this(fileName, completeTable, false);
        }
    }
}
