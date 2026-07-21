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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.Arrays;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.re2.Re2BenchmarkRunner.buildOptions;
import static io.airlift.slice.re2.Re2BenchmarkRunner.compileProg;
import static io.airlift.slice.re2.Re2BenchmarkRunner.randomText;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkDfaCache
{
    private static final String HARD = "[ -~]*ABCDEFGHIJKLMNOPQRSTUVWXYZ$";
    private static final String LATE_TRANSITION = "^a*bc$";

    @State(Scope.Benchmark)
    public static class SharedWarmState
    {
        @Param({"8", "262144"})
        int textSize;

        private Slice text;
        private Prog program;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer(randomText(textSize));
            program = compileProg(HARD);
            search(program, text, false);
        }
    }

    @State(Scope.Thread)
    public static class ColdStartState
    {
        @Param({"8", "262144"})
        int textSize;

        private Slice text;
        private Prog program;
        private Dfa.DfaInstance dfa;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer(randomText(textSize));
            program = compileProg(HARD);
            search(program, text, false);
            dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
        }

        @Setup(Level.Invocation)
        public void resetCache()
        {
            dfa.resetCacheExternal();
        }
    }

    @State(Scope.Thread)
    public static class SharedColdState
    {
        @Param({"1", "2", "4", "8", "16"})
        int workerCount;

        @Param({"8", "262144"})
        int textSize;

        private Slice text;
        private Prog program;
        private Dfa.DfaInstance dfa;
        private Phaser phaser;
        private Thread[] workers;
        private long[] results;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private volatile boolean isShutdown;

        @Setup(Level.Trial)
        public void setup()
        {
            text = Slices.wrappedBuffer(randomText(textSize));
            program = compileProg(HARD);
            dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.FIRST_MATCH);
            phaser = new Phaser(workerCount + 1);
            workers = new Thread[workerCount];
            results = new long[workerCount];

            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                int resultIndex = workerIndex;
                workers[workerIndex] = new Thread(() -> runWorker(resultIndex), "dfa-cold-worker-" + workerIndex);
                workers[workerIndex].start();
            }
        }

        @Setup(Level.Invocation)
        public void resetCache()
        {
            dfa.resetCacheExternal();
        }

        @TearDown(Level.Trial)
        public void tearDown()
                throws InterruptedException
        {
            isShutdown = true;
            phaser.arriveAndAwaitAdvance();
            phaser.arriveAndDeregister();
            for (Thread worker : workers) {
                worker.join();
            }
        }

        private void runWorker(int resultIndex)
        {
            while (true) {
                phaser.arriveAndAwaitAdvance();
                if (isShutdown) {
                    phaser.arriveAndDeregister();
                    return;
                }

                try {
                    results[resultIndex] = search(program, text, false);
                }
                catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
                finally {
                    phaser.arriveAndAwaitAdvance();
                }
            }
        }

        public long runColdWave()
        {
            phaser.arriveAndAwaitAdvance();
            phaser.arriveAndAwaitAdvance();

            Throwable throwable = failure.get();
            if (throwable != null) {
                throw new AssertionError("cold DFA worker failed", throwable);
            }

            long checksum = 0;
            for (long result : results) {
                checksum += result;
            }
            return checksum;
        }
    }

    @State(Scope.Thread)
    public static class LateTransitionState
    {
        @Param({"8", "4096", "262144"})
        int textSize;

        private Slice warmText;
        private Slice textWithLateTransition;
        private Prog program;
        private Dfa.DfaInstance dfa;

        @Setup(Level.Trial)
        public void setup()
        {
            byte[] warmBytes = new byte[textSize];
            Arrays.fill(warmBytes, (byte) 'a');
            byte[] lateTransitionBytes = warmBytes.clone();
            lateTransitionBytes[lateTransitionBytes.length - 1] = 'b';

            warmText = Slices.wrappedBuffer(warmBytes);
            textWithLateTransition = Slices.wrappedBuffer(lateTransitionBytes);
            program = compileProg(LATE_TRANSITION);
            search(program, warmText, true);
            dfa = program.getCachedDfa(Dfa.DfaInstance.Kind.LONGEST_MATCH);
        }

        @Setup(Level.Invocation)
        public void prepareKnownPath()
        {
            dfa.resetCacheExternal();
            search(program, warmText, true);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public long searchSharedWarm(SharedWarmState state)
    {
        return search(state.program, state.text, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public long searchColdStart(ColdStartState state)
    {
        return search(state.program, state.text, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public long searchSharedColdWave(SharedColdState state)
    {
        return state.runColdWave();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public long searchNewTransitionAfterLongScan(LateTransitionState state)
    {
        return search(state.program, state.textWithLateTransition, true);
    }

    private static long search(Prog program, Slice text, boolean anchored)
    {
        return Dfa.search(program, text, anchored, Prog.MatchKind.FIRST_MATCH, true);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = buildOptions(BenchmarkDfaCache.class, args);
        new Runner(options).run();
    }
}
