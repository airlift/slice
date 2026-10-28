Full UTF-8 Case Mapping
======================

`BenchmarkFullCaseMapping` compares the existing simple uppercase/lowercase functions
with the full mappings. It covers ASCII, mixed UTF-8, supplementary letters, expansions,
contextual sigma, and malformed input at approximately 32 bytes and 1 KiB.

Every fork exercises both APIs in both directions on all workloads before JMH warmup,
so shared code sees both APIs and varied input. The measured methods call
each API directly. Some full mappings produce different output from simple mappings;
their timings compare the cost of the two behaviors.

Build the harness with JDK 25 or later:

```sh
mvn -DskipTests test-compile dependency:build-classpath -Dmdep.outputFile=target/benchmark-classpath.txt
javac --release 25 -cp "target/classes:$(cat target/benchmark-classpath.txt)" \
    -processor org.openjdk.jmh.generators.BenchmarkProcessor \
    -d target/test-classes -s target/generated-test-sources/benchmark \
    src/test/java/io/airlift/slice/BenchmarkFullCaseMapping.java
```

Run on an otherwise idle host. On Linux, prefix the Java command with `taskset -c 1`
to pin the benchmark and its forked JVMs to one CPU:

```sh
java -cp "target/classes:target/test-classes:$(cat target/benchmark-classpath.txt)" \
    org.openjdk.jmh.Main 'BenchmarkFullCaseMapping.*' \
    -t 1 -f 2 -wi 4 -w 500ms -i 4 -r 500ms -prof gc \
    -jvmArgs '-Xms512m -Xmx512m -XX:+UseG1GC -XX:ActiveProcessorCount=1' \
    -rf json -rff target/full-case-mapping.json
```

Both simple and full conversions use eight-byte ASCII casing, including ASCII runs
inside mixed input. Compare these equally optimized functions to measure the cost
of full mappings. Keep results from the original simple implementation as a separate
historical baseline.

These are steady-state microbenchmarks; they exclude class initialization. Retain the
raw fork/iteration results and compare uncertainty before treating small differences
as performance changes.

Memory Copy Microbenchmark
==========================

Throughput numbers: higher is better

Mac Pro Early 2009 -- 2x2.66GHz Xeon -- OS 10.9
-----------------------------------------------

    Benchmark                                       Mode Thr    Cnt  Sec         Mean   Mean error    Units
    i.a.s.MemoryCopyBenchmark.b00sliceZero         thrpt   1      5    1    33180.705      525.792   ops/ms
    i.a.s.MemoryCopyBenchmark.b01customLoopZero    thrpt   1      5    1    44505.121      223.078   ops/ms
    i.a.s.MemoryCopyBenchmark.b02unsafeZero        thrpt   1      5    1    33298.713      741.633   ops/ms
    i.a.s.MemoryCopyBenchmark.b03slice32B          thrpt   1      5    1     3620.478       26.610   ops/ms
    i.a.s.MemoryCopyBenchmark.b04customLoop32B     thrpt   1      5    1     7469.741     1860.961   ops/ms
    i.a.s.MemoryCopyBenchmark.b05unsafe32B         thrpt   1      5    1     3748.963       30.115   ops/ms
    i.a.s.MemoryCopyBenchmark.b06slice128B         thrpt   1      5    1     3450.564       35.321   ops/ms
    i.a.s.MemoryCopyBenchmark.b07customLoop128B    thrpt   1      5    1     5439.138       90.792   ops/ms
    i.a.s.MemoryCopyBenchmark.b08unsafe128B        thrpt   1      5    1     3504.716        9.448   ops/ms
    i.a.s.MemoryCopyBenchmark.b09slice512B         thrpt   1      5    1     2965.151       44.551   ops/ms
    i.a.s.MemoryCopyBenchmark.b10customLoop512B    thrpt   1      5    1     2325.568      113.557   ops/ms
    i.a.s.MemoryCopyBenchmark.b11unsafe512B        thrpt   1      5    1     2996.845       16.525   ops/ms
    i.a.s.MemoryCopyBenchmark.b12slice1K           thrpt   1      5    1     2006.529        4.079   ops/ms
    i.a.s.MemoryCopyBenchmark.b13customLoop1K      thrpt   1      5    1     1484.227        0.831   ops/ms
    i.a.s.MemoryCopyBenchmark.b14unsafe1K          thrpt   1      5    1     2039.754        9.237   ops/ms
    i.a.s.MemoryCopyBenchmark.b15slice1M           thrpt   1      5    1        3.993        0.005   ops/ms
    i.a.s.MemoryCopyBenchmark.b16customLoop1M      thrpt   1      5    1        3.531        0.011   ops/ms
    i.a.s.MemoryCopyBenchmark.b17unsafe1M          thrpt   1      5    1        3.978        0.052   ops/ms
    i.a.s.MemoryCopyBenchmark.b18slice128M         thrpt   1      5    1        0.029        0.000   ops/ms
    i.a.s.MemoryCopyBenchmark.b19customLoop128M    thrpt   1      5    1        0.027        0.001   ops/ms
    i.a.s.MemoryCopyBenchmark.b20unsafe128M        thrpt   1      5    1        0.029        0.000   ops/ms

Xeon X5670 2.93GHz -- CentOS 6.4
--------------------------------

    Benchmark                                       Mode Thr    Cnt  Sec         Mean   Mean error    Units
    i.a.s.MemoryCopyBenchmark.b00sliceZero         thrpt   1      7    1    32019.795       14.552   ops/ms
    i.a.s.MemoryCopyBenchmark.b01customLoopZero    thrpt   1      7    1    42989.712      151.868   ops/ms
    i.a.s.MemoryCopyBenchmark.b02unsafeZero        thrpt   1      7    1    31491.911       33.982   ops/ms
    i.a.s.MemoryCopyBenchmark.b03slice32B          thrpt   1      7    1     8324.091        1.213   ops/ms
    i.a.s.MemoryCopyBenchmark.b04customLoop32B     thrpt   1      7    1    13790.033       18.228   ops/ms
    i.a.s.MemoryCopyBenchmark.b05unsafe32B         thrpt   1      7    1     8997.760        3.275   ops/ms
    i.a.s.MemoryCopyBenchmark.b06slice128B         thrpt   1      7    1     7517.828        7.459   ops/ms
    i.a.s.MemoryCopyBenchmark.b07customLoop128B    thrpt   1      7    1     8225.268       49.770   ops/ms
    i.a.s.MemoryCopyBenchmark.b08unsafe128B        thrpt   1      7    1     7996.823       40.365   ops/ms
    i.a.s.MemoryCopyBenchmark.b09slice512B         thrpt   1      7    1     5854.990       53.925   ops/ms
    i.a.s.MemoryCopyBenchmark.b10customLoop512B    thrpt   1      7    1     3553.523       21.415   ops/ms
    i.a.s.MemoryCopyBenchmark.b11unsafe512B        thrpt   1      7    1     6106.234       37.416   ops/ms
    i.a.s.MemoryCopyBenchmark.b12slice1K           thrpt   1      7    1     3377.940       26.430   ops/ms
    i.a.s.MemoryCopyBenchmark.b13customLoop1K      thrpt   1      7    1     2879.045       51.883   ops/ms
    i.a.s.MemoryCopyBenchmark.b14unsafe1K          thrpt   1      7    1     3426.431        4.206   ops/ms
    i.a.s.MemoryCopyBenchmark.b15slice1M           thrpt   1      7    1        4.958        0.017   ops/ms
    i.a.s.MemoryCopyBenchmark.b16customLoop1M      thrpt   1      7    1        4.313        0.032   ops/ms
    i.a.s.MemoryCopyBenchmark.b17unsafe1M          thrpt   1      7    1        4.942        0.050   ops/ms
    i.a.s.MemoryCopyBenchmark.b18slice128M         thrpt   1      7    1        0.037        0.001   ops/ms
    i.a.s.MemoryCopyBenchmark.b19customLoop128M    thrpt   1      7    1        0.033        0.001   ops/ms
    i.a.s.MemoryCopyBenchmark.b20unsafe128M        thrpt   1      7    1        0.037        0.001   ops/ms
