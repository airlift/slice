Microbenchmarks
===============

Memory Copy
-----------

Mac Pro Early 2009
2x2.66GHz Xeon

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

Throughput numbers: higher is better
