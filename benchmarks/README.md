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
