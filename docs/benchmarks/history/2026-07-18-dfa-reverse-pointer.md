# Reverse DFA Absolute-Pointer Experiment

**Status:** Rejected. No production change retained.

## Question

The date `count-spans` outlier runs 42,917 forward DFA searches and 42,916
reverse DFA searches per operation. Reverse traversal accounts for about 38% of
the public time, so this experiment tested whether the accepted 64-bit
absolute-pointer sidecar could accelerate the existing reverse DFA without
introducing tagged-state semantics or changing RE2's two-pass algorithm.

The reverse work is unusually fragmented: it processes 268,796 bytes total,
only 6.3 bytes per search. The acceptance gates therefore required the reverse
stage to take at most `0.87x` object time and the public operation to take at
most `0.95x` object time. A miss ended the experiment without adaptive policy
work.

## Candidate

The candidate added a physically separate backward raw-pointer loop. Normal
non-match transitions used the existing FFM sidecar; every zero entry resumed
the unchanged object/integer continuation at the same state and byte position.
The existing exclusive cache-mutation, growth, reset, accounting, and fallback
protocols were reused.

A direct route replay confirmed that the candidate was active: the reversed
`LONGEST_MATCH` DFA retained 117,760 sidecar bytes and populated 218 pointer
transitions. Exact reverse boundaries, denied native access, reset reuse, and
concurrent cache reconstruction passed focused tests.

## Target Result

AWS session `20260719T053547Z-15241` ran the exact
`curated/03-date/ascii` workload on `c8i.2xlarge` Intel and `c8g.2xlarge`
Graviton4 hosts. Object-before, pointer, and object-after variants ran in
independent JVMs. Every variant returned result 111,817 with 42,916 reverse
matches.

Lower ratios are better.

| Architecture | Object reverse bracket | Pointer reverse | Pointer/object | Object public bracket | Pointer public | Pointer/object |
|---|---:|---:|---:|---:|---:|---:|
| Intel | 2.419 ms | 2.418 ms | 1.000x | 6.376 ms | 6.377 ms | 1.000x |
| Graviton | 2.758 ms | 2.794 ms | 1.013x | 7.310 ms | 7.344 ms | 1.005x |

The reverse controls agree within 0.2% on both hosts, making the direct-stage
rejection decisive. Public-stage values are informational because one object
variant had more than 2% before/after drift; they independently show no
material operation improvement.

## Decision

Reject reverse absolute pointers for this short-call span workload. The FFM
address-space and reachability setup cannot be amortized over six transitions,
so the long-loop pointer advantage does not transfer to repeated boundary
recovery. Remove the candidate and its campaign mode.

Any future attempt to eliminate the date gap must change the amount of work,
not only the physical layout of the reverse transition loop. A tagged DFA would
be a separate algorithm campaign with substantially higher correctness risk.
