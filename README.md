# Robustness Benchmarking Tool for PostgreSQL
## ABSTRACT

Predictable performance for declarative queries, or robustness, remains a challenge
for relational database systems. Query optimizers can cause divergent performance
through non-optimal plan choices when workloads change. This thesis presents
a systematic approach using query parametrization to analyze query optimizer
behavior under slight workload variations.
We developed a Python-based benchmarking tool for PostgreSQL that auto-
mates query execution, analysis and visualization of performance metrics. The
tool explores used-defined queries and parameterizations, examining query execu-
tion times, query plans and cardinalities. Our work contributes to ongoing robust-
ness research efforts by providing insights into optimizer behavior across different
parameter spaces and data distributions.
This framework enables database administrators to evaluate system robustness
and provides researchers with a foundation for developing improved database oper-
ators, working towards mitigating unpredictable performance in declarative query
processing.
Evaluated on TPC-H and Join-Order Benchmark, we found that the PostgreSQL
optimizer delivers balanced performance for TPC-H queries. However, results for
highly skewed real-world data expose optimization boundaries with patterns of
performance degradation.


