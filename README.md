# Robustness Benchmarking Tool for PostgreSQL
## ABSTRACT

Predictable performance for declarative queries, or robustness, remains a challenge for relational database systems. Query optimizers can cause divergent performance through non-optimal plan choices when workloads change. This thesis presents a systematic approach using parameterized queries to analyze query optimizer behavior under slight workload variations.
We developed a Python-based benchmarking tool for PostgreSQL that auto- mates query execution, analysis and visualization. The tool explores user-defined queries and parameterizations, examining query execution times, query plans and cardinalities. Our work contributes to the ongoing robustness research e↵orts by providing insights into optimizer behavior across di↵erent parameter spaces and data distributions.
This framework enables database administrators to evaluate robustness and pro- vides researchers with a foundation for developing improved database operators, to mitigate unpredictable performance in declarative query processing.
We evaluated our solution on the TPC-H and Join-Order Benchmark and found that the PostgreSQL optimizer delivers balanced performance for TPC-H queries. However, results for Join-Order Benchmark, with highly skewed real-world data, expose optimization boundaries with patterns of performance degradation.


