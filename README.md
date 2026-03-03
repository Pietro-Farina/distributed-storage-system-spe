# Simulation and Performance Evaluation on a Distributed Storage System
Quorum-based replication is widely used in distributed storage systems to balance consistency and performance.
Although quorum intersection rules guarantee sequential consistency, they do not indicate how replication parameters should
be selected under realistic workload and failure conditions.

This paper presents a systematic performance evaluation of a quorum-based sequentially consistent key–value store with configurable
replication, quorum sizes, timeout thresholds, workload skew, and crash injection. Our results quantify how quorum sizes and
workload characteristics impact system performance, providing empirical evidence for practical consistency-latency trade-offs.
These findings offer both theoretical and operational insights for the design of quorum-based distributed systems.

The paper is available in the analysis folder.