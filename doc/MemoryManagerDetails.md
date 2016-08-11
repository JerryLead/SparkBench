# Details of Memory Management in Spark 2.0

## MemoryManager


An abstract memory manager that enforces how memory is shared between execution and storage.

Execution memory refers to that used for computation in shuffles, joins,
sorts and aggregations.

Storage memory refers to that used for caching and propagating
internal data across the cluster.

There exists one MemoryManager per JVM.
