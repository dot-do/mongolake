# MongoLake Performance Benchmarks

This directory contains performance benchmarks for MongoLake operations. These benchmarks establish baselines for regression detection and track performance over time.

## Running Benchmarks

### Vitest Bench Suite (Recommended)

The vitest-based benchmarks provide statistical analysis with percentile data:

```bash
# Run all vitest benchmarks
pnpm run benchmark:vitest

# Output includes JSON results at benchmark-results.json
```

### Legacy Custom Benchmarks

The original benchmark suite with detailed console output:

```bash
# Run all benchmarks
pnpm run benchmark

# Run individual benchmark suites
pnpm run benchmark:insert
pnpm run benchmark:find
pnpm run benchmark:parquet
pnpm run benchmark:index
```

## Benchmark Files

| File | Description |
|------|-------------|
| `insert.bench.ts` | Vitest insertOne/insertMany throughput benchmarks |
| `query.bench.ts` | Vitest find/filter/sort/projection benchmarks |
| `aggregation.bench.ts` | Vitest aggregation pipeline benchmarks |
| `insert-benchmark.ts` | Legacy insert performance suite |
| `find-benchmark.ts` | Legacy query performance suite |
| `parquet-benchmark.ts` | Parquet I/O and serialization benchmarks |
| `index-benchmark.ts` | B-tree index operation benchmarks |
| `utils.ts` | Shared benchmark utilities and data generators |
| `run-all.ts` | Runner for legacy benchmark suite |

## Baseline Numbers

**Environment:**
- Node.js v22+ (Apple Silicon M-series)
- vitest v3.2.4
- Date: February 2026

### Insert Operations

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| insertOne - simple doc (~200B) | 713,305 | 0.0014 | 0.0019 |
| insertOne - medium doc (~1KB) | 315,550 | 0.0032 | 0.0057 |
| insertOne - large doc (~5KB) | 49,795 | 0.0201 | 0.0630 |
| insertMany - batch 10 | 104,709 | 0.0096 | 0.0167 |
| insertMany - batch 100 | 7,840 | 0.1276 | 0.3246 |
| insertMany - batch 1000 | 1,086 | 0.9209 | 3.3968 |
| Insert w/o index (100 docs) | 9,972 | 0.1003 | 0.2784 |
| Insert w/ 1 index (100 docs) | 8,786 | 0.1138 | 0.3067 |
| Insert w/ 3 indexes (100 docs) | 4,323 | 0.2313 | 0.5949 |

### Query Operations

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| findOne - equality filter | 1,150,759 | 0.0009 | 0.0021 |
| findOne - via index | 4,963,657 | 0.0002 | 0.0005 |
| findOne - compound filter | 231,217 | 0.0043 | 0.0097 |
| find 10 results (1K docs) | 12,095 | 0.0827 | 0.3205 |
| find 100 results (1K docs) | 10,869 | 0.0920 | 0.3386 |
| find 1000 results (10K docs) | 1,055 | 0.9478 | 1.4990 |
| Full scan 10K docs | 802 | 1.2475 | 5.0988 |

### Index vs Full Scan Comparison

| Operation | Throughput (ops/sec) | Notes |
|-----------|---------------------|-------|
| Full scan: age === 30 | 52,129 | Baseline |
| Index lookup: age === 30 | 353,474 | **6.8x faster** |
| Full scan: range query | 10,564 | Baseline |
| Index range query | 11,997 | 1.1x faster |
| Index lookup: string key | 29,775 | - |

### Sorting Performance

| Operation | Throughput (ops/sec) | p50 (ms) |
|-----------|---------------------|----------|
| Sort 1K docs - single field | 1,476 | 0.68 |
| Sort 1K docs - multi-field | 748 | 1.34 |
| Sort 10K docs - single field | 172 | 5.83 |

### Projection Performance

| Operation | Throughput (ops/sec) | p50 (ms) |
|-----------|---------------------|----------|
| Include 3 fields - 1K docs | 5,286 | 0.19 |
| Exclude 2 fields - 1K docs | 2,798 | 0.36 |
| Include 3 fields - 10K docs | 502 | 1.99 |

### Filter Complexity (10K docs)

| Filter Type | Throughput (ops/sec) | p50 (ms) |
|-------------|---------------------|----------|
| Simple equality | 1,101 | 0.91 |
| Range filter | 953 | 1.05 |
| $in filter | 556 | 1.80 |
| $and filter | 301 | 3.33 |
| $or filter | 212 | 4.72 |
| Complex compound | 196 | 5.11 |

### Aggregation Pipeline

| Pipeline | Throughput (ops/sec) | p50 (ms) |
|----------|---------------------|----------|
| $match only (1K docs) | 8,669 | 0.12 |
| $match only (10K docs) | 958 | 1.04 |
| $match + $sort (1K docs) | 1,205 | 0.83 |
| $match + $sort + $limit (1K docs) | 1,330 | 0.75 |
| $match + $project (1K docs) | 6,941 | 0.14 |
| $count (10K docs) | 1,074 | 0.93 |

### $group with Accumulators

| Pipeline | Throughput (ops/sec) | p50 (ms) |
|----------|---------------------|----------|
| $group $sum (1K docs) | 9,856 | 0.10 |
| $group $sum (10K docs) | 959 | 1.04 |
| $group $avg (1K docs) | 7,676 | 0.13 |
| $group $min/$max (1K docs) | 7,535 | 0.13 |
| $group total (_id: null, 10K docs) | 1,293 | 0.77 |
| $group compound key (1K docs) | 2,680 | 0.37 |

### Complex Pipelines

| Pipeline | Throughput (ops/sec) | p50 (ms) |
|----------|---------------------|----------|
| $match + $group + $sort (1K docs) | 6,233 | 0.16 |
| $match + $group + $sort (10K docs) | 625 | 1.60 |
| $unwind + $group (1K docs) | 1,799 | 0.56 |
| Multi-stage analytics (1K docs) | 2,330 | 0.43 |
| Full pipeline w/ projection (1K docs) | 1,590 | 0.63 |

### Parquet Serialization

| Operation | Throughput (ops/sec) | p50 (ms) |
|-----------|---------------------|----------|
| writeParquet - 100 docs | 1,160 | 0.86 |
| writeParquet - 1000 docs | 124 | 8.09 |

## Key Findings

1. **Index Performance**: Index lookups are **6.8x faster** than full scans for equality queries
2. **Document Size Impact**: Large documents (~5KB) are 14x slower to insert than simple documents
3. **Batch Size Trade-off**: Larger batch sizes reduce per-document overhead but increase total latency
4. **Filter Complexity**: Complex compound filters ($or with nested $and) are ~5.6x slower than simple equality
5. **Projection Efficiency**: Include projections are ~1.9x faster than exclude projections
6. **Aggregation Scaling**: 10K document aggregations are ~9-10x slower than 1K document aggregations

## Regression Detection

To detect performance regressions:

1. Run benchmarks before and after changes
2. Compare p50/p99 latencies and throughput
3. Flag regressions > 20% for investigation
4. Use JSON output for automated CI comparison:

```bash
# Generate JSON results
pnpm run benchmark:vitest

# Results saved to benchmark-results.json
```

## Environment Variables

- `BENCH_ITERATIONS`: Override default iteration count
- `BENCH_WARMUP`: Override warmup iterations

## Notes

- Benchmarks run in single-threaded mode for consistency
- Results may vary based on hardware and system load
- Always run benchmarks on a quiet system for accurate results
- These benchmarks measure core operations, not full end-to-end latency
