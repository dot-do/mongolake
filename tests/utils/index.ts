/**
 * Test Utilities
 *
 * Shared test utilities for MongoLake tests.
 * Import from this module to access factories, mocks, assertions, and fixtures.
 *
 * @example
 * ```ts
 * import {
 *   // Factories
 *   createUser,
 *   createObjectId,
 *   createDeduplicationDoc,
 *
 *   // Mocks
 *   createMockR2Bucket,
 *   createMockEnv,
 *   createMockFetch,
 *
 *   // Assertions
 *   assertDocumentId,
 *   assertInsertSuccess,
 *   assertSortedBy,
 *
 *   // Fixtures
 *   USERS,
 *   PRODUCTS,
 *   FILTERS,
 * } from '../utils/index.js';
 * ```
 */

// ============================================================================
// Factories
// ============================================================================

export {
  // ObjectId factories
  createObjectId,
  createObjectIdString,
  createObjectIdFromDate,
  createObjectIds,
  // Date factories
  createDate,
  createPastDate,
  createFutureDate,
  createDateAt,
  // Document factories
  createUser,
  createUsers,
  createOrder,
  createOrderItem,
  createProduct,
  createDeduplicationDoc,
  createDeduplicationSequence,
  createAddress,
  createAddressWithCoordinates,
  // Nested document factories
  createNestedDocument,
  createDocumentWithPath,
  // Bulk factories
  createBatch,
  createDocumentWithManyKeys,
  createLargeDocument,
  // Type helpers
  createDocument,
  createMatchingDocument,
  // Counter management
  resetDocumentCounter,
  // Parquet file factories
  ParquetFileBuilder,
  createParquetFile,
  createParquetFiles,
  // WAL entry factories
  WalEntryBuilder,
  createWalEntry,
  createWalSequence,
  // Shard state factories
  ShardStateBuilder,
  createShardState,
  createShardCluster,
  // Branch metadata factories
  BranchMetadataBuilder,
  createBranchMetadata,
  createMainBranch,
  createBranchHierarchy,
  // Transaction state factories
  TransactionStateBuilder,
  createTransactionState,
  createInProgressTransaction,
  // Index entry factories
  IndexMetadataBuilder,
  BTreeIndexEntryBuilder,
  createIndexMetadata,
  createIndexEntry,
  createIndexEntries,
  createSerializedNode,
  createSerializedBTree,
  // Session state factories
  SessionStateBuilder,
  createSessionState,
  // Collection metadata factories
  CollectionMetadataBuilder,
  createCollectionMetadata,
  // Auth user factories
  AuthUserBuilder,
  createAuthUser,
  createAuthUsers,
  // Types
  type UserDocument,
  type OrderDocument,
  type ProductDocument,
  type AddressDocument,
  type OrderItem,
  type DeduplicationDocument,
  type ParquetFileMetadata,
  type ZoneMapEntry,
  type WalEntry,
  type ShardState,
  type BranchState,
  type BranchMetadata,
  type TransactionState,
  type ReadConcernLevel,
  type TransactionOptions,
  type BufferedOperation,
  type TransactionTestState,
  type BTreeIndexEntry,
  type IndexMetadata,
  type SerializedBTree,
  type SerializedNode,
  type SessionOptions,
  type SessionTestState,
  type CollectionMetadata,
  type AuthUser,
} from './factories.js';

// ============================================================================
// Mocks
// ============================================================================

export {
  // Storage mocks
  createMockStorage,
  createMockR2Bucket,
  createSpiedR2Bucket,
  createMockDurableObjectStorage,
  // Network mocks
  createMockFetch,
  installFetchMock,
  restoreFetch,
  // Timer mocks
  createMockTimers,
  // Environment mocks
  createMockEnv,
  // Request/Response helpers
  createMockRequest,
  createWebSocketRequest,
  parseJsonResponse,
  // Event emitter mock
  createMockEventEmitter,
  // Types
  type MockStorage,
  type MockMultipartUpload,
  type MockDurableObjectStorage,
  type MockFetch,
  type MockTimers,
  type MockMongoLakeEnv,
  type MockEventEmitter,
} from './mocks.js';

// ============================================================================
// Socket Mocks
// ============================================================================

export {
  // Socket mock class and factory
  MockSocketImpl,
  createMockSocket,
  createSpiedMockSocket,
  createMockSockets,
  createErroringMockSocket,
  createSlowMockSocket,
  // Types
  type MockSocket,
  type MockSocketOptions,
  type SpiedMockSocket,
} from './mock-socket.js';

// ============================================================================
// Assertions
// ============================================================================

export {
  // Document assertions
  assertValidObjectId,
  assertObjectId,
  assertDocumentId,
  assertDocumentFields,
  assertDocumentExcludesFields,
  assertRecentlyCreated,
  // Collection assertions
  assertContainsDocumentWithId,
  assertNotContainsDocumentWithId,
  assertSortedBy,
  assertAllMatch,
  assertSomeMatch,
  assertNoneMatch,
  assertUniqueIds,
  // Operation result assertions
  assertInsertSuccess,
  assertUpdateSuccess,
  assertDeleteSuccess,
  // Error assertions
  assertThrowsAsync,
  assertNoThrowAsync,
  // Timing assertions
  assertCompletesWithin,
  assertTakesAtLeast,
  // Deduplication assertions
  assertDeduplicationStats,
  // HTTP response assertions
  assertResponseStatus,
  assertResponseSuccess,
  assertResponseHeader,
  assertJsonResponse,
} from './assertions.js';

// ============================================================================
// Fixtures
// ============================================================================

export {
  // ObjectId fixtures
  OBJECT_IDS,
  getObjectIdInstance,
  // Document fixtures
  USERS,
  PRODUCTS,
  ORDERS,
  ADDRESSES,
  getAllUsers,
  getUsersMatching,
  getAllProducts,
  getAllOrders,
  // Deduplication fixtures
  DEDUPLICATION_SCENARIOS,
  // Filter fixtures
  FILTERS,
  // Update fixtures
  UPDATES,
  // Error fixtures
  ERRORS,
  // Date fixtures
  DATES,
  // Parquet fixtures
  PARQUET_MAGIC,
  createMinimalParquetHeader,
  // Large dataset generators
  generateLargeUserDataset,
  generateLargeProductDataset,
} from './fixtures.js';

// ============================================================================
// Concurrency
// ============================================================================

export {
  // Classes
  ParallelRunner,
  RaceConditionDetector,
  LockContentionSimulator,
  ThreadPoolMock,
  // Assertion helpers
  assertNoInterleaving,
  assertEventuallyConsistent,
  assertMonotonicallyIncreasing,
  assertNoDuplicates,
  assertMutualExclusion,
  assertParallelCompletion,
  assertReadConsistency,
  // Utility functions
  delay,
  withTimeout,
  withRetry,
  createBarrier,
  createLatch,
  // Types
  type TaskResult,
  type ParallelExecutionStats,
  type ParallelRunnerOptions,
  type RaceEvent,
  type RaceCondition,
  type LockRequest,
  type LockContentionStats,
  type MockWorker,
  type ThreadPoolConfig,
} from './concurrency.js';

// ============================================================================
// Security
// ============================================================================

export {
  // NoSQL injection payloads
  nosqlInjectionPayloads,
  allNosqlPayloads,
  // Path traversal payloads
  pathTraversalPayloads,
  allPathTraversalPayloads,
  // Token forgery
  generateMalformedJwt,
  forgedTokens,
  generateForgedJwt,
  // Oversized payloads
  generateOversizedPayload,
  generateDeeplyNestedObject,
  generateWideObject,
  generateLargeArray,
  generateOversizedString,
  // Malformed BSON
  malformedBsonPayloads,
  generateMalformedBsonDocument,
  // Auth bypass
  authBypassPatterns,
  generateAuthTestCases,
  // Consolidated payloads
  injectionPayloads,
  // Helpers
  containsPathTraversal,
  containsNoSqlInjection,
  sanitizePath,
  // Test suite generator
  createSecurityTestSuite,
  // Types
  type SecurityTestCase,
} from './security.js';

// ============================================================================
// Chaos Testing
// ============================================================================

export {
  // Classes
  FaultInjector,
  NetworkPartition,
  RandomFailure,
  TimeoutSimulator,
  // Error types
  TimeoutError,
  CircuitOpenError,
  // Chaos storage mocks
  createChaosStorage,
  createChaosR2Bucket,
  // Scenario runner
  runChaosScenario,
  createChaosTestSuite,
  // Assertions
  assertChaosSuccess,
  assertChaosTriggered,
  // Predefined scenarios
  CHAOS_SCENARIOS,
  // Types
  type FaultType,
  type FaultConfig,
  type FaultStats,
  type NodeState,
  type PartitionConfig,
  type ChaosScenario,
  type ChaosStorageOptions,
  type ChaosContext,
  type ChaosScenarioResult,
  type NetworkEvent,
} from './chaos.js';

// ============================================================================
// Memory Profiling
// ============================================================================

export {
  // Classes
  MemoryProfiler,
  // Snapshot functions
  createMemorySnapshot,
  calculateDiff,
  // Memory tracking helpers
  withMemoryTracking,
  withMemoryTrackingSync,
  monitorMemoryOverIterations,
  // Assertions
  assertNoMemoryLeak,
  assertMemoryDiffWithinBounds,
  assertMemoryReturnsToBaseline,
  assertNoLeakOverIterations,
  // Benchmark utilities
  createMemoryBenchmark,
  compareMemoryBenchmarks,
  formatBenchmarkResult,
  // Vitest integration
  withMemoryTest,
  memoryTest,
  // Utility functions
  forceGarbageCollection,
  isGCAvailable,
  getV8HeapStatistics,
  formatBytes,
  waitMs,
  createMemoryPressure,
  runUntilMemoryStabilizes,
  // Types
  type MemorySnapshot,
  type V8HeapStats,
  type MemoryDiff,
  type MemoryReport,
  type MemorySummary,
  type LeakIndicator,
  type MemoryLeakAssertionOptions,
  type MemoryProfilerOptions,
  type MemoryBenchmarkResult,
  type MemoryBenchmarkOptions,
} from './memory-profiler.js';
