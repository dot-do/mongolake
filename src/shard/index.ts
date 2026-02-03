/**
 * MongoLake Shard Module
 *
 * Provides shard routing, monitoring, and dynamic splitting capabilities:
 * - ShardRouter: Consistent hashing for document/collection routing
 * - ShardMonitor: Metrics collection and threshold detection
 * - SplitCoordinator: Orchestrates shard split operations
 */

// Router exports
export {
  ShardRouter,
  hashCollectionToShard,
  hashDocumentToShard,
  createShardRouter,
} from './router.js';
export type {
  ShardRouterOptions,
  ShardAssignment,
  ShardAffinityHint,
  SplitInfo,
  RouterStats,
} from './router.js';

// Monitor exports
export {
  ShardMonitor,
  DEFAULT_SPLIT_THRESHOLDS,
  createShardMonitor,
} from './monitor.js';
export type {
  ShardMetrics,
  CollectionMetrics,
  SplitThresholds,
  SplitRecommendation,
  ShardMonitorConfig,
} from './monitor.js';

// Split coordinator exports
export {
  SplitCoordinator,
  createSplitCoordinator,
} from './split-coordinator.js';
export type {
  SplitState,
  SplitOperation,
  SplitCoordinatorConfig,
  SplitRequestResult,
  KeyRange,
  SplitPoint,
  KeyDistribution,
  RebalanceRecommendation,
  SplitMetadata,
  SplitCoordinatorPersistenceConfig,
  // Merge types
  MergeState,
  MergeOperation,
  MergeRequestResult,
  MergeRecommendation,
  MergeMetadata,
} from './split-coordinator.js';
