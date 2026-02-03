/**
 * MongoLake RPC Module
 *
 * Exports RPC service and replica routing components.
 */

export {
  RPCService,
  createRPCService,
  hashToShardId,
  TransientError,
  ShardUnavailableError,
  RPCError,
  type ConsistencyLevel,
  type ReadToken,
  type ShardConnection,
  type OperationResult,
  type BatchResult,
  type RetryConfig,
  type ShardHealth,
  type RPCServiceOptions,
  type Session,
  type RPCFindOptions,
  type InsertManyOptions,
  type InsertOneResultWithToken,
  type InsertManyResultWithToken,
  type UpdateResultWithToken,
  type DeleteResultWithToken,
} from './service.js';

export {
  ReplicaRouter,
  type ReadPreference,
  type ReplicaReadOptions,
  type RoutedReadResult,
  type ReplicaPoolConfig,
  DEFAULT_REPLICA_POOL_CONFIG,
} from './replica-router.js';
