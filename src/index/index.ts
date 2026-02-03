/**
 * Index Module
 *
 * Provides B-tree, full-text, and geospatial indexing capabilities for MongoLake collections.
 */

export {
  BTree,
  BTreeNode,
  type BTreeEntry,
  type SerializedNode,
  type SerializedBTree,
  type IndexMetadata,
  type CompareFn,
} from './btree.js';

export {
  TextIndex,
  type TextIndexEntry,
  type TextIndexMetadata,
  type SerializedTextIndex,
  type TextSearchResult,
  type TextSearchOptions,
} from './text-index.js';

export {
  GeoIndex,
  parseCoordinates,
  isGeoJSONPoint,
  isGeoJSONPolygon,
  isGeoJSONGeometry,
  euclideanDistance,
  haversineDistance,
  pointInBox,
  pointInPolygon,
  pointInCircle,
  pointIntersectsGeometry,
  metersToRadians,
  radiansToMeters,
  EARTH_RADIUS_METERS,
  EARTH_RADIUS_KM,
  EARTH_RADIUS_MILES,
  type GeoJSONPoint,
  type GeoJSONLineString,
  type GeoJSONPolygon,
  type GeoJSONMultiPoint,
  type GeoJSONMultiLineString,
  type GeoJSONMultiPolygon,
  type GeoJSONGeometryCollection,
  type GeoJSONGeometry,
  type LegacyCoordinates,
  type LegacyCoordinateObject,
  type CoordinateInput,
  type GeoIndexType,
  type GeoIndexEntry,
  type GeoIndexMetadata,
  type SerializedGeoIndex,
  type NearQueryOptions,
  type GeoWithinOptions,
  type GeoIntersectsOptions,
  type GeoDistanceResult,
} from './geo-index.js';

export {
  IndexManager,
  type IndexScanResult,
  type QueryPlan,
} from './index-manager.js';

export {
  QueryPlanner,
  type ExecutionPlan,
  type PlanExecutionResult,
} from './query-planner.js';

export {
  CompoundIndex,
  intersectIndexResults,
  unionIndexResults,
  parseIndexSpec,
  generateCompoundIndexName,
  type CompoundIndexField,
  type CompoundIndexMetadata,
  type SerializedCompoundIndex,
  type CompoundKeyValue,
  type CompoundPrefixCondition,
} from './compound.js';
