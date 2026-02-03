/**
 * Geospatial Query Operators
 *
 * MongoDB-compatible geospatial query operator implementation.
 *
 * Features:
 * - $near and $nearSphere for proximity queries
 * - $geoWithin with $box, $polygon, $center, $centerSphere
 * - $geoIntersects for geometry intersection
 * - Support for GeoJSON geometries
 * - Legacy coordinate pair support
 */

import {
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
  radiansToMeters,
  type GeoJSONPoint,
  type GeoJSONPolygon,
  type GeoJSONGeometry,
  type NearQueryOptions,
  type GeoWithinOptions,
  type GeoIntersectsOptions,
  type GeoDistanceResult,
} from '../index/geo-index.js';

// ============================================================================
// Types
// ============================================================================

/** Geospatial query context for filter evaluation */
export interface GeoQueryContext {
  /** Geo index to use for query optimization */
  geoIndex?: GeoIndex;
  /** Pre-computed matching document IDs */
  matchingDocIds?: Set<string>;
  /** Pre-computed distances for $near queries */
  distances?: Map<string, number>;
}

/** $near query specification */
export interface NearQuery {
  $near?: GeoJSONPoint | [number, number] | NearQueryOptions;
  $maxDistance?: number;
  $minDistance?: number;
}

/** $nearSphere query specification */
export interface NearSphereQuery {
  $nearSphere?: GeoJSONPoint | [number, number] | NearQueryOptions;
  $maxDistance?: number;
  $minDistance?: number;
}

/** $geoWithin query specification */
export interface GeoWithinQuery {
  $geoWithin: GeoWithinOptions;
}

/** $geoIntersects query specification */
export interface GeoIntersectsQuery {
  $geoIntersects: GeoIntersectsOptions;
}

/** Union of all geo query types */
export type GeoQuery = NearQuery | NearSphereQuery | GeoWithinQuery | GeoIntersectsQuery;

// ============================================================================
// Query Detection
// ============================================================================

/**
 * Check if a filter condition contains a geospatial query operator
 */
export function isGeoQuery(condition: unknown): boolean {
  if (typeof condition !== 'object' || condition === null) {
    return false;
  }

  const ops = condition as Record<string, unknown>;
  return (
    '$near' in ops ||
    '$nearSphere' in ops ||
    '$geoWithin' in ops ||
    '$geoIntersects' in ops
  );
}

/**
 * Check if a filter contains any geospatial query operators
 */
export function hasGeoQuery(filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators - check recursively
    if (key === '$and' && Array.isArray(value)) {
      if (value.some(f => hasGeoQuery(f as Record<string, unknown>))) {
        return true;
      }
      continue;
    }
    if (key === '$or' && Array.isArray(value)) {
      if (value.some(f => hasGeoQuery(f as Record<string, unknown>))) {
        return true;
      }
      continue;
    }

    // Check field-level conditions
    if (isGeoQuery(value)) {
      return true;
    }
  }

  return false;
}

/**
 * Extract geo query information from a filter
 */
export function extractGeoQuery(filter: Record<string, unknown>): {
  field: string;
  query: GeoQuery;
} | null {
  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith('$')) continue;

    if (isGeoQuery(value)) {
      return { field: key, query: value as GeoQuery };
    }
  }

  // Check in $and conditions
  if ('$and' in filter && Array.isArray(filter.$and)) {
    for (const f of filter.$and) {
      const result = extractGeoQuery(f as Record<string, unknown>);
      if (result) return result;
    }
  }

  return null;
}

// ============================================================================
// Query Parsing
// ============================================================================

/**
 * Parse $near or $nearSphere query to normalized options
 */
export function parseNearQuery(query: NearQuery | NearSphereQuery): {
  point: [number, number];
  options: NearQueryOptions;
  spherical: boolean;
} | null {
  const isNear = '$near' in query;
  const isNearSphere = '$nearSphere' in query;

  if (!isNear && !isNearSphere) {
    return null;
  }

  const spherical = isNearSphere;
  const nearValue = isNear
    ? (query as NearQuery).$near
    : (query as NearSphereQuery).$nearSphere;

  let point: [number, number] | null = null;
  let options: NearQueryOptions = {};

  // Parse the $near value
  if (Array.isArray(nearValue)) {
    // Legacy coordinate array
    const coords = parseCoordinates(nearValue);
    if (coords) {
      point = coords;
    }
  } else if (isGeoJSONPoint(nearValue)) {
    // GeoJSON Point
    point = nearValue.coordinates;
  } else if (typeof nearValue === 'object' && nearValue !== null) {
    // NearQueryOptions with $geometry
    const opts = nearValue as NearQueryOptions;
    if (opts.$geometry) {
      point = opts.$geometry.coordinates;
    }
    options = opts;
  }

  if (!point) {
    return null;
  }

  // Add $maxDistance and $minDistance from query root if present
  if ('$maxDistance' in query && query.$maxDistance !== undefined) {
    options.$maxDistance = query.$maxDistance;
  }
  if ('$minDistance' in query && query.$minDistance !== undefined) {
    options.$minDistance = query.$minDistance;
  }

  return { point, options, spherical };
}

/**
 * Parse $geoWithin query options
 */
export function parseGeoWithinQuery(query: GeoWithinQuery): GeoWithinOptions | null {
  if (!query.$geoWithin) {
    return null;
  }
  return query.$geoWithin;
}

/**
 * Parse $geoIntersects query options
 */
export function parseGeoIntersectsQuery(query: GeoIntersectsQuery): GeoIntersectsOptions | null {
  if (!query.$geoIntersects || !query.$geoIntersects.$geometry) {
    return null;
  }
  return query.$geoIntersects;
}

// ============================================================================
// Query Evaluation
// ============================================================================

/**
 * Evaluate a $near or $nearSphere condition against a document value
 */
export function matchesNearCondition(
  value: unknown,
  query: NearQuery | NearSphereQuery
): boolean {
  const parsed = parseNearQuery(query);
  if (!parsed) {
    return false;
  }

  const coords = parseCoordinates(value);
  if (!coords) {
    return false;
  }

  const { point, options, spherical } = parsed;
  const distance = spherical
    ? haversineDistance(coords, point)
    : euclideanDistance(coords, point);

  const minDistance = options.$minDistance ?? 0;
  const maxDistance = options.$maxDistance ?? Infinity;

  return distance >= minDistance && distance <= maxDistance;
}

/**
 * Evaluate a $geoWithin condition against a document value
 */
export function matchesGeoWithinCondition(
  value: unknown,
  query: GeoWithinQuery
): boolean {
  const coords = parseCoordinates(value);
  if (!coords) {
    return false;
  }

  const options = query.$geoWithin;
  if (!options) {
    return false;
  }

  // $box - rectangular area
  if (options.$box) {
    return pointInBox(coords, options.$box[0], options.$box[1]);
  }

  // $polygon - arbitrary polygon
  if (options.$polygon) {
    return pointInPolygon(coords, options.$polygon);
  }

  // $center - circular area (flat geometry)
  if (options.$center) {
    const [center, radius] = options.$center;
    return pointInCircle(coords, center, radius, false);
  }

  // $centerSphere - circular area on sphere (radius in radians)
  if (options.$centerSphere) {
    const [center, radiusRadians] = options.$centerSphere;
    const radiusMeters = radiansToMeters(radiusRadians);
    return pointInCircle(coords, center, radiusMeters, true);
  }

  // $geometry - GeoJSON polygon
  if (options.$geometry && isGeoJSONPolygon(options.$geometry)) {
    const polygon = options.$geometry;
    // Check outer ring
    if (!pointInPolygon(coords, polygon.coordinates[0]!)) {
      return false;
    }
    // Check holes
    for (let i = 1; i < polygon.coordinates.length; i++) {
      if (pointInPolygon(coords, polygon.coordinates[i]!)) {
        return false; // Point is in a hole
      }
    }
    return true;
  }

  return false;
}

/**
 * Evaluate a $geoIntersects condition against a document value
 */
export function matchesGeoIntersectsCondition(
  value: unknown,
  query: GeoIntersectsQuery
): boolean {
  const options = query.$geoIntersects;
  if (!options || !options.$geometry) {
    return false;
  }

  // If value is a GeoJSON geometry, check full intersection
  if (isGeoJSONGeometry(value)) {
    return geometriesIntersect(value as GeoJSONGeometry, options.$geometry);
  }

  // Otherwise, parse as coordinates and check point intersection
  const coords = parseCoordinates(value);
  if (!coords) {
    return false;
  }

  return pointIntersectsGeometry(coords, options.$geometry);
}

/**
 * Check if two geometries intersect
 */
function geometriesIntersect(g1: GeoJSONGeometry, g2: GeoJSONGeometry): boolean {
  // Point-to-geometry intersection
  if (g1.type === 'Point') {
    return pointIntersectsGeometry(g1.coordinates, g2);
  }
  if (g2.type === 'Point') {
    return pointIntersectsGeometry(g2.coordinates, g1);
  }

  // Polygon-to-polygon intersection
  if (g1.type === 'Polygon' && g2.type === 'Polygon') {
    return polygonsIntersectHelper(g1.coordinates[0]!, g2.coordinates[0]!);
  }

  // For other cases, check if any points from g1 are in g2 or vice versa
  const coords1 = extractAllCoords(g1);
  const coords2 = extractAllCoords(g2);

  for (const coord of coords1) {
    if (pointIntersectsGeometry(coord, g2)) {
      return true;
    }
  }

  for (const coord of coords2) {
    if (pointIntersectsGeometry(coord, g1)) {
      return true;
    }
  }

  return false;
}

/**
 * Helper to check polygon intersection
 */
function polygonsIntersectHelper(
  poly1: [number, number][],
  poly2: [number, number][]
): boolean {
  // Check if any vertex of poly1 is inside poly2
  for (const point of poly1) {
    if (pointInPolygon(point, poly2)) {
      return true;
    }
  }

  // Check if any vertex of poly2 is inside poly1
  for (const point of poly2) {
    if (pointInPolygon(point, poly1)) {
      return true;
    }
  }

  // Could also check edge intersections for more accuracy
  return false;
}

/**
 * Extract all coordinates from a geometry
 */
function extractAllCoords(geometry: GeoJSONGeometry): [number, number][] {
  switch (geometry.type) {
    case 'Point':
      return [geometry.coordinates];
    case 'LineString':
    case 'MultiPoint':
      return geometry.coordinates;
    case 'Polygon':
    case 'MultiLineString':
      return geometry.coordinates.flat();
    case 'MultiPolygon':
      return geometry.coordinates.flat(2);
    case 'GeometryCollection':
      return geometry.geometries.flatMap(g => extractAllCoords(g));
    default:
      return [];
  }
}

/**
 * Evaluate any geo condition against a document value
 */
export function matchesGeoCondition(
  value: unknown,
  condition: GeoQuery
): boolean {
  if ('$near' in condition || '$nearSphere' in condition) {
    return matchesNearCondition(value, condition as NearQuery | NearSphereQuery);
  }

  if ('$geoWithin' in condition) {
    return matchesGeoWithinCondition(value, condition as GeoWithinQuery);
  }

  if ('$geoIntersects' in condition) {
    return matchesGeoIntersectsCondition(value, condition as GeoIntersectsQuery);
  }

  return false;
}

// ============================================================================
// Index-Optimized Query Execution
// ============================================================================

/**
 * Execute a geo query using an index for optimization
 */
export function executeGeoQuery(
  geoIndex: GeoIndex,
  query: GeoQuery
): GeoDistanceResult[] | string[] {
  if ('$near' in query || '$nearSphere' in query) {
    const parsed = parseNearQuery(query as NearQuery | NearSphereQuery);
    if (!parsed) return [];
    return geoIndex.findNear(parsed.point, parsed.options);
  }

  if ('$geoWithin' in query) {
    const options = (query as GeoWithinQuery).$geoWithin;
    return geoIndex.findWithin(options);
  }

  if ('$geoIntersects' in query) {
    const options = (query as GeoIntersectsQuery).$geoIntersects;
    return geoIndex.findIntersects(options);
  }

  return [];
}

/**
 * Get matching document IDs for a geo query condition using an index
 */
export function getGeoMatchingDocIds(
  geoIndex: GeoIndex,
  condition: Record<string, unknown>
): string[] {
  const results = executeGeoQuery(geoIndex, condition as GeoQuery);

  // Results may be GeoDistanceResult[] or string[]
  if (results.length > 0 && typeof results[0] === 'object') {
    return (results as GeoDistanceResult[]).map(r => r.docId);
  }

  return results as string[];
}

// ============================================================================
// Distance Calculation for Results
// ============================================================================

/**
 * Calculate distance from a point for $near result sorting/projection
 */
export function calculateGeoDistance(
  docValue: unknown,
  point: [number, number],
  spherical: boolean
): number | null {
  const coords = parseCoordinates(docValue);
  if (!coords) {
    return null;
  }

  return spherical
    ? haversineDistance(coords, point)
    : euclideanDistance(coords, point);
}

// ============================================================================
// Exports
// ============================================================================

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
  radiansToMeters,
  type GeoJSONPoint,
  type GeoJSONPolygon,
  type GeoJSONGeometry,
  type NearQueryOptions,
  type GeoWithinOptions,
  type GeoIntersectsOptions,
  type GeoDistanceResult,
};
