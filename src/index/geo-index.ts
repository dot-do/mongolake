/**
 * Geospatial Index
 *
 * MongoDB-compatible geospatial indexing implementation.
 * Supports 2d (flat) and 2dsphere (spherical) coordinate systems.
 *
 * Features:
 * - $near and $nearSphere operators for proximity queries
 * - $geoWithin with $box, $polygon, $center, $centerSphere
 * - $geoIntersects operator for geometry intersection
 * - GeoJSON document support (Point, LineString, Polygon)
 * - Distance calculation (Haversine for sphere, Euclidean for 2d)
 */

// ============================================================================
// Constants
// ============================================================================

/** Earth radius in meters */
export const EARTH_RADIUS_METERS = 6371000;

/** Earth radius in kilometers */
export const EARTH_RADIUS_KM = 6371;

/** Earth radius in miles */
export const EARTH_RADIUS_MILES = 3958.8;

/** Degrees to radians conversion factor */
const DEG_TO_RAD = Math.PI / 180;

/** Radians to degrees conversion factor (exported for potential future use) */
export const RAD_TO_DEG = 180 / Math.PI;

// ============================================================================
// Types
// ============================================================================

/** GeoJSON Point type */
export interface GeoJSONPoint {
  type: 'Point';
  coordinates: [number, number]; // [longitude, latitude]
}

/** GeoJSON LineString type */
export interface GeoJSONLineString {
  type: 'LineString';
  coordinates: [number, number][];
}

/** GeoJSON Polygon type */
export interface GeoJSONPolygon {
  type: 'Polygon';
  coordinates: [number, number][][]; // Array of rings, each ring is array of [lng, lat]
}

/** GeoJSON MultiPoint type */
export interface GeoJSONMultiPoint {
  type: 'MultiPoint';
  coordinates: [number, number][];
}

/** GeoJSON MultiLineString type */
export interface GeoJSONMultiLineString {
  type: 'MultiLineString';
  coordinates: [number, number][][];
}

/** GeoJSON MultiPolygon type */
export interface GeoJSONMultiPolygon {
  type: 'MultiPolygon';
  coordinates: [number, number][][][];
}

/** GeoJSON GeometryCollection type */
export interface GeoJSONGeometryCollection {
  type: 'GeometryCollection';
  geometries: GeoJSONGeometry[];
}

/** Union of all supported GeoJSON geometry types */
export type GeoJSONGeometry =
  | GeoJSONPoint
  | GeoJSONLineString
  | GeoJSONPolygon
  | GeoJSONMultiPoint
  | GeoJSONMultiLineString
  | GeoJSONMultiPolygon
  | GeoJSONGeometryCollection;

/** Legacy coordinate pair [longitude, latitude] or [x, y] */
export type LegacyCoordinates = [number, number];

/** Legacy embedded object with coordinate fields */
export interface LegacyCoordinateObject {
  lng?: number;
  lon?: number;
  longitude?: number;
  x?: number;
  lat?: number;
  latitude?: number;
  y?: number;
}

/** Valid coordinate input types */
export type CoordinateInput = GeoJSONPoint | LegacyCoordinates | LegacyCoordinateObject;

/** Geo index type */
export type GeoIndexType = '2d' | '2dsphere';

/** Geo index entry */
export interface GeoIndexEntry {
  docId: string;
  coordinates: [number, number]; // [longitude, latitude] or [x, y]
  geometry?: GeoJSONGeometry;
}

/** Geo index metadata */
export interface GeoIndexMetadata {
  name: string;
  field: string;
  type: GeoIndexType;
  min?: number;
  max?: number;
  bits?: number;
  createdAt: string;
}

/** Serialized geo index for persistence */
export interface SerializedGeoIndex {
  metadata: GeoIndexMetadata;
  entries: GeoIndexEntry[];
}

/** $near query options */
export interface NearQueryOptions {
  /** Maximum distance in meters (for 2dsphere) or units (for 2d) */
  $maxDistance?: number;
  /** Minimum distance in meters (for 2dsphere) or units (for 2d) */
  $minDistance?: number;
  /** Geometry to search from */
  $geometry?: GeoJSONPoint;
}

/** $geoWithin query options */
export interface GeoWithinOptions {
  $box?: [[number, number], [number, number]]; // [[bottom-left], [top-right]]
  $polygon?: [number, number][]; // Array of [lng, lat] points
  $center?: [[number, number], number]; // [[x, y], radius]
  $centerSphere?: [[number, number], number]; // [[lng, lat], radius in radians]
  $geometry?: GeoJSONPolygon;
}

/** $geoIntersects query options */
export interface GeoIntersectsOptions {
  $geometry: GeoJSONGeometry;
}

/** Distance result with document and distance */
export interface GeoDistanceResult {
  docId: string;
  distance: number;
}

// ============================================================================
// Coordinate Utilities
// ============================================================================

/**
 * Parse coordinate input into [longitude, latitude] format
 */
export function parseCoordinates(input: unknown): [number, number] | null {
  if (!input) {
    return null;
  }

  // GeoJSON Point
  if (isGeoJSONPoint(input)) {
    return input.coordinates;
  }

  // Legacy array [lng, lat]
  if (Array.isArray(input) && input.length >= 2) {
    const [lng, lat] = input;
    if (typeof lng === 'number' && typeof lat === 'number') {
      return [lng, lat];
    }
  }

  // Legacy object with coordinate fields
  if (typeof input === 'object' && input !== null) {
    const obj = input as LegacyCoordinateObject;
    const lng = obj.lng ?? obj.lon ?? obj.longitude ?? obj.x;
    const lat = obj.lat ?? obj.latitude ?? obj.y;
    if (typeof lng === 'number' && typeof lat === 'number') {
      return [lng, lat];
    }
  }

  return null;
}

/**
 * Check if a value is a GeoJSON Point
 */
export function isGeoJSONPoint(value: unknown): value is GeoJSONPoint {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as GeoJSONPoint).type === 'Point' &&
    Array.isArray((value as GeoJSONPoint).coordinates) &&
    (value as GeoJSONPoint).coordinates.length === 2
  );
}

/**
 * Check if a value is a GeoJSON Polygon
 */
export function isGeoJSONPolygon(value: unknown): value is GeoJSONPolygon {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as GeoJSONPolygon).type === 'Polygon' &&
    Array.isArray((value as GeoJSONPolygon).coordinates)
  );
}

/**
 * Check if a value is any GeoJSON geometry
 */
export function isGeoJSONGeometry(value: unknown): value is GeoJSONGeometry {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const geom = value as { type?: string };
  return ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'].includes(geom.type || '');
}

// ============================================================================
// Distance Calculations
// ============================================================================

/**
 * Calculate Euclidean distance between two 2D points
 * Used for 2d indexes (flat coordinate system)
 */
export function euclideanDistance(
  p1: [number, number],
  p2: [number, number]
): number {
  const dx = p2[0] - p1[0];
  const dy = p2[1] - p1[1];
  return Math.sqrt(dx * dx + dy * dy);
}

/**
 * Calculate Haversine distance between two points on a sphere
 * Used for 2dsphere indexes (spherical coordinate system)
 *
 * @param p1 - First point [longitude, latitude] in degrees
 * @param p2 - Second point [longitude, latitude] in degrees
 * @param radius - Sphere radius (default: Earth radius in meters)
 * @returns Distance in the same units as the radius
 */
export function haversineDistance(
  p1: [number, number],
  p2: [number, number],
  radius: number = EARTH_RADIUS_METERS
): number {
  const [lng1, lat1] = p1;
  const [lng2, lat2] = p2;

  const dLat = (lat2 - lat1) * DEG_TO_RAD;
  const dLng = (lng2 - lng1) * DEG_TO_RAD;

  const lat1Rad = lat1 * DEG_TO_RAD;
  const lat2Rad = lat2 * DEG_TO_RAD;

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(dLng / 2) * Math.sin(dLng / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return radius * c;
}

/**
 * Convert distance in meters to radians (for spherical calculations)
 */
export function metersToRadians(meters: number): number {
  return meters / EARTH_RADIUS_METERS;
}

/**
 * Convert radians to meters
 */
export function radiansToMeters(radians: number): number {
  return radians * EARTH_RADIUS_METERS;
}

// ============================================================================
// Geometric Operations
// ============================================================================

/**
 * Check if a point is inside a polygon using ray casting algorithm
 */
export function pointInPolygon(
  point: [number, number],
  polygon: [number, number][]
): boolean {
  const [x, y] = point;
  let inside = false;

  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const [xi, yi] = polygon[i]!;
    const [xj, yj] = polygon[j]!;

    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }

  return inside;
}

/**
 * Check if a point is inside a box
 */
export function pointInBox(
  point: [number, number],
  bottomLeft: [number, number],
  topRight: [number, number]
): boolean {
  const [x, y] = point;
  const [minX, minY] = bottomLeft;
  const [maxX, maxY] = topRight;
  return x >= minX && x <= maxX && y >= minY && y <= maxY;
}

/**
 * Check if a point is inside a circle
 */
export function pointInCircle(
  point: [number, number],
  center: [number, number],
  radius: number,
  spherical: boolean = false
): boolean {
  const distance = spherical
    ? haversineDistance(point, center)
    : euclideanDistance(point, center);
  return distance <= radius;
}

/**
 * Check if two line segments intersect
 */
function lineSegmentsIntersect(
  p1: [number, number],
  p2: [number, number],
  p3: [number, number],
  p4: [number, number]
): boolean {
  const d1 = direction(p3, p4, p1);
  const d2 = direction(p3, p4, p2);
  const d3 = direction(p1, p2, p3);
  const d4 = direction(p1, p2, p4);

  if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
      ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
    return true;
  }

  if (d1 === 0 && onSegment(p3, p4, p1)) return true;
  if (d2 === 0 && onSegment(p3, p4, p2)) return true;
  if (d3 === 0 && onSegment(p1, p2, p3)) return true;
  if (d4 === 0 && onSegment(p1, p2, p4)) return true;

  return false;
}

function direction(p1: [number, number], p2: [number, number], p3: [number, number]): number {
  return (p3[0] - p1[0]) * (p2[1] - p1[1]) - (p2[0] - p1[0]) * (p3[1] - p1[1]);
}

function onSegment(p1: [number, number], p2: [number, number], p3: [number, number]): boolean {
  return (
    Math.min(p1[0], p2[0]) <= p3[0] &&
    p3[0] <= Math.max(p1[0], p2[0]) &&
    Math.min(p1[1], p2[1]) <= p3[1] &&
    p3[1] <= Math.max(p1[1], p2[1])
  );
}

/**
 * Check if a polygon intersects with another polygon
 */
export function polygonsIntersect(
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

  // Check if any edges intersect
  for (let i = 0; i < poly1.length; i++) {
    const p1 = poly1[i]!;
    const p2 = poly1[(i + 1) % poly1.length]!;
    for (let j = 0; j < poly2.length; j++) {
      const p3 = poly2[j]!;
      const p4 = poly2[(j + 1) % poly2.length]!;
      if (lineSegmentsIntersect(p1, p2, p3, p4)) {
        return true;
      }
    }
  }

  return false;
}

/**
 * Check if a point intersects with a GeoJSON geometry
 */
export function pointIntersectsGeometry(
  point: [number, number],
  geometry: GeoJSONGeometry
): boolean {
  switch (geometry.type) {
    case 'Point':
      return point[0] === geometry.coordinates[0] && point[1] === geometry.coordinates[1];

    case 'LineString':
      // Check if point is on any segment of the line
      for (let i = 0; i < geometry.coordinates.length - 1; i++) {
        if (pointOnLineSegment(point, geometry.coordinates[i]!, geometry.coordinates[i + 1]!)) {
          return true;
        }
      }
      return false;

    case 'Polygon':
      // Check if point is inside the polygon (first ring is outer, rest are holes)
      if (!pointInPolygon(point, geometry.coordinates[0]!)) {
        return false;
      }
      // Check holes
      for (let i = 1; i < geometry.coordinates.length; i++) {
        if (pointInPolygon(point, geometry.coordinates[i]!)) {
          return false; // Point is in a hole
        }
      }
      return true;

    case 'MultiPoint':
      return geometry.coordinates.some(
        coord => coord[0] === point[0] && coord[1] === point[1]
      );

    case 'MultiLineString':
      return geometry.coordinates.some(line => {
        for (let i = 0; i < line.length - 1; i++) {
          if (pointOnLineSegment(point, line[i]!, line[i + 1]!)) {
            return true;
          }
        }
        return false;
      });

    case 'MultiPolygon':
      return geometry.coordinates.some(polygon => {
        if (!pointInPolygon(point, polygon[0]!)) {
          return false;
        }
        for (let i = 1; i < polygon.length; i++) {
          if (pointInPolygon(point, polygon[i]!)) {
            return false;
          }
        }
        return true;
      });

    case 'GeometryCollection':
      return geometry.geometries.some(geom => pointIntersectsGeometry(point, geom));

    default:
      return false;
  }
}

/**
 * Check if a point is on a line segment
 */
function pointOnLineSegment(
  point: [number, number],
  lineStart: [number, number],
  lineEnd: [number, number],
  tolerance: number = 1e-9
): boolean {
  const d1 = euclideanDistance(point, lineStart);
  const d2 = euclideanDistance(point, lineEnd);
  const lineLength = euclideanDistance(lineStart, lineEnd);
  return Math.abs(d1 + d2 - lineLength) < tolerance;
}

// ============================================================================
// GeoIndex Class
// ============================================================================

/**
 * Geospatial Index for proximity and spatial queries
 *
 * Implements MongoDB-compatible geospatial indexing with:
 * - 2d indexes for flat coordinate systems
 * - 2dsphere indexes for spherical (Earth) coordinates
 * - Support for GeoJSON geometries
 * - $near, $nearSphere, $geoWithin, $geoIntersects operators
 *
 * @example
 * ```typescript
 * // Create a 2dsphere index on location field
 * const geoIndex = new GeoIndex('location_2dsphere', 'location', '2dsphere');
 *
 * // Index documents
 * geoIndex.indexDocument('doc1', {
 *   location: { type: 'Point', coordinates: [-73.97, 40.77] }
 * });
 *
 * // Find nearby documents
 * const results = geoIndex.findNear([-73.97, 40.77], { $maxDistance: 1000 });
 * ```
 */
export class GeoIndex {
  /** Index name */
  readonly name: string;

  /** Field being indexed */
  readonly field: string;

  /** Index type (2d or 2dsphere) */
  readonly type: GeoIndexType;

  /** Minimum bound for 2d index (default: -180 for longitude) */
  readonly min: number;

  /** Maximum bound for 2d index (default: 180 for longitude) */
  readonly max: number;

  /** Precision bits for geohash (default: 26) */
  readonly bits: number;

  /** Indexed entries */
  private entries: Map<string, GeoIndexEntry> = new Map();

  constructor(
    name: string,
    field: string,
    type: GeoIndexType = '2dsphere',
    options?: { min?: number; max?: number; bits?: number }
  ) {
    this.name = name;
    this.field = field;
    this.type = type;
    this.min = options?.min ?? -180;
    this.max = options?.max ?? 180;
    this.bits = options?.bits ?? 26;
  }

  // --------------------------------------------------------------------------
  // Indexing Operations
  // --------------------------------------------------------------------------

  /**
   * Index a document's geospatial field
   *
   * @param docId - Document ID
   * @param doc - Document containing the geospatial field
   */
  indexDocument(docId: string, doc: Record<string, unknown>): void {
    // Remove existing entry if re-indexing
    this.entries.delete(docId);

    // Get the field value
    const value = this.getFieldValue(doc);
    if (!value) {
      return; // No valid coordinates to index
    }

    const coordinates = parseCoordinates(value);
    if (!coordinates) {
      return;
    }

    // Validate coordinates for 2dsphere
    if (this.type === '2dsphere') {
      const [lng, lat] = coordinates;
      if (lat < -90 || lat > 90) {
        throw new Error(`Invalid latitude: ${lat}. Must be between -90 and 90.`);
      }
      if (lng < -180 || lng > 180) {
        throw new Error(`Invalid longitude: ${lng}. Must be between -180 and 180.`);
      }
    }

    // Store the entry
    const entry: GeoIndexEntry = {
      docId,
      coordinates,
    };

    // Store full geometry if it's GeoJSON
    if (isGeoJSONGeometry(value)) {
      entry.geometry = value as GeoJSONGeometry;
    }

    this.entries.set(docId, entry);
  }

  /**
   * Remove a document from the index
   *
   * @param docId - Document ID to remove
   */
  unindexDocument(docId: string): void {
    this.entries.delete(docId);
  }

  /**
   * Check if a document is indexed
   */
  hasDocument(docId: string): boolean {
    return this.entries.has(docId);
  }

  /**
   * Get the number of indexed documents
   */
  get size(): number {
    return this.entries.size;
  }

  /**
   * Check if the index is empty
   */
  get isEmpty(): boolean {
    return this.entries.size === 0;
  }

  /**
   * Clear all entries from the index
   */
  clear(): void {
    this.entries.clear();
  }

  // --------------------------------------------------------------------------
  // Query Operations
  // --------------------------------------------------------------------------

  /**
   * Find documents near a point ($near / $nearSphere)
   *
   * @param point - Reference point [longitude, latitude]
   * @param options - Query options with $maxDistance and $minDistance
   * @returns Array of document IDs sorted by distance (nearest first)
   */
  findNear(
    point: [number, number],
    options: NearQueryOptions = {}
  ): GeoDistanceResult[] {
    const results: GeoDistanceResult[] = [];
    const maxDistance = options.$maxDistance ?? Infinity;
    const minDistance = options.$minDistance ?? 0;

    for (const entry of this.entries.values()) {
      const distance = this.calculateDistance(point, entry.coordinates);

      if (distance >= minDistance && distance <= maxDistance) {
        results.push({
          docId: entry.docId,
          distance,
        });
      }
    }

    // Sort by distance (nearest first)
    results.sort((a, b) => a.distance - b.distance);

    return results;
  }

  /**
   * Find documents within a geometry ($geoWithin)
   *
   * @param options - Geometry specification ($box, $polygon, $center, $centerSphere, or $geometry)
   * @returns Array of matching document IDs
   */
  findWithin(options: GeoWithinOptions): string[] {
    const results: string[] = [];

    for (const entry of this.entries.values()) {
      let matches = false;

      if (options.$box) {
        matches = pointInBox(entry.coordinates, options.$box[0], options.$box[1]);
      } else if (options.$polygon) {
        matches = pointInPolygon(entry.coordinates, options.$polygon);
      } else if (options.$center) {
        // $center uses flat distance
        const [center, radius] = options.$center;
        matches = pointInCircle(entry.coordinates, center, radius, false);
      } else if (options.$centerSphere) {
        // $centerSphere uses spherical distance with radius in radians
        const [center, radiusRadians] = options.$centerSphere;
        const radiusMeters = radiansToMeters(radiusRadians);
        matches = pointInCircle(entry.coordinates, center, radiusMeters, true);
      } else if (options.$geometry && isGeoJSONPolygon(options.$geometry)) {
        // GeoJSON polygon
        const outerRing = options.$geometry.coordinates[0]!;
        matches = pointInPolygon(entry.coordinates, outerRing);
        // Check holes
        for (let i = 1; i < options.$geometry.coordinates.length; i++) {
          if (pointInPolygon(entry.coordinates, options.$geometry.coordinates[i]!)) {
            matches = false;
            break;
          }
        }
      }

      if (matches) {
        results.push(entry.docId);
      }
    }

    return results;
  }

  /**
   * Find documents that intersect with a geometry ($geoIntersects)
   *
   * @param options - Geometry specification
   * @returns Array of matching document IDs
   */
  findIntersects(options: GeoIntersectsOptions): string[] {
    const results: string[] = [];

    for (const entry of this.entries.values()) {
      let matches = false;

      // Check if the indexed geometry intersects with the query geometry
      if (entry.geometry) {
        matches = this.geometriesIntersect(entry.geometry, options.$geometry);
      } else {
        // Point intersection
        matches = pointIntersectsGeometry(entry.coordinates, options.$geometry);
      }

      if (matches) {
        results.push(entry.docId);
      }
    }

    return results;
  }

  /**
   * Get all matching document IDs for a geo query condition
   */
  getMatchingDocIds(condition: Record<string, unknown>): string[] {
    if ('$near' in condition || '$nearSphere' in condition) {
      const nearOpts = (condition.$near || condition.$nearSphere) as NearQueryOptions;
      let point: [number, number];

      if (nearOpts.$geometry) {
        point = nearOpts.$geometry.coordinates;
      } else if (Array.isArray(condition.$near) || Array.isArray(condition.$nearSphere)) {
        point = (condition.$near || condition.$nearSphere) as [number, number];
      } else {
        return [];
      }

      const results = this.findNear(point, nearOpts);
      return results.map(r => r.docId);
    }

    if ('$geoWithin' in condition) {
      return this.findWithin(condition.$geoWithin as GeoWithinOptions);
    }

    if ('$geoIntersects' in condition) {
      return this.findIntersects(condition.$geoIntersects as GeoIntersectsOptions);
    }

    return [];
  }

  // --------------------------------------------------------------------------
  // Distance Helpers
  // --------------------------------------------------------------------------

  /**
   * Calculate distance between two points using the appropriate method
   */
  calculateDistance(p1: [number, number], p2: [number, number]): number {
    if (this.type === '2dsphere') {
      return haversineDistance(p1, p2);
    }
    return euclideanDistance(p1, p2);
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * Get the geospatial field value from a document
   */
  private getFieldValue(doc: Record<string, unknown>): unknown {
    const parts = this.field.split('.');
    let value: unknown = doc;

    for (const part of parts) {
      if (value === null || value === undefined) {
        return undefined;
      }
      if (typeof value === 'object') {
        value = (value as Record<string, unknown>)[part];
      } else {
        return undefined;
      }
    }

    return value;
  }

  /**
   * Check if two geometries intersect
   */
  private geometriesIntersect(g1: GeoJSONGeometry, g2: GeoJSONGeometry): boolean {
    // Simplified intersection check - primarily handles Point and Polygon cases
    if (g1.type === 'Point') {
      return pointIntersectsGeometry(g1.coordinates, g2);
    }

    if (g2.type === 'Point') {
      return pointIntersectsGeometry(g2.coordinates, g1);
    }

    if (g1.type === 'Polygon' && g2.type === 'Polygon') {
      return polygonsIntersect(g1.coordinates[0]!, g2.coordinates[0]!);
    }

    // For more complex cases, do a simplified bounds check
    return this.boundingBoxesIntersect(g1, g2);
  }

  /**
   * Check if bounding boxes of two geometries intersect
   */
  private boundingBoxesIntersect(g1: GeoJSONGeometry, g2: GeoJSONGeometry): boolean {
    const bbox1 = this.getBoundingBox(g1);
    const bbox2 = this.getBoundingBox(g2);

    if (!bbox1 || !bbox2) return false;

    return !(
      bbox1.maxX < bbox2.minX ||
      bbox1.minX > bbox2.maxX ||
      bbox1.maxY < bbox2.minY ||
      bbox1.minY > bbox2.maxY
    );
  }

  /**
   * Get bounding box for a geometry
   */
  private getBoundingBox(geometry: GeoJSONGeometry): {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
  } | null {
    const coords = this.extractAllCoordinates(geometry);
    if (coords.length === 0) return null;

    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;

    for (const [x, y] of coords) {
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
    }

    return { minX, minY, maxX, maxY };
  }

  /**
   * Extract all coordinates from a geometry
   */
  private extractAllCoordinates(geometry: GeoJSONGeometry): [number, number][] {
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
        return geometry.geometries.flatMap(g => this.extractAllCoordinates(g));
      default:
        return [];
    }
  }

  // --------------------------------------------------------------------------
  // Serialization
  // --------------------------------------------------------------------------

  /**
   * Serialize the index for persistence
   */
  serialize(): SerializedGeoIndex {
    return {
      metadata: {
        name: this.name,
        field: this.field,
        type: this.type,
        min: this.min,
        max: this.max,
        bits: this.bits,
        createdAt: new Date().toISOString(),
      },
      entries: Array.from(this.entries.values()),
    };
  }

  /**
   * Deserialize a geo index from storage
   */
  static deserialize(data: SerializedGeoIndex): GeoIndex {
    const geoIndex = new GeoIndex(
      data.metadata.name,
      data.metadata.field,
      data.metadata.type,
      {
        min: data.metadata.min,
        max: data.metadata.max,
        bits: data.metadata.bits,
      }
    );

    for (const entry of data.entries) {
      geoIndex.entries.set(entry.docId, entry);
    }

    return geoIndex;
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.serialize());
  }

  /**
   * Create from JSON string
   */
  static fromJSON(json: string): GeoIndex {
    return GeoIndex.deserialize(JSON.parse(json));
  }
}

// ============================================================================
// Exports
// ============================================================================

export default GeoIndex;
