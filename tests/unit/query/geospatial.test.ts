/**
 * Geospatial Query Tests
 *
 * Comprehensive tests for MongoDB-compatible geospatial query operators.
 *
 * Features covered:
 * - $near and $nearSphere operators
 * - $geoWithin with $box, $polygon, $center, $centerSphere
 * - $geoIntersects operator
 * - 2d and 2dsphere index types
 * - GeoJSON document support
 * - Distance calculation (Haversine for sphere, Euclidean for 2d)
 * - Query planner integration for geo indexes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  GeoIndex,
  parseCoordinates,
  isGeoJSONPoint,
  isGeoJSONPolygon,
  euclideanDistance,
  haversineDistance,
  pointInBox,
  pointInPolygon,
  pointInCircle,
  pointIntersectsGeometry,
  metersToRadians,
  radiansToMeters,
  EARTH_RADIUS_METERS,
  type GeoJSONPoint,
  type GeoJSONPolygon,
} from '../../../src/index/geo-index.js';
import {
  isGeoQuery,
  hasGeoQuery,
  extractGeoQuery,
  parseNearQuery,
  matchesNearCondition,
  matchesGeoWithinCondition,
  matchesGeoIntersectsCondition,
  matchesGeoCondition,
  executeGeoQuery,
  calculateGeoDistance,
} from '../../../src/query/geospatial.js';
import { matchesFilter } from '../../../src/utils/filter.js';
import type { Document, Filter } from '../../../src/types.js';

// ============================================================================
// Test Data
// ============================================================================

interface LocationDocument extends Document {
  _id: string;
  name: string;
  location: GeoJSONPoint | [number, number];
  category?: string;
}

// Sample locations (using real NYC coordinates)
const NYC_LOCATIONS = {
  timesSquare: { type: 'Point' as const, coordinates: [-73.9857, 40.7580] as [number, number] },
  empirePState: { type: 'Point' as const, coordinates: [-73.9857, 40.7484] as [number, number] },
  centralPark: { type: 'Point' as const, coordinates: [-73.9654, 40.7829] as [number, number] },
  brooklynBridge: { type: 'Point' as const, coordinates: [-73.9969, 40.7061] as [number, number] },
  statuOfLiberty: { type: 'Point' as const, coordinates: [-74.0445, 40.6892] as [number, number] },
};

// ============================================================================
// Coordinate Parsing Tests
// ============================================================================

describe('Coordinate Parsing', () => {
  describe('parseCoordinates', () => {
    it('should parse GeoJSON Point', () => {
      const point: GeoJSONPoint = { type: 'Point', coordinates: [-73.97, 40.77] };
      expect(parseCoordinates(point)).toEqual([-73.97, 40.77]);
    });

    it('should parse legacy coordinate array', () => {
      expect(parseCoordinates([-73.97, 40.77])).toEqual([-73.97, 40.77]);
    });

    it('should parse legacy coordinate object with lng/lat', () => {
      expect(parseCoordinates({ lng: -73.97, lat: 40.77 })).toEqual([-73.97, 40.77]);
    });

    it('should parse legacy coordinate object with lon/lat', () => {
      expect(parseCoordinates({ lon: -73.97, lat: 40.77 })).toEqual([-73.97, 40.77]);
    });

    it('should parse legacy coordinate object with longitude/latitude', () => {
      expect(parseCoordinates({ longitude: -73.97, latitude: 40.77 })).toEqual([-73.97, 40.77]);
    });

    it('should parse legacy coordinate object with x/y', () => {
      expect(parseCoordinates({ x: 10, y: 20 })).toEqual([10, 20]);
    });

    it('should return null for invalid input', () => {
      expect(parseCoordinates(null)).toBeNull();
      expect(parseCoordinates(undefined)).toBeNull();
      expect(parseCoordinates('invalid')).toBeNull();
      expect(parseCoordinates({})).toBeNull();
      expect(parseCoordinates([1])).toBeNull();
    });
  });

  describe('isGeoJSONPoint', () => {
    it('should return true for valid GeoJSON Point', () => {
      expect(isGeoJSONPoint({ type: 'Point', coordinates: [-73.97, 40.77] })).toBe(true);
    });

    it('should return false for invalid input', () => {
      expect(isGeoJSONPoint(null)).toBe(false);
      expect(isGeoJSONPoint({ type: 'LineString', coordinates: [] })).toBe(false);
      expect(isGeoJSONPoint({ type: 'Point', coordinates: [1] })).toBe(false);
      expect(isGeoJSONPoint([-73.97, 40.77])).toBe(false);
    });
  });

  describe('isGeoJSONPolygon', () => {
    it('should return true for valid GeoJSON Polygon', () => {
      const polygon: GeoJSONPolygon = {
        type: 'Polygon',
        coordinates: [[[-74, 40], [-73, 40], [-73, 41], [-74, 41], [-74, 40]]],
      };
      expect(isGeoJSONPolygon(polygon)).toBe(true);
    });

    it('should return false for invalid input', () => {
      expect(isGeoJSONPolygon(null)).toBe(false);
      expect(isGeoJSONPolygon({ type: 'Point', coordinates: [0, 0] })).toBe(false);
    });
  });
});

// ============================================================================
// Distance Calculation Tests
// ============================================================================

describe('Distance Calculations', () => {
  describe('euclideanDistance', () => {
    it('should calculate 2D Euclidean distance', () => {
      expect(euclideanDistance([0, 0], [3, 4])).toBe(5);
      expect(euclideanDistance([0, 0], [0, 0])).toBe(0);
      expect(euclideanDistance([1, 1], [4, 5])).toBe(5);
    });

    it('should handle negative coordinates', () => {
      expect(euclideanDistance([-1, -1], [2, 3])).toBe(5);
    });
  });

  describe('haversineDistance', () => {
    it('should calculate spherical distance between two points', () => {
      // Distance from Times Square to Empire State Building (~1.1 km)
      const distance = haversineDistance(
        NYC_LOCATIONS.timesSquare.coordinates,
        NYC_LOCATIONS.empirePState.coordinates
      );
      expect(distance).toBeGreaterThan(1000);
      expect(distance).toBeLessThan(1200);
    });

    it('should return 0 for same point', () => {
      const point: [number, number] = [-73.97, 40.77];
      expect(haversineDistance(point, point)).toBe(0);
    });

    it('should calculate distance between antipodal points', () => {
      // New York to opposite side of Earth
      const distance = haversineDistance([0, 0], [180, 0]);
      expect(distance).toBeCloseTo(Math.PI * EARTH_RADIUS_METERS, -3);
    });
  });

  describe('metersToRadians and radiansToMeters', () => {
    it('should convert meters to radians', () => {
      const meters = EARTH_RADIUS_METERS;
      expect(metersToRadians(meters)).toBeCloseTo(1, 5);
    });

    it('should convert radians to meters', () => {
      const radians = 1;
      expect(radiansToMeters(radians)).toBe(EARTH_RADIUS_METERS);
    });

    it('should be reversible', () => {
      const meters = 10000;
      expect(radiansToMeters(metersToRadians(meters))).toBeCloseTo(meters, 5);
    });
  });
});

// ============================================================================
// Geometric Operations Tests
// ============================================================================

describe('Geometric Operations', () => {
  describe('pointInBox', () => {
    it('should return true for point inside box', () => {
      expect(pointInBox([0, 0], [-1, -1], [1, 1])).toBe(true);
      expect(pointInBox([0.5, 0.5], [0, 0], [1, 1])).toBe(true);
    });

    it('should return true for point on box edge', () => {
      expect(pointInBox([0, 0], [0, 0], [1, 1])).toBe(true);
      expect(pointInBox([1, 1], [0, 0], [1, 1])).toBe(true);
    });

    it('should return false for point outside box', () => {
      expect(pointInBox([2, 2], [0, 0], [1, 1])).toBe(false);
      expect(pointInBox([-1, 0], [0, 0], [1, 1])).toBe(false);
    });
  });

  describe('pointInPolygon', () => {
    const square: [number, number][] = [[0, 0], [10, 0], [10, 10], [0, 10]];
    const triangle: [number, number][] = [[0, 0], [10, 0], [5, 10]];

    it('should return true for point inside polygon', () => {
      expect(pointInPolygon([5, 5], square)).toBe(true);
      expect(pointInPolygon([5, 3], triangle)).toBe(true);
    });

    it('should return false for point outside polygon', () => {
      expect(pointInPolygon([15, 5], square)).toBe(false);
      expect(pointInPolygon([0, 15], triangle)).toBe(false);
    });

    it('should handle complex polygon shapes', () => {
      // L-shaped polygon
      const lShape: [number, number][] = [
        [0, 0], [5, 0], [5, 5], [10, 5], [10, 10], [0, 10],
      ];
      expect(pointInPolygon([2, 2], lShape)).toBe(true);
      expect(pointInPolygon([8, 8], lShape)).toBe(true);
      expect(pointInPolygon([7, 2], lShape)).toBe(false);
    });
  });

  describe('pointInCircle', () => {
    it('should return true for point inside circle (2D)', () => {
      expect(pointInCircle([0, 0], [0, 0], 1, false)).toBe(true);
      expect(pointInCircle([0.5, 0], [0, 0], 1, false)).toBe(true);
    });

    it('should return true for point on circle edge', () => {
      expect(pointInCircle([1, 0], [0, 0], 1, false)).toBe(true);
    });

    it('should return false for point outside circle', () => {
      expect(pointInCircle([2, 0], [0, 0], 1, false)).toBe(false);
    });

    it('should work with spherical distance', () => {
      // Point within 1km of Times Square
      const nearbyPoint: [number, number] = [-73.9850, 40.7575];
      const radiusMeters = 1000;
      expect(pointInCircle(
        nearbyPoint,
        NYC_LOCATIONS.timesSquare.coordinates,
        radiusMeters,
        true
      )).toBe(true);
    });
  });
});

// ============================================================================
// GeoIndex Tests
// ============================================================================

describe('GeoIndex', () => {
  let geoIndex: GeoIndex;

  beforeEach(() => {
    geoIndex = new GeoIndex('location_2dsphere', 'location', '2dsphere');
  });

  describe('constructor', () => {
    it('should create a 2dsphere index', () => {
      expect(geoIndex.name).toBe('location_2dsphere');
      expect(geoIndex.field).toBe('location');
      expect(geoIndex.type).toBe('2dsphere');
    });

    it('should create a 2d index with custom bounds', () => {
      const index2d = new GeoIndex('coords_2d', 'coords', '2d', { min: 0, max: 100 });
      expect(index2d.type).toBe('2d');
      expect(index2d.min).toBe(0);
      expect(index2d.max).toBe(100);
    });
  });

  describe('indexDocument', () => {
    it('should index document with GeoJSON Point', () => {
      geoIndex.indexDocument('doc1', { location: NYC_LOCATIONS.timesSquare });
      expect(geoIndex.hasDocument('doc1')).toBe(true);
      expect(geoIndex.size).toBe(1);
    });

    it('should index document with legacy coordinates', () => {
      geoIndex.indexDocument('doc1', { location: [-73.97, 40.77] });
      expect(geoIndex.hasDocument('doc1')).toBe(true);
    });

    it('should skip documents without valid coordinates', () => {
      geoIndex.indexDocument('doc1', { location: 'invalid' });
      expect(geoIndex.hasDocument('doc1')).toBe(false);
    });

    it('should throw for invalid latitude', () => {
      expect(() => {
        geoIndex.indexDocument('doc1', { location: [-73.97, 95] }); // lat > 90
      }).toThrow('Invalid latitude');
    });

    it('should throw for invalid longitude', () => {
      expect(() => {
        geoIndex.indexDocument('doc1', { location: [-185, 40.77] }); // lng < -180
      }).toThrow('Invalid longitude');
    });
  });

  describe('unindexDocument', () => {
    it('should remove document from index', () => {
      geoIndex.indexDocument('doc1', { location: NYC_LOCATIONS.timesSquare });
      expect(geoIndex.hasDocument('doc1')).toBe(true);

      geoIndex.unindexDocument('doc1');
      expect(geoIndex.hasDocument('doc1')).toBe(false);
    });
  });

  describe('findNear', () => {
    beforeEach(() => {
      geoIndex.indexDocument('timesSquare', { location: NYC_LOCATIONS.timesSquare });
      geoIndex.indexDocument('empirePState', { location: NYC_LOCATIONS.empirePState });
      geoIndex.indexDocument('centralPark', { location: NYC_LOCATIONS.centralPark });
      geoIndex.indexDocument('brooklynBridge', { location: NYC_LOCATIONS.brooklynBridge });
      geoIndex.indexDocument('statueOfLiberty', { location: NYC_LOCATIONS.statuOfLiberty });
    });

    it('should find nearby documents sorted by distance', () => {
      const results = geoIndex.findNear(NYC_LOCATIONS.timesSquare.coordinates);

      expect(results.length).toBe(5);
      expect(results[0]!.docId).toBe('timesSquare');
      expect(results[0]!.distance).toBe(0);
      // Empire State is closer than others
      expect(results[1]!.docId).toBe('empirePState');
    });

    it('should filter by maxDistance', () => {
      const results = geoIndex.findNear(
        NYC_LOCATIONS.timesSquare.coordinates,
        { $maxDistance: 2000 } // 2km
      );

      // Only Times Square and Empire State are within 2km
      expect(results.length).toBe(2);
    });

    it('should filter by minDistance', () => {
      const results = geoIndex.findNear(
        NYC_LOCATIONS.timesSquare.coordinates,
        { $minDistance: 1000 } // 1km
      );

      // Times Square itself should be excluded
      expect(results.find(r => r.docId === 'timesSquare')).toBeUndefined();
    });

    it('should filter by both minDistance and maxDistance', () => {
      const results = geoIndex.findNear(
        NYC_LOCATIONS.timesSquare.coordinates,
        { $minDistance: 500, $maxDistance: 5000 }
      );

      // Should include Empire State and Central Park, but not Times Square or further locations
      expect(results.find(r => r.docId === 'timesSquare')).toBeUndefined();
    });
  });

  describe('findWithin - $box', () => {
    beforeEach(() => {
      geoIndex.indexDocument('doc1', { location: [5, 5] });
      geoIndex.indexDocument('doc2', { location: [15, 15] });
      geoIndex.indexDocument('doc3', { location: [25, 25] });
    });

    it('should find documents within box', () => {
      // 2d index for simpler testing
      const index2d = new GeoIndex('test', 'location', '2d');
      index2d.indexDocument('doc1', { location: [5, 5] });
      index2d.indexDocument('doc2', { location: [15, 15] });
      index2d.indexDocument('doc3', { location: [25, 25] });

      const results = index2d.findWithin({
        $box: [[0, 0], [20, 20]],
      });

      expect(results).toContain('doc1');
      expect(results).toContain('doc2');
      expect(results).not.toContain('doc3');
    });
  });

  describe('findWithin - $polygon', () => {
    it('should find documents within polygon', () => {
      const index2d = new GeoIndex('test', 'location', '2d');
      index2d.indexDocument('doc1', { location: [5, 5] });
      index2d.indexDocument('doc2', { location: [50, 50] });

      const results = index2d.findWithin({
        $polygon: [[0, 0], [10, 0], [10, 10], [0, 10]],
      });

      expect(results).toContain('doc1');
      expect(results).not.toContain('doc2');
    });
  });

  describe('findWithin - $center', () => {
    it('should find documents within circle (2d)', () => {
      const index2d = new GeoIndex('test', 'location', '2d');
      index2d.indexDocument('doc1', { location: [0, 0] });
      index2d.indexDocument('doc2', { location: [5, 0] });
      index2d.indexDocument('doc3', { location: [15, 0] });

      const results = index2d.findWithin({
        $center: [[0, 0], 10],
      });

      expect(results).toContain('doc1');
      expect(results).toContain('doc2');
      expect(results).not.toContain('doc3');
    });
  });

  describe('findWithin - $centerSphere', () => {
    it('should find documents within spherical circle', () => {
      geoIndex.indexDocument('timesSquare', { location: NYC_LOCATIONS.timesSquare });
      geoIndex.indexDocument('empirePState', { location: NYC_LOCATIONS.empirePState });
      geoIndex.indexDocument('statueOfLiberty', { location: NYC_LOCATIONS.statuOfLiberty });

      // 2km radius in radians
      const radiusRadians = 2000 / EARTH_RADIUS_METERS;

      const results = geoIndex.findWithin({
        $centerSphere: [NYC_LOCATIONS.timesSquare.coordinates, radiusRadians],
      });

      expect(results).toContain('timesSquare');
      expect(results).toContain('empirePState');
      expect(results).not.toContain('statueOfLiberty');
    });
  });

  describe('findWithin - $geometry (GeoJSON Polygon)', () => {
    it('should find documents within GeoJSON polygon', () => {
      const index2d = new GeoIndex('test', 'location', '2d');
      index2d.indexDocument('doc1', { location: [5, 5] });
      index2d.indexDocument('doc2', { location: [50, 50] });

      const results = index2d.findWithin({
        $geometry: {
          type: 'Polygon',
          coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        },
      });

      expect(results).toContain('doc1');
      expect(results).not.toContain('doc2');
    });
  });

  describe('findIntersects', () => {
    it('should find documents that intersect with a polygon', () => {
      geoIndex.indexDocument('timesSquare', { location: NYC_LOCATIONS.timesSquare });
      geoIndex.indexDocument('statueOfLiberty', { location: NYC_LOCATIONS.statuOfLiberty });

      // Polygon around Manhattan
      const manhattanPolygon: GeoJSONPolygon = {
        type: 'Polygon',
        coordinates: [[
          [-74.02, 40.70],
          [-73.93, 40.70],
          [-73.93, 40.80],
          [-74.02, 40.80],
          [-74.02, 40.70],
        ]],
      };

      const results = geoIndex.findIntersects({
        $geometry: manhattanPolygon,
      });

      expect(results).toContain('timesSquare');
      expect(results).not.toContain('statueOfLiberty');
    });
  });

  describe('serialization', () => {
    it('should serialize and deserialize correctly', () => {
      geoIndex.indexDocument('doc1', { location: NYC_LOCATIONS.timesSquare });
      geoIndex.indexDocument('doc2', { location: NYC_LOCATIONS.centralPark });

      const serialized = geoIndex.serialize();
      const deserialized = GeoIndex.deserialize(serialized);

      expect(deserialized.name).toBe(geoIndex.name);
      expect(deserialized.field).toBe(geoIndex.field);
      expect(deserialized.type).toBe(geoIndex.type);
      expect(deserialized.hasDocument('doc1')).toBe(true);
      expect(deserialized.hasDocument('doc2')).toBe(true);
    });

    it('should support JSON serialization', () => {
      geoIndex.indexDocument('doc1', { location: NYC_LOCATIONS.timesSquare });

      const json = geoIndex.toJSON();
      const restored = GeoIndex.fromJSON(json);

      expect(restored.hasDocument('doc1')).toBe(true);
    });
  });
});

// ============================================================================
// Query Detection Tests
// ============================================================================

describe('Query Detection', () => {
  describe('isGeoQuery', () => {
    it('should detect $near query', () => {
      expect(isGeoQuery({ $near: [-73.97, 40.77] })).toBe(true);
    });

    it('should detect $nearSphere query', () => {
      expect(isGeoQuery({ $nearSphere: { type: 'Point', coordinates: [-73.97, 40.77] } })).toBe(true);
    });

    it('should detect $geoWithin query', () => {
      expect(isGeoQuery({ $geoWithin: { $box: [[0, 0], [10, 10]] } })).toBe(true);
    });

    it('should detect $geoIntersects query', () => {
      expect(isGeoQuery({ $geoIntersects: { $geometry: { type: 'Point', coordinates: [0, 0] } } })).toBe(true);
    });

    it('should return false for non-geo queries', () => {
      expect(isGeoQuery({ $eq: 5 })).toBe(false);
      expect(isGeoQuery({ $in: [1, 2, 3] })).toBe(false);
      expect(isGeoQuery(null)).toBe(false);
    });
  });

  describe('hasGeoQuery', () => {
    it('should detect geo query in filter', () => {
      expect(hasGeoQuery({ location: { $near: [-73.97, 40.77] } })).toBe(true);
    });

    it('should detect geo query in $and', () => {
      expect(hasGeoQuery({
        $and: [
          { name: 'test' },
          { location: { $near: [-73.97, 40.77] } },
        ],
      })).toBe(true);
    });

    it('should return false when no geo query', () => {
      expect(hasGeoQuery({ name: 'test', age: { $gt: 20 } })).toBe(false);
    });
  });

  describe('extractGeoQuery', () => {
    it('should extract geo query from filter', () => {
      const result = extractGeoQuery({ location: { $near: [-73.97, 40.77] } });
      expect(result).not.toBeNull();
      expect(result!.field).toBe('location');
      expect(result!.query).toHaveProperty('$near');
    });

    it('should return null when no geo query', () => {
      expect(extractGeoQuery({ name: 'test' })).toBeNull();
    });
  });
});

// ============================================================================
// Query Parsing Tests
// ============================================================================

describe('Query Parsing', () => {
  describe('parseNearQuery', () => {
    it('should parse $near with array coordinates', () => {
      const result = parseNearQuery({ $near: [-73.97, 40.77] });
      expect(result).not.toBeNull();
      expect(result!.point).toEqual([-73.97, 40.77]);
      expect(result!.spherical).toBe(false);
    });

    it('should parse $nearSphere with GeoJSON Point', () => {
      const result = parseNearQuery({
        $nearSphere: { type: 'Point', coordinates: [-73.97, 40.77] },
      });
      expect(result).not.toBeNull();
      expect(result!.point).toEqual([-73.97, 40.77]);
      expect(result!.spherical).toBe(true);
    });

    it('should parse $near with $geometry and options', () => {
      const result = parseNearQuery({
        $near: {
          $geometry: { type: 'Point', coordinates: [-73.97, 40.77] },
          $maxDistance: 1000,
          $minDistance: 100,
        },
      });
      expect(result).not.toBeNull();
      expect(result!.point).toEqual([-73.97, 40.77]);
      expect(result!.options.$maxDistance).toBe(1000);
      expect(result!.options.$minDistance).toBe(100);
    });

    it('should pick up root-level $maxDistance', () => {
      const result = parseNearQuery({
        $near: [-73.97, 40.77],
        $maxDistance: 5000,
      });
      expect(result!.options.$maxDistance).toBe(5000);
    });
  });
});

// ============================================================================
// Query Matching Tests
// ============================================================================

describe('Query Matching', () => {
  describe('matchesNearCondition', () => {
    it('should match document within distance', () => {
      const doc = { location: [-73.9857, 40.7484] }; // Empire State
      const query = {
        $near: NYC_LOCATIONS.timesSquare.coordinates,
        $maxDistance: 2000, // 2km
      };

      expect(matchesNearCondition(doc.location, query)).toBe(true);
    });

    it('should not match document outside distance', () => {
      const doc = { location: NYC_LOCATIONS.statuOfLiberty.coordinates };
      // Use $nearSphere for proper spherical distance calculation
      // Statue of Liberty is ~9km from Times Square
      const query = {
        $nearSphere: NYC_LOCATIONS.timesSquare.coordinates,
        $maxDistance: 1000, // 1km - Statue of Liberty is ~9km away
      };

      expect(matchesNearCondition(doc.location, query)).toBe(false);
    });
  });

  describe('matchesGeoWithinCondition', () => {
    it('should match document within box', () => {
      const doc = { location: [5, 5] };
      const query = {
        $geoWithin: { $box: [[0, 0], [10, 10]] },
      };

      expect(matchesGeoWithinCondition(doc.location, query)).toBe(true);
    });

    it('should not match document outside box', () => {
      const doc = { location: [15, 15] };
      const query = {
        $geoWithin: { $box: [[0, 0], [10, 10]] },
      };

      expect(matchesGeoWithinCondition(doc.location, query)).toBe(false);
    });

    it('should match document within polygon', () => {
      const doc = { location: [5, 5] };
      const query = {
        $geoWithin: { $polygon: [[0, 0], [10, 0], [10, 10], [0, 10]] },
      };

      expect(matchesGeoWithinCondition(doc.location, query)).toBe(true);
    });
  });

  describe('matchesGeoIntersectsCondition', () => {
    it('should match point intersecting polygon', () => {
      const doc = { location: [5, 5] };
      const query = {
        $geoIntersects: {
          $geometry: {
            type: 'Polygon' as const,
            coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
          },
        },
      };

      expect(matchesGeoIntersectsCondition(doc.location, query)).toBe(true);
    });

    it('should not match point outside polygon', () => {
      const doc = { location: [15, 15] };
      const query = {
        $geoIntersects: {
          $geometry: {
            type: 'Polygon' as const,
            coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
          },
        },
      };

      expect(matchesGeoIntersectsCondition(doc.location, query)).toBe(false);
    });
  });
});

// ============================================================================
// matchesFilter Integration Tests
// ============================================================================

describe('matchesFilter with Geospatial', () => {
  it('should match document with $geoWithin $box', () => {
    const doc: LocationDocument = {
      _id: 'doc1',
      name: 'Test Location',
      location: [5, 5],
    };

    const filter = {
      location: { $geoWithin: { $box: [[0, 0], [10, 10]] } },
    };

    expect(matchesFilter(doc, filter as Filter<Document>)).toBe(true);
  });

  it('should not match document outside $geoWithin $box', () => {
    const doc: LocationDocument = {
      _id: 'doc1',
      name: 'Test Location',
      location: [15, 15],
    };

    const filter = {
      location: { $geoWithin: { $box: [[0, 0], [10, 10]] } },
    };

    expect(matchesFilter(doc, filter as Filter<Document>)).toBe(false);
  });

  it('should combine geo query with other conditions', () => {
    const doc: LocationDocument = {
      _id: 'doc1',
      name: 'Coffee Shop',
      location: [5, 5],
      category: 'restaurant',
    };

    const filter = {
      location: { $geoWithin: { $box: [[0, 0], [10, 10]] } },
      category: 'restaurant',
    };

    expect(matchesFilter(doc, filter as Filter<Document>)).toBe(true);
  });

  it('should fail when category does not match', () => {
    const doc: LocationDocument = {
      _id: 'doc1',
      name: 'Coffee Shop',
      location: [5, 5],
      category: 'cafe',
    };

    const filter = {
      location: { $geoWithin: { $box: [[0, 0], [10, 10]] } },
      category: 'restaurant',
    };

    expect(matchesFilter(doc, filter as Filter<Document>)).toBe(false);
  });
});

// ============================================================================
// Index-Optimized Query Execution Tests
// ============================================================================

describe('Index-Optimized Query Execution', () => {
  let geoIndex: GeoIndex;

  beforeEach(() => {
    geoIndex = new GeoIndex('location_2dsphere', 'location', '2dsphere');
    geoIndex.indexDocument('timesSquare', { location: NYC_LOCATIONS.timesSquare });
    geoIndex.indexDocument('empirePState', { location: NYC_LOCATIONS.empirePState });
    geoIndex.indexDocument('centralPark', { location: NYC_LOCATIONS.centralPark });
    geoIndex.indexDocument('brooklynBridge', { location: NYC_LOCATIONS.brooklynBridge });
    geoIndex.indexDocument('statueOfLiberty', { location: NYC_LOCATIONS.statuOfLiberty });
  });

  describe('executeGeoQuery', () => {
    it('should execute $near query using index', () => {
      const results = executeGeoQuery(geoIndex, {
        $near: {
          $geometry: NYC_LOCATIONS.timesSquare,
          $maxDistance: 2000,
        },
      });

      expect(results.length).toBeGreaterThan(0);
      expect(results.length).toBeLessThanOrEqual(5);
    });

    it('should execute $geoWithin query using index', () => {
      // 2d index for box query
      const index2d = new GeoIndex('test', 'location', '2d');
      index2d.indexDocument('doc1', { location: [5, 5] });
      index2d.indexDocument('doc2', { location: [15, 15] });

      const results = executeGeoQuery(index2d, {
        $geoWithin: { $box: [[0, 0], [10, 10]] },
      });

      expect(results).toContain('doc1');
      expect(results).not.toContain('doc2');
    });
  });

  describe('calculateGeoDistance', () => {
    it('should calculate distance for sorting/projection', () => {
      const distance = calculateGeoDistance(
        NYC_LOCATIONS.empirePState,
        NYC_LOCATIONS.timesSquare.coordinates,
        true
      );

      expect(distance).not.toBeNull();
      expect(distance).toBeGreaterThan(1000);
      expect(distance).toBeLessThan(1200);
    });

    it('should return null for invalid coordinates', () => {
      expect(calculateGeoDistance('invalid', [0, 0], true)).toBeNull();
    });
  });
});

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

describe('Edge Cases', () => {
  describe('GeoIndex edge cases', () => {
    it('should handle empty index', () => {
      const geoIndex = new GeoIndex('test', 'location', '2dsphere');
      expect(geoIndex.isEmpty).toBe(true);
      expect(geoIndex.findNear([0, 0])).toEqual([]);
    });

    it('should handle re-indexing same document', () => {
      const geoIndex = new GeoIndex('test', 'location', '2dsphere');
      geoIndex.indexDocument('doc1', { location: [0, 0] });
      geoIndex.indexDocument('doc1', { location: [1, 1] });
      expect(geoIndex.size).toBe(1);
    });

    it('should handle nested location field', () => {
      const geoIndex = new GeoIndex('test', 'address.location', '2dsphere');
      geoIndex.indexDocument('doc1', {
        address: { location: { type: 'Point', coordinates: [0, 0] } },
      });
      expect(geoIndex.hasDocument('doc1')).toBe(true);
    });

    it('should clear all entries', () => {
      const geoIndex = new GeoIndex('test', 'location', '2dsphere');
      geoIndex.indexDocument('doc1', { location: [0, 0] });
      geoIndex.indexDocument('doc2', { location: [1, 1] });
      geoIndex.clear();
      expect(geoIndex.isEmpty).toBe(true);
    });
  });

  describe('Coordinate validation', () => {
    it('should handle coordinates at boundaries', () => {
      const geoIndex = new GeoIndex('test', 'location', '2dsphere');

      // Valid boundary values
      geoIndex.indexDocument('northPole', { location: [0, 90] });
      geoIndex.indexDocument('southPole', { location: [0, -90] });
      geoIndex.indexDocument('dateLine', { location: [180, 0] });
      geoIndex.indexDocument('antiDateLine', { location: [-180, 0] });

      expect(geoIndex.size).toBe(4);
    });
  });
});

// ============================================================================
// Real-world Use Cases
// ============================================================================

describe('Real-world Use Cases', () => {
  describe('Restaurant finder', () => {
    let geoIndex: GeoIndex;

    beforeEach(() => {
      geoIndex = new GeoIndex('restaurants_location', 'location', '2dsphere');

      // Add some restaurants near Times Square
      geoIndex.indexDocument('italian', { location: [-73.985, 40.758], cuisine: 'Italian' });
      geoIndex.indexDocument('japanese', { location: [-73.984, 40.757], cuisine: 'Japanese' });
      geoIndex.indexDocument('mexican', { location: [-73.986, 40.759], cuisine: 'Mexican' });
      geoIndex.indexDocument('farAway', { location: [-74.0, 40.7], cuisine: 'American' });
    });

    it('should find restaurants within walking distance', () => {
      const results = geoIndex.findNear(
        NYC_LOCATIONS.timesSquare.coordinates,
        { $maxDistance: 500 } // 500 meters
      );

      // First 3 restaurants are nearby, farAway is not
      expect(results.length).toBe(3);
      expect(results.find(r => r.docId === 'farAway')).toBeUndefined();
    });

    it('should sort results by distance', () => {
      const results = geoIndex.findNear(NYC_LOCATIONS.timesSquare.coordinates);

      // Results should be sorted by distance
      for (let i = 1; i < results.length; i++) {
        expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance);
      }
    });
  });

  describe('Delivery zone checker', () => {
    it('should check if address is within delivery zone', () => {
      // Manhattan delivery zone (simplified)
      const deliveryZone: GeoJSONPolygon = {
        type: 'Polygon',
        coordinates: [[
          [-74.02, 40.70],
          [-73.93, 40.70],
          [-73.93, 40.80],
          [-74.02, 40.80],
          [-74.02, 40.70],
        ]],
      };

      // Times Square is in Manhattan
      const inZone = pointIntersectsGeometry(
        NYC_LOCATIONS.timesSquare.coordinates,
        deliveryZone
      );
      expect(inZone).toBe(true);

      // Statue of Liberty is not in delivery zone
      const outOfZone = pointIntersectsGeometry(
        NYC_LOCATIONS.statuOfLiberty.coordinates,
        deliveryZone
      );
      expect(outOfZone).toBe(false);
    });
  });

  describe('Geofencing', () => {
    it('should detect when device enters a geofenced area', () => {
      const geoIndex = new GeoIndex('geofences', 'center', '2dsphere');

      // Define geofenced areas
      geoIndex.indexDocument('office', {
        center: { type: 'Point', coordinates: [-73.985, 40.758] },
        radius: 100, // 100 meters
      });

      // Check if a location is near the geofence center
      const deviceLocation: [number, number] = [-73.9851, 40.7581];
      const nearbyGeofences = geoIndex.findNear(deviceLocation, { $maxDistance: 100 });

      expect(nearbyGeofences.length).toBe(1);
      expect(nearbyGeofences[0]!.docId).toBe('office');
    });
  });
});
