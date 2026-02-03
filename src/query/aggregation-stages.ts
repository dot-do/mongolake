/**
 * MongoLake Aggregation Stages
 *
 * Implementation of additional MongoDB aggregation pipeline stages:
 * - $bucket - group by value ranges
 * - $bucketAuto - automatic bucket distribution
 * - $facet - multiple pipelines (already in aggregation.ts, re-exported here)
 * - $graphLookup - recursive graph traversal
 * - $merge - merge into collection
 * - $out - output to collection
 * - $redact - access control at document level
 * - $replaceRoot / $replaceWith - replace document
 * - $sample - random sample
 * - $sortByCount - group and count
 */

import type {
  Document,
  WithId,
  BucketAutoStage,
  GraphLookupStage,
  MergeStage,
  OutStage,
  RedactExpression,
  ReplaceRootStage,
  SampleStage,
} from '@types';
import { getNestedValue } from '@utils/nested.js';
import { matchesFilter } from '@utils/filter.js';

/** $sortByCount stage - just the field expression */
export type SortByCountStage = string;

// ============================================================================
// Accumulator Evaluation Helper
// ============================================================================

/**
 * Get field value from document using $ notation
 */
function getFieldValue(doc: Record<string, unknown>, expr: unknown): unknown {
  if (typeof expr === 'string' && expr.startsWith('$')) {
    return getNestedValue(doc, expr.slice(1));
  }
  return expr;
}

/**
 * Evaluate a group accumulator expression
 */
export function evaluateAccumulator(
  groupDocs: Record<string, unknown>[],
  accExpr: Record<string, unknown>
): unknown {
  if ('$sum' in accExpr) {
    if (typeof accExpr.$sum === 'number') {
      return groupDocs.length * accExpr.$sum;
    }
    const sumField = String(accExpr.$sum).replace('$', '');
    return groupDocs.reduce((sum, d) => sum + (Number(getNestedValue(d, sumField)) || 0), 0);
  }

  if ('$avg' in accExpr) {
    const avgField = String(accExpr.$avg).replace('$', '');
    const values = groupDocs.map((d) => Number(getNestedValue(d, avgField)) || 0);
    return values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0;
  }

  if ('$min' in accExpr) {
    const minField = String(accExpr.$min).replace('$', '');
    const values = groupDocs
      .map((d) => getNestedValue(d, minField))
      .filter((v) => v !== undefined && v !== null) as number[];
    return values.length > 0 ? Math.min(...values) : null;
  }

  if ('$max' in accExpr) {
    const maxField = String(accExpr.$max).replace('$', '');
    const values = groupDocs
      .map((d) => getNestedValue(d, maxField))
      .filter((v) => v !== undefined && v !== null) as number[];
    return values.length > 0 ? Math.max(...values) : null;
  }

  if ('$first' in accExpr) {
    const firstField = String(accExpr.$first).replace('$', '');
    return groupDocs.length > 0 ? getNestedValue(groupDocs[0]!, firstField) : null;
  }

  if ('$last' in accExpr) {
    const lastField = String(accExpr.$last).replace('$', '');
    return groupDocs.length > 0 ? getNestedValue(groupDocs[groupDocs.length - 1]!, lastField) : null;
  }

  if ('$push' in accExpr) {
    const pushField = String(accExpr.$push).replace('$', '');
    return groupDocs.map((d) => getNestedValue(d, pushField));
  }

  if ('$addToSet' in accExpr) {
    const addToSetField = String(accExpr.$addToSet).replace('$', '');
    const values = groupDocs.map((d) => getNestedValue(d, addToSetField));
    const seen = new Set<string>();
    const uniqueValues: unknown[] = [];
    for (const v of values) {
      const key = JSON.stringify(v);
      if (!seen.has(key)) {
        seen.add(key);
        uniqueValues.push(v);
      }
    }
    return uniqueValues;
  }

  if ('$count' in accExpr) {
    return groupDocs.length;
  }

  return null;
}

// ============================================================================
// $bucketAuto Stage Implementation
// ============================================================================

/**
 * Process $bucketAuto stage - automatically distribute documents into buckets
 */
export function processBucketAuto(
  docs: WithId<Document>[],
  spec: BucketAutoStage
): WithId<Document>[] {
  const { groupBy, buckets, output } = spec;
  const fieldPath = groupBy.startsWith('$') ? groupBy.slice(1) : groupBy;

  // Extract and sort values
  const docsWithValues = docs
    .map((doc) => ({
      doc,
      value: getNestedValue(doc as Record<string, unknown>, fieldPath) as number | null,
    }))
    .filter((item) => item.value !== null && item.value !== undefined && typeof item.value === 'number');

  if (docsWithValues.length === 0) {
    return [];
  }

  // Sort by value
  docsWithValues.sort((a, b) => (a.value as number) - (b.value as number));

  // Calculate bucket size
  const bucketSize = Math.ceil(docsWithValues.length / buckets);
  const result: WithId<Document>[] = [];

  for (let i = 0; i < buckets && i * bucketSize < docsWithValues.length; i++) {
    const start = i * bucketSize;
    const end = Math.min(start + bucketSize, docsWithValues.length);
    const bucketDocs = docsWithValues.slice(start, end);

    if (bucketDocs.length === 0) continue;

    const firstBucketDoc = bucketDocs[0];
    const lastBucketDoc = bucketDocs[bucketDocs.length - 1];
    if (!firstBucketDoc || !lastBucketDoc) continue;
    const minValue = firstBucketDoc.value as number;
    const maxValue = lastBucketDoc.value as number;

    const bucketResult: Record<string, unknown> = {
      _id: { min: minValue, max: i === buckets - 1 ? maxValue : (docsWithValues[end]?.value ?? maxValue) },
    };

    if (output) {
      const rawDocs = bucketDocs.map((item) => item.doc as Record<string, unknown>);
      for (const [field, expr] of Object.entries(output)) {
        bucketResult[field] = evaluateAccumulator(rawDocs, expr as Record<string, unknown>);
      }
    } else {
      bucketResult.count = bucketDocs.length;
    }

    result.push(bucketResult as WithId<Document>);
  }

  return result;
}

// ============================================================================
// $graphLookup Stage Implementation
// ============================================================================

/**
 * Process $graphLookup stage - recursive graph traversal
 */
export async function processGraphLookup(
  docs: WithId<Document>[],
  spec: GraphLookupStage,
  getCollection: (name: string) => Promise<WithId<Document>[]>
): Promise<WithId<Document>[]> {
  const { startWith, connectFromField, connectToField, as, maxDepth, depthField, restrictSearchWithMatch } = spec;
  // maxDepth undefined means no limit, but we use a reasonable default
  const effectiveMaxDepth = maxDepth !== undefined ? maxDepth : 100;

  const foreignDocs = await getCollection(spec.from);

  const result: WithId<Document>[] = [];

  for (const doc of docs) {
    const startValue = getFieldValue(doc as Record<string, unknown>, startWith);
    const visited = new Set<string>();
    const connections: Array<Record<string, unknown>> = [];

    // BFS traversal - start at depth 0 but only queue next level connections
    const queue: Array<{ value: unknown; depth: number }> = [{ value: startValue, depth: 0 }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const { value, depth } = current;

      // Find matching documents
      for (const foreignDoc of foreignDocs) {
        const foreignValue = getNestedValue(foreignDoc as Record<string, unknown>, connectToField);
        const docKey = String(foreignDoc._id);

        // Check if values match - handle null matching explicitly
        let matches = false;
        if (value === null || value === undefined) {
          matches = foreignValue === null || foreignValue === undefined;
        } else if (Array.isArray(value)) {
          matches = value.includes(foreignValue);
        } else {
          matches = value === foreignValue;
        }

        if (matches && !visited.has(docKey)) {
          // Apply restrictSearchWithMatch if provided
          if (restrictSearchWithMatch) {
            if (!matchesFilter(foreignDoc, restrictSearchWithMatch)) {
              continue;
            }
          }

          visited.add(docKey);

          const connectionDoc: Record<string, unknown> = { ...foreignDoc };
          if (depthField) {
            connectionDoc[depthField] = depth;
          }
          connections.push(connectionDoc);

          // Queue the next level only if we haven't reached maxDepth
          if (depth < effectiveMaxDepth) {
            const nextValue = getNestedValue(foreignDoc as Record<string, unknown>, connectFromField);
            if (nextValue !== undefined) {
              queue.push({ value: nextValue, depth: depth + 1 });
            }
          }
        }
      }
    }

    const resultDoc = { ...doc, [as]: connections } as WithId<Document>;
    result.push(resultDoc);
  }

  return result;
}

// ============================================================================
// $merge Stage Implementation (Metadata only - actual merge handled by caller)
// ============================================================================

/**
 * Validate $merge stage specification
 */
export function validateMergeStage(spec: MergeStage): void {
  if (!spec.into) {
    throw new Error('$merge requires "into" field');
  }

  const validWhenMatched = ['replace', 'keepExisting', 'merge', 'fail'];
  if (spec.whenMatched && typeof spec.whenMatched === 'string' && !validWhenMatched.includes(spec.whenMatched)) {
    throw new Error(`Invalid whenMatched value: ${spec.whenMatched}`);
  }

  const validWhenNotMatched = ['insert', 'discard', 'fail'];
  if (spec.whenNotMatched && !validWhenNotMatched.includes(spec.whenNotMatched)) {
    throw new Error(`Invalid whenNotMatched value: ${spec.whenNotMatched}`);
  }
}

/**
 * Get target collection info from $merge stage
 */
export function getMergeTarget(spec: MergeStage): { db?: string; coll: string } {
  if (typeof spec.into === 'string') {
    return { coll: spec.into };
  }
  return spec.into;
}

// ============================================================================
// $out Stage Implementation (Metadata only - actual output handled by caller)
// ============================================================================

/**
 * Get target collection info from $out stage
 */
export function getOutTarget(spec: OutStage | string): { db?: string; coll: string } {
  if (typeof spec === 'string') {
    return { coll: spec };
  }
  return spec;
}

// ============================================================================
// $redact Stage Implementation
// ============================================================================

/**
 * Evaluate a conditional expression for $redact
 */
function evaluateRedactCondition(
  doc: Record<string, unknown>,
  condition: unknown
): unknown {
  if (typeof condition === 'string' && condition.startsWith('$$')) {
    return condition;
  }

  if (typeof condition === 'string' && condition.startsWith('$')) {
    return getNestedValue(doc, condition.slice(1));
  }

  if (typeof condition === 'object' && condition !== null) {
    const condObj = condition as Record<string, unknown>;

    // Handle $cond
    if ('$cond' in condObj) {
      const cond = condObj.$cond as { if: unknown; then: unknown; else: unknown };
      const ifResult = evaluateRedactCondition(doc, cond.if);
      return ifResult ? cond.then : cond.else;
    }

    // Handle comparison operators
    if ('$eq' in condObj) {
      const [a, b] = condObj.$eq as [unknown, unknown];
      const aVal = evaluateRedactCondition(doc, a);
      const bVal = evaluateRedactCondition(doc, b);
      return aVal === bVal;
    }

    if ('$gt' in condObj) {
      const [a, b] = condObj.$gt as [unknown, unknown];
      const aVal = evaluateRedactCondition(doc, a) as number;
      const bVal = evaluateRedactCondition(doc, b) as number;
      return aVal > bVal;
    }

    if ('$gte' in condObj) {
      const [a, b] = condObj.$gte as [unknown, unknown];
      const aVal = evaluateRedactCondition(doc, a) as number;
      const bVal = evaluateRedactCondition(doc, b) as number;
      return aVal >= bVal;
    }

    if ('$lt' in condObj) {
      const [a, b] = condObj.$lt as [unknown, unknown];
      const aVal = evaluateRedactCondition(doc, a) as number;
      const bVal = evaluateRedactCondition(doc, b) as number;
      return aVal < bVal;
    }

    if ('$lte' in condObj) {
      const [a, b] = condObj.$lte as [unknown, unknown];
      const aVal = evaluateRedactCondition(doc, a) as number;
      const bVal = evaluateRedactCondition(doc, b) as number;
      return aVal <= bVal;
    }

    // Handle $ifNull
    if ('$ifNull' in condObj) {
      const [expr, replacement] = condObj.$ifNull as [unknown, unknown];
      const val = evaluateRedactCondition(doc, expr);
      return val ?? replacement;
    }

    // Handle $in (check if value is in array)
    if ('$in' in condObj) {
      const [val, arr] = condObj.$in as [unknown, unknown];
      const actualVal = evaluateRedactCondition(doc, val);
      const actualArr = evaluateRedactCondition(doc, arr) as unknown[];
      return Array.isArray(actualArr) && actualArr.includes(actualVal);
    }
  }

  return condition;
}

/**
 * Apply redaction to a single document recursively
 */
function applyRedaction(
  doc: Record<string, unknown>,
  expression: RedactExpression
): Record<string, unknown> | null {
  const result = evaluateRedactCondition(doc, expression);

  if (result === '$$PRUNE') {
    return null;
  }

  if (result === '$$KEEP') {
    return doc;
  }

  if (result === '$$DESCEND') {
    const redactedDoc: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const redactedNested = applyRedaction(value as Record<string, unknown>, expression);
        if (redactedNested !== null) {
          redactedDoc[key] = redactedNested;
        }
      } else if (Array.isArray(value)) {
        const redactedArray = value
          .map((item) => {
            if (typeof item === 'object' && item !== null) {
              return applyRedaction(item as Record<string, unknown>, expression);
            }
            return item;
          })
          .filter((item) => item !== null);
        redactedDoc[key] = redactedArray;
      } else {
        redactedDoc[key] = value;
      }
    }
    return redactedDoc;
  }

  return doc;
}

/**
 * Process $redact stage - document-level access control
 */
export function processRedact(
  docs: WithId<Document>[],
  expression: RedactExpression
): WithId<Document>[] {
  return docs
    .map((doc) => applyRedaction(doc as Record<string, unknown>, expression))
    .filter((doc): doc is WithId<Document> => doc !== null);
}

// ============================================================================
// $replaceRoot / $replaceWith Stage Implementation
// ============================================================================

/**
 * Process $replaceRoot or $replaceWith stage - replace document with new root
 */
export function processReplaceRoot(
  docs: WithId<Document>[],
  spec: ReplaceRootStage | string
): WithId<Document>[] {
  const newRootExpr = typeof spec === 'string' ? spec : spec.newRoot;

  return docs
    .map((doc) => {
      let newRoot: unknown;

      if (typeof newRootExpr === 'string' && newRootExpr.startsWith('$')) {
        // Field path expression
        newRoot = getNestedValue(doc as Record<string, unknown>, newRootExpr.slice(1));
      } else if (typeof newRootExpr === 'object' && newRootExpr !== null) {
        // Object expression - evaluate field references
        newRoot = evaluateObjectExpression(doc as Record<string, unknown>, newRootExpr as Record<string, unknown>);
      } else {
        newRoot = newRootExpr;
      }

      if (newRoot === null || newRoot === undefined || typeof newRoot !== 'object') {
        throw new Error('$replaceRoot requires the new root to be a document');
      }

      return newRoot as WithId<Document>;
    });
}

/**
 * Evaluate an object expression, resolving field references
 */
function evaluateObjectExpression(
  doc: Record<string, unknown>,
  expr: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(expr)) {
    if (typeof value === 'string' && value.startsWith('$')) {
      result[key] = getNestedValue(doc, value.slice(1));
    } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      // Handle special operators like $mergeObjects
      const valueObj = value as Record<string, unknown>;
      if ('$mergeObjects' in valueObj) {
        const objectsToMerge = valueObj.$mergeObjects as unknown[];
        let merged: Record<string, unknown> = {};
        for (const obj of objectsToMerge) {
          if (typeof obj === 'string' && obj.startsWith('$')) {
            const resolvedObj = getNestedValue(doc, obj.slice(1)) as Record<string, unknown>;
            if (resolvedObj && typeof resolvedObj === 'object') {
              merged = { ...merged, ...resolvedObj };
            }
          } else if (typeof obj === 'object' && obj !== null) {
            merged = { ...merged, ...evaluateObjectExpression(doc, obj as Record<string, unknown>) };
          }
        }
        result[key] = merged;
      } else {
        result[key] = evaluateObjectExpression(doc, valueObj);
      }
    } else {
      result[key] = value;
    }
  }

  return result;
}

// ============================================================================
// $sample Stage Implementation
// ============================================================================

/**
 * Process $sample stage - random sampling
 */
export function processSample(
  docs: WithId<Document>[],
  spec: SampleStage
): WithId<Document>[] {
  const { size } = spec;

  if (size <= 0) {
    return [];
  }

  if (size >= docs.length) {
    return [...docs];
  }

  // Fisher-Yates shuffle and take first `size` elements
  const shuffled = [...docs];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!];
  }

  return shuffled.slice(0, size);
}

// ============================================================================
// $sortByCount Stage Implementation
// ============================================================================

/**
 * Process $sortByCount stage - group by field and count, sorted descending
 */
export function processSortByCount(
  docs: WithId<Document>[],
  fieldExpr: SortByCountStage
): WithId<Document>[] {
  const fieldPath = fieldExpr.startsWith('$') ? fieldExpr.slice(1) : fieldExpr;

  // Group and count
  const counts = new Map<unknown, number>();

  for (const doc of docs) {
    const value = getNestedValue(doc as Record<string, unknown>, fieldPath);
    const current = counts.get(value) || 0;
    counts.set(value, current + 1);
  }

  // Convert to result documents and sort by count descending
  const result: WithId<Document>[] = [];
  for (const [id, count] of counts) {
    // The _id can be any grouping key value, and count is a number.
    // Single cast is safe - the shape is a valid Document with _id.
    result.push({ _id: id, count } as WithId<Document>);
  }

  result.sort((a, b) => {
    const aCount = (a as Record<string, unknown>).count as number;
    const bCount = (b as Record<string, unknown>).count as number;
    return bCount - aCount;
  });

  return result;
}

// Re-export types from types.ts for convenience
export type {
  BucketAutoStage,
  GraphLookupStage,
  MergeStage,
  OutStage,
  RedactExpression,
  ReplaceRootStage,
  SampleStage,
} from '../types.js';
