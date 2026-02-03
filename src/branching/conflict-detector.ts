/**
 * Conflict Detector
 *
 * Detects conflicts when merging branches by comparing changes made
 * on both the source branch and target branch since the branch point.
 *
 * ## Conflict Types
 *
 * - **FIELD_CONFLICT**: Same field modified differently on both branches
 * - **DELETE_UPDATE**: Document deleted on main, updated on branch
 * - **UPDATE_DELETE**: Document updated on main, deleted on branch
 * - **DUPLICATE_INSERT**: Same document ID inserted on both branches
 *
 * ## Performance
 *
 * The detector uses O(n) indexing for efficient conflict scanning:
 * - Builds hash indexes for both main and branch changes
 * - Single-pass conflict detection using index lookups
 * - Lazy field comparison only when documents overlap
 *
 * ## Usage
 *
 * ```typescript
 * const detector = new ConflictDetector(storage, branchStore, 'mydb');
 * const report = await detector.detectConflicts('feature', mainChanges, branchChanges);
 *
 * if (report.hasConflicts) {
 *   console.log('Conflicts found:', report.conflicts);
 *   for (const conflict of report.conflicts) {
 *     console.log(`  - ${conflict.description}`);
 *     console.log(`    Hint: ${conflict.resolutionHint}`);
 *   }
 * }
 * ```
 */

import type { StorageBackend } from '../storage/index.js';
import type { Document } from '../types.js';
import { BranchStore, DEFAULT_BRANCH } from './metadata.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Types of conflicts that can occur during merge.
 */
export enum ConflictType {
  /** Same field modified on both branches with different values */
  FIELD_CONFLICT = 'field_conflict',
  /** Document deleted on main, updated on branch */
  DELETE_UPDATE = 'delete_update',
  /** Document updated on main, deleted on branch */
  UPDATE_DELETE = 'update_delete',
  /** Same document ID inserted on both branches */
  DUPLICATE_INSERT = 'duplicate_insert',
}

/**
 * Severity levels for conflicts.
 *
 * Severity indicates how difficult the conflict is to resolve:
 * - LOW: Simple field choice, often can be resolved automatically with rules
 * - MEDIUM: Requires human decision about document existence
 * - HIGH: Fundamental conflict that needs careful consideration
 */
export enum ConflictSeverity {
  /** Field conflicts - can often be resolved by choosing one version */
  LOW = 'low',
  /** Delete/update conflicts - requires decision on document existence */
  MEDIUM = 'medium',
  /** Duplicate inserts - fundamental conflict that needs resolution */
  HIGH = 'high',
}

/**
 * A change to a document.
 */
export interface DocumentChange {
  /** Document ID */
  documentId: string;
  /** Collection name */
  collection: string;
  /** Type of operation */
  operation: 'insert' | 'update' | 'delete';
  /** Fields that were modified (for updates) */
  fields?: string[];
  /** Document state before the change */
  before?: Document;
  /** Document state after the change */
  after?: Document;
}

/**
 * A conflict between main and branch changes.
 */
export interface DocumentConflict {
  /** Document ID */
  documentId: string;
  /** Collection name */
  collection: string;
  /** Type of conflict */
  type: ConflictType;
  /** Severity level */
  severity: ConflictSeverity;
  /** Human-readable description of the conflict */
  description: string;
  /** Fields that conflict (for field conflicts) */
  conflictingFields?: string[];
  /** Operation performed on main branch */
  mainOperation: 'insert' | 'update' | 'delete';
  /** Operation performed on source branch */
  branchOperation: 'insert' | 'update' | 'delete';
  /** Document version on main branch */
  mainVersion?: Document;
  /** Document version on source branch */
  branchVersion?: Document;
  /** Original document version at branch point */
  baseVersion?: Document;
  /** Hint for resolving this conflict */
  resolutionHint: string;
}

/**
 * Summary statistics for the conflict report.
 */
export interface ConflictSummary {
  /** Total number of conflicts */
  totalConflicts: number;
  /** Number of changes on main branch */
  mainChangesCount: number;
  /** Number of changes on source branch */
  branchChangesCount: number;
  /** Number of auto-mergeable changes */
  autoMergeableCount: number;
  /** Breakdown by severity */
  bySeverity: {
    low: number;
    medium: number;
    high: number;
  };
  /** Breakdown by conflict type */
  byType: {
    [K in ConflictType]?: number;
  };
}

/**
 * A change that can be automatically merged.
 */
export interface AutoMergeableChange {
  /** Document ID */
  documentId: string;
  /** Collection name */
  collection: string;
  /** The change to apply */
  change: DocumentChange;
  /** Source of the change (main or branch) */
  source: 'main' | 'branch';
}

/**
 * Complete conflict detection report.
 */
export interface ConflictReport {
  /** Whether any conflicts were detected */
  hasConflicts: boolean;
  /** List of detected conflicts */
  conflicts: DocumentConflict[];
  /** Changes that can be auto-merged */
  autoMergeableChanges: AutoMergeableChange[];
  /** Source branch name */
  sourceBranch: string;
  /** Target branch name */
  targetBranch: string;
  /** Summary statistics */
  summary: ConflictSummary;
}

// ============================================================================
// Resolution Hints
// ============================================================================

/**
 * Resolution hint generators for each conflict type.
 */
const RESOLUTION_HINTS = {
  [ConflictType.FIELD_CONFLICT]: (fields: string[]): string => {
    if (fields.length === 1) {
      return `Field "${fields[0]}" was modified on both branches. ` +
        'Options: (1) Use main version, (2) Use branch version, (3) Manually merge values.';
    }
    return `Fields [${fields.join(', ')}] were modified on both branches. ` +
      'Options: (1) Use main version for all, (2) Use branch version for all, ' +
      '(3) Choose per field, (4) Manually merge values.';
  },

  [ConflictType.DELETE_UPDATE]: (): string =>
    'Document was deleted on main but updated on branch. ' +
    'Options: (1) Keep deleted (discard branch changes), ' +
    '(2) Restore document with branch changes, ' +
    '(3) Create new document with branch data.',

  [ConflictType.UPDATE_DELETE]: (): string =>
    'Document was updated on main but deleted on branch. ' +
    'Options: (1) Keep main updates (discard deletion), ' +
    '(2) Apply deletion (discard main updates), ' +
    '(3) Archive document before deletion.',

  [ConflictType.DUPLICATE_INSERT]: (): string =>
    'Same document ID was inserted on both branches with different data. ' +
    'Options: (1) Keep main version, (2) Keep branch version, ' +
    '(3) Merge fields from both, (4) Rename one document ID.',
} as const;

// ============================================================================
// Conflict Descriptions
// ============================================================================

/**
 * Generate human-readable conflict descriptions.
 */
function generateDescription(
  type: ConflictType,
  collection: string,
  documentId: string,
  fields?: string[]
): string {
  const docRef = `${collection}/${documentId}`;

  switch (type) {
    case ConflictType.FIELD_CONFLICT:
      if (fields && fields.length > 0) {
        const fieldList = fields.length <= 3
          ? fields.join(', ')
          : `${fields.slice(0, 3).join(', ')} and ${fields.length - 3} more`;
        return `Field conflict in ${docRef}: both branches modified ${fieldList}`;
      }
      return `Field conflict in ${docRef}: same fields modified on both branches`;

    case ConflictType.DELETE_UPDATE:
      return `Delete/update conflict in ${docRef}: deleted on main, updated on branch`;

    case ConflictType.UPDATE_DELETE:
      return `Update/delete conflict in ${docRef}: updated on main, deleted on branch`;

    case ConflictType.DUPLICATE_INSERT:
      return `Duplicate insert in ${docRef}: document created on both branches`;

    default:
      return `Conflict in ${docRef}`;
  }
}

// ============================================================================
// Conflict Detector
// ============================================================================

/**
 * Detects conflicts when merging branches.
 *
 * Uses efficient O(n) scanning with hash-based indexing for performance.
 * Supports field-level conflict detection for granular merge decisions.
 */
export class ConflictDetector {
  private readonly branchStore: BranchStore;

  /**
   * Create a new ConflictDetector.
   *
   * @param _storage - Storage backend (reserved for future document loading)
   * @param branchStore - Branch store for branch metadata
   * @param _database - Database name (reserved for future use)
   */
  constructor(_storage: StorageBackend, branchStore: BranchStore, _database: string) {
    this.branchStore = branchStore;
  }

  /**
   * Detect conflicts between main branch and source branch changes.
   *
   * This method performs efficient conflict detection using hash-based
   * indexing. Time complexity is O(n + m) where n = main changes and
   * m = branch changes.
   *
   * @param sourceBranch - Name of the branch to merge from
   * @param mainChanges - Changes made on main since branch point
   * @param branchChanges - Changes made on source branch
   * @returns Conflict report with detailed conflict information
   */
  async detectConflicts(
    sourceBranch: string,
    mainChanges: DocumentChange[],
    branchChanges: DocumentChange[]
  ): Promise<ConflictReport> {
    // Validate branch exists
    const branch = await this.branchStore.getBranch(sourceBranch);
    if (!branch) {
      throw new Error(`Branch "${sourceBranch}" not found`);
    }

    // Build indexes for O(1) lookups
    const mainIndex = this.buildChangeIndex(mainChanges);
    const branchIndex = this.buildChangeIndex(branchChanges);

    // Detect conflicts and auto-mergeable changes
    const { conflicts, autoMergeableChanges } = this.scanForConflicts(
      mainIndex,
      branchIndex,
      branchChanges
    );

    // Build summary with detailed breakdowns
    const summary = this.buildSummary(conflicts, autoMergeableChanges, mainChanges, branchChanges);

    return {
      hasConflicts: conflicts.length > 0,
      conflicts,
      autoMergeableChanges,
      sourceBranch,
      targetBranch: DEFAULT_BRANCH,
      summary,
    };
  }

  /**
   * Build an optimized index of changes by document key.
   * Uses Map for O(1) average-case lookups.
   */
  private buildChangeIndex(changes: DocumentChange[]): Map<string, DocumentChange> {
    const index = new Map<string, DocumentChange>();
    for (const change of changes) {
      index.set(this.getDocumentKey(change), change);
    }
    return index;
  }

  /**
   * Get a unique key for a document change.
   * Format: "collection:documentId"
   */
  private getDocumentKey(change: DocumentChange): string {
    return `${change.collection}:${change.documentId}`;
  }

  /**
   * Scan for conflicts between main and branch changes.
   * Single-pass algorithm with early termination for non-overlapping documents.
   */
  private scanForConflicts(
    mainIndex: Map<string, DocumentChange>,
    _branchIndex: Map<string, DocumentChange>, // Reserved for future: detecting main-only changes
    branchChanges: DocumentChange[]
  ): { conflicts: DocumentConflict[]; autoMergeableChanges: AutoMergeableChange[] } {
    const conflicts: DocumentConflict[] = [];
    const autoMergeableChanges: AutoMergeableChange[] = [];

    // Process each branch change
    for (const branchChange of branchChanges) {
      const key = this.getDocumentKey(branchChange);
      const mainChange = mainIndex.get(key);

      if (mainChange) {
        // Both branches modified the same document - check for conflict
        const conflict = this.detectDocumentConflict(mainChange, branchChange);
        if (conflict) {
          conflicts.push(conflict);
        } else if (this.canAutoMerge(mainChange, branchChange)) {
          autoMergeableChanges.push({
            documentId: branchChange.documentId,
            collection: branchChange.collection,
            change: branchChange,
            source: 'branch',
          });
        }
      } else {
        // Branch-only change - can be auto-merged
        autoMergeableChanges.push({
          documentId: branchChange.documentId,
          collection: branchChange.collection,
          change: branchChange,
          source: 'branch',
        });
      }
    }

    return { conflicts, autoMergeableChanges };
  }

  /**
   * Detect conflict between main and branch changes to the same document.
   */
  private detectDocumentConflict(
    mainChange: DocumentChange,
    branchChange: DocumentChange
  ): DocumentConflict | null {
    const { documentId, collection } = branchChange;

    // Same delete on both branches - no conflict (convergent)
    if (mainChange.operation === 'delete' && branchChange.operation === 'delete') {
      return null;
    }

    // Delete on main, update on branch
    if (mainChange.operation === 'delete' && branchChange.operation === 'update') {
      return this.createConflict(
        ConflictType.DELETE_UPDATE,
        ConflictSeverity.MEDIUM,
        documentId,
        collection,
        'delete',
        'update',
        undefined,
        branchChange.after,
        branchChange.before
      );
    }

    // Update on main, delete on branch
    if (mainChange.operation === 'update' && branchChange.operation === 'delete') {
      return this.createConflict(
        ConflictType.UPDATE_DELETE,
        ConflictSeverity.MEDIUM,
        documentId,
        collection,
        'update',
        'delete',
        mainChange.after,
        undefined,
        mainChange.before
      );
    }

    // Both inserts with same ID
    if (mainChange.operation === 'insert' && branchChange.operation === 'insert') {
      return this.createConflict(
        ConflictType.DUPLICATE_INSERT,
        ConflictSeverity.HIGH,
        documentId,
        collection,
        'insert',
        'insert',
        mainChange.after,
        branchChange.after,
        undefined
      );
    }

    // Both updates - check for field conflicts
    if (mainChange.operation === 'update' && branchChange.operation === 'update') {
      return this.detectFieldConflict(mainChange, branchChange);
    }

    // Other operation combinations - treat as conflict
    if (mainChange.operation !== branchChange.operation) {
      return {
        documentId,
        collection,
        type: ConflictType.FIELD_CONFLICT,
        severity: ConflictSeverity.MEDIUM,
        description: `Operation mismatch in ${collection}/${documentId}: ` +
          `${mainChange.operation} on main, ${branchChange.operation} on branch`,
        mainOperation: mainChange.operation,
        branchOperation: branchChange.operation,
        mainVersion: mainChange.after,
        branchVersion: branchChange.after,
        baseVersion: mainChange.before || branchChange.before,
        resolutionHint: `Different operations performed. Choose which operation to apply.`,
      };
    }

    return null;
  }

  /**
   * Create a standardized conflict object with description and hints.
   */
  private createConflict(
    type: ConflictType,
    severity: ConflictSeverity,
    documentId: string,
    collection: string,
    mainOperation: 'insert' | 'update' | 'delete',
    branchOperation: 'insert' | 'update' | 'delete',
    mainVersion: Document | undefined,
    branchVersion: Document | undefined,
    baseVersion: Document | undefined,
    conflictingFields?: string[]
  ): DocumentConflict {
    return {
      documentId,
      collection,
      type,
      severity,
      description: generateDescription(type, collection, documentId, conflictingFields),
      conflictingFields,
      mainOperation,
      branchOperation,
      mainVersion,
      branchVersion,
      baseVersion,
      resolutionHint: type === ConflictType.FIELD_CONFLICT && conflictingFields
        ? RESOLUTION_HINTS[type](conflictingFields)
        : RESOLUTION_HINTS[type]([]),
    };
  }

  /**
   * Detect field-level conflicts between two updates.
   * Uses Set intersection for efficient field overlap detection.
   */
  private detectFieldConflict(
    mainChange: DocumentChange,
    branchChange: DocumentChange
  ): DocumentConflict | null {
    const mainFields = new Set(mainChange.fields || []);
    const branchFields = branchChange.fields || [];

    // Find overlapping fields using Set lookup
    const conflictingFields = branchFields.filter(field => mainFields.has(field));

    if (conflictingFields.length === 0) {
      // Different fields modified - can auto-merge
      return null;
    }

    // Same fields modified - this is a conflict
    return this.createConflict(
      ConflictType.FIELD_CONFLICT,
      ConflictSeverity.LOW,
      mainChange.documentId,
      mainChange.collection,
      'update',
      'update',
      mainChange.after,
      branchChange.after,
      mainChange.before,
      conflictingFields
    );
  }

  /**
   * Check if two changes to the same document can be auto-merged.
   * Returns true only for non-overlapping field updates.
   */
  private canAutoMerge(mainChange: DocumentChange, branchChange: DocumentChange): boolean {
    // Only update + update with non-overlapping fields can auto-merge
    if (mainChange.operation !== 'update' || branchChange.operation !== 'update') {
      return false;
    }

    const mainFields = new Set(mainChange.fields || []);
    const branchFields = branchChange.fields || [];

    // Check for any overlapping fields
    return !branchFields.some(field => mainFields.has(field));
  }

  /**
   * Build detailed summary with breakdowns by severity and type.
   */
  private buildSummary(
    conflicts: DocumentConflict[],
    autoMergeableChanges: AutoMergeableChange[],
    mainChanges: DocumentChange[],
    branchChanges: DocumentChange[]
  ): ConflictSummary {
    const bySeverity = { low: 0, medium: 0, high: 0 };
    const byType: { [K in ConflictType]?: number } = {};

    for (const conflict of conflicts) {
      bySeverity[conflict.severity]++;
      byType[conflict.type] = (byType[conflict.type] || 0) + 1;
    }

    return {
      totalConflicts: conflicts.length,
      mainChangesCount: mainChanges.length,
      branchChangesCount: branchChanges.length,
      autoMergeableCount: autoMergeableChanges.length,
      bySeverity,
      byType,
    };
  }
}
