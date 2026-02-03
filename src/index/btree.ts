/**
 * B-tree Index Implementation
 *
 * A balanced B-tree data structure optimized for single-field indexes in MongoLake.
 * Supports insert, search, delete, and range operations with efficient disk persistence.
 *
 * B-tree Properties:
 * - All leaves are at the same depth
 * - Each node (except root) has at least t-1 keys
 * - Each node has at most 2t-1 keys
 * - Root has at least 1 key (if tree is non-empty)
 *
 * Note: t is the minimum degree (minimum children for non-root nodes)
 */

// ============================================================================
// Types
// ============================================================================

/** Comparison function type for custom key ordering */
export type CompareFn<K> = (a: K, b: K) => number;

/** Entry stored in the B-tree: key maps to document IDs */
export interface BTreeEntry<K> {
  key: K;
  docIds: string[];
}

/** Serialized format for a B-tree node */
export interface SerializedNode<K> {
  id: string;
  isLeaf: boolean;
  keys: K[];
  docIds: string[][];
  childIds: string[];
}

/** Serialized format for the entire B-tree */
export interface SerializedBTree<K> {
  name: string;
  field: string;
  minDegree: number;
  rootId: string | null;
  nodes: SerializedNode<K>[];
  unique: boolean;
}

/** Index metadata stored in collection manifest */
export interface IndexMetadata {
  name: string;
  field: string;
  unique: boolean;
  sparse: boolean;
  createdAt: string;
}

// ============================================================================
// B-tree Node
// ============================================================================

/**
 * A node in the B-tree structure
 */
export class BTreeNode<K> {
  /** Unique identifier for this node (used for serialization) */
  id: string;

  /** Whether this node is a leaf */
  isLeaf: boolean;

  /** Keys stored in this node (sorted) */
  keys: K[];

  /** Document IDs for each key (parallel array to keys) */
  docIds: string[][];

  /** Child node references (for internal nodes only) */
  children: BTreeNode<K>[];

  /** Child IDs for serialization */
  childIds: string[];

  constructor(isLeaf: boolean = true) {
    this.id = crypto.randomUUID();
    this.isLeaf = isLeaf;
    this.keys = [];
    this.docIds = [];
    this.children = [];
    this.childIds = [];
  }

  /**
   * Get number of keys in this node
   */
  get keyCount(): number {
    return this.keys.length;
  }

  /**
   * Serialize this node to a plain object
   */
  serialize(): SerializedNode<K> {
    return {
      id: this.id,
      isLeaf: this.isLeaf,
      keys: this.keys,
      docIds: this.docIds,
      childIds: this.children.map((c) => c.id),
    };
  }

  /**
   * Create a node from serialized data
   */
  static deserialize<K>(data: SerializedNode<K>): BTreeNode<K> {
    const node = new BTreeNode<K>(data.isLeaf);
    node.id = data.id;
    node.keys = data.keys;
    node.docIds = data.docIds;
    node.childIds = data.childIds;
    return node;
  }
}

// ============================================================================
// B-tree Implementation
// ============================================================================

/**
 * B-tree index structure for efficient key-based lookups
 *
 * @example
 * ```typescript
 * const tree = new BTree<number>('age_index', 'age', 3);
 * tree.insert(25, 'doc1');
 * tree.insert(30, 'doc2');
 * tree.insert(25, 'doc3');
 *
 * // Find all documents with age 25
 * const docs = tree.search(25); // ['doc1', 'doc3']
 *
 * // Range query
 * const range = tree.range(20, 30); // [[25, ['doc1', 'doc3']], [30, ['doc2']]]
 * ```
 */
export class BTree<K = unknown> {
  /** Index name */
  readonly name: string;

  /** Field being indexed */
  readonly field: string;

  /** Minimum degree (t) - determines node capacity */
  readonly minDegree: number;

  /** Root node (null for empty tree) */
  root: BTreeNode<K> | null;

  /** Comparison function for keys */
  private compare: CompareFn<K>;

  /** Whether this is a unique index */
  readonly unique: boolean;

  /**
   * Create a new B-tree index
   *
   * @param name - Index name
   * @param field - Field to index
   * @param minDegree - Minimum degree (default: 64 for good disk performance)
   * @param compare - Custom comparison function (default: natural ordering)
   * @param unique - Whether this is a unique index
   */
  constructor(
    name: string,
    field: string,
    minDegree: number = 64,
    compare?: CompareFn<K>,
    unique: boolean = false
  ) {
    if (minDegree < 2) {
      throw new Error('Minimum degree must be at least 2');
    }

    this.name = name;
    this.field = field;
    this.minDegree = minDegree;
    this.root = null;
    this.unique = unique;

    // Default comparison: natural ordering
    this.compare = compare || ((a, b) => {
      if (a === b) return 0;
      if (a === null || a === undefined) return -1;
      if (b === null || b === undefined) return 1;
      return a < b ? -1 : 1;
    });
  }

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  /**
   * Insert a key-document ID pair into the index
   *
   * @param key - The key value to index
   * @param docId - The document ID
   * @throws Error if unique constraint is violated
   */
  insert(key: K, docId: string): void {
    // Handle empty tree
    if (!this.root) {
      this.root = new BTreeNode<K>(true);
      this.root.keys.push(key);
      this.root.docIds.push([docId]);
      return;
    }

    // Check for existing key first
    const existing = this.searchNode(this.root, key);
    if (existing) {
      if (this.unique) {
        throw new Error(`Duplicate key value: ${key}`);
      }
      // Add to existing key's docIds if not already present
      if (!existing.docIds.includes(docId)) {
        existing.docIds.push(docId);
      }
      return;
    }

    // If root is full, split it first
    if (this.root.keyCount === 2 * this.minDegree - 1) {
      const newRoot = new BTreeNode<K>(false);
      newRoot.children.push(this.root);
      this.splitChild(newRoot, 0);
      this.root = newRoot;
    }

    // Insert into non-full tree
    this.insertNonFull(this.root, key, docId);
  }

  /**
   * Search for documents by key
   *
   * @param key - The key to search for
   * @returns Array of document IDs (empty if not found)
   */
  search(key: K): string[] {
    if (!this.root) return [];
    const entry = this.searchNode(this.root, key);
    return entry ? [...entry.docIds] : [];
  }

  /**
   * Check if a key exists in the index
   *
   * @param key - The key to check
   * @returns true if the key exists
   */
  has(key: K): boolean {
    return this.search(key).length > 0;
  }

  /**
   * Range query: find all entries within a key range
   *
   * @param minKey - Minimum key (inclusive), null for unbounded
   * @param maxKey - Maximum key (inclusive), null for unbounded
   * @returns Array of [key, docIds] tuples in sorted order
   */
  range(minKey: K | null, maxKey: K | null): Array<[K, string[]]> {
    const results: Array<[K, string[]]> = [];
    if (!this.root) return results;

    this.rangeSearch(this.root, minKey, maxKey, results);
    return results;
  }

  /**
   * Get all entries in sorted order
   *
   * @returns Array of [key, docIds] tuples
   */
  entries(): Array<[K, string[]]> {
    return this.range(null, null);
  }

  /**
   * Delete a key-document ID pair from the index
   *
   * @param key - The key to delete
   * @param docId - Optional specific document ID to remove
   *               If not provided, removes all docIds for the key
   * @returns true if the key was found and deleted
   */
  delete(key: K, docId?: string): boolean {
    if (!this.root) return false;

    const deleted = this.deleteFromNode(this.root, key, docId);

    // If root becomes empty after deletion, update root
    if (this.root.keyCount === 0) {
      if (this.root.isLeaf) {
        this.root = null;
      } else {
        this.root = this.root.children[0] ?? null;
      }
    }

    return deleted;
  }

  /**
   * Get the total number of keys in the index
   */
  get size(): number {
    if (!this.root) return 0;
    return this.countKeys(this.root);
  }

  /**
   * Check if the index is empty
   */
  get isEmpty(): boolean {
    return this.root === null;
  }

  /**
   * Clear all entries from the index
   */
  clear(): void {
    this.root = null;
  }

  /**
   * Get the minimum key in the index
   */
  min(): K | undefined {
    if (!this.root) return undefined;
    let node = this.root;
    while (!node.isLeaf) {
      node = node.children[0]!;
    }
    return node.keys[0];
  }

  /**
   * Get the maximum key in the index
   */
  max(): K | undefined {
    if (!this.root) return undefined;
    let node = this.root;
    while (!node.isLeaf) {
      node = node.children[node.children.length - 1]!;
    }
    return node.keys[node.keyCount - 1];
  }

  // --------------------------------------------------------------------------
  // Serialization
  // --------------------------------------------------------------------------

  /**
   * Serialize the B-tree to a plain object for storage
   */
  serialize(): SerializedBTree<K> {
    const nodes: SerializedNode<K>[] = [];

    if (this.root) {
      this.collectNodes(this.root, nodes);
    }

    return {
      name: this.name,
      field: this.field,
      minDegree: this.minDegree,
      rootId: this.root?.id || null,
      nodes,
      unique: this.unique,
    };
  }

  /**
   * Create a B-tree from serialized data
   */
  static deserialize<K>(
    data: SerializedBTree<K>,
    compare?: CompareFn<K>
  ): BTree<K> {
    const tree = new BTree<K>(
      data.name,
      data.field,
      data.minDegree,
      compare,
      data.unique
    );

    if (!data.rootId || data.nodes.length === 0) {
      return tree;
    }

    // Create all nodes
    const nodeMap = new Map<string, BTreeNode<K>>();
    for (const nodeData of data.nodes) {
      const node = BTreeNode.deserialize<K>(nodeData);
      nodeMap.set(node.id, node);
    }

    // Reconstruct parent-child relationships
    for (const nodeData of data.nodes) {
      const node = nodeMap.get(nodeData.id)!;
      node.children = nodeData.childIds.map((id) => nodeMap.get(id)!);
    }

    tree.root = nodeMap.get(data.rootId) || null;
    return tree;
  }

  /**
   * Convert to JSON string for storage
   */
  toJSON(): string {
    return JSON.stringify(this.serialize());
  }

  /**
   * Create a B-tree from JSON string
   */
  static fromJSON<K>(json: string, compare?: CompareFn<K>): BTree<K> {
    return BTree.deserialize<K>(JSON.parse(json), compare);
  }

  // --------------------------------------------------------------------------
  // Private Methods: Search
  // --------------------------------------------------------------------------

  /**
   * Search for a key in a subtree
   */
  private searchNode(node: BTreeNode<K>, key: K): { docIds: string[] } | null {
    let i = 0;

    // Find the first key >= search key
    while (i < node.keyCount && this.compare(key, node.keys[i]!) > 0) {
      i++;
    }

    // Check if we found the key
    if (i < node.keyCount && this.compare(key, node.keys[i]!) === 0) {
      return { docIds: node.docIds[i]! };
    }

    // If leaf, key is not in tree
    if (node.isLeaf) {
      return null;
    }

    // Recurse into appropriate child
    return this.searchNode(node.children[i]!, key);
  }

  /**
   * Range search helper - collects all entries within range
   */
  private rangeSearch(
    node: BTreeNode<K>,
    minKey: K | null,
    maxKey: K | null,
    results: Array<[K, string[]]>
  ): void {
    let i = 0;

    // Find starting position if minKey is specified
    if (minKey !== null) {
      while (i < node.keyCount && this.compare(node.keys[i]!, minKey) < 0) {
        i++;
      }
    }

    // Traverse keys and children
    while (i < node.keyCount) {
      const key = node.keys[i]!;

      // Check if we've passed maxKey
      if (maxKey !== null && this.compare(key, maxKey) > 0) {
        break;
      }

      // Visit left child first (if internal node)
      if (!node.isLeaf) {
        this.rangeSearch(node.children[i]!, minKey, maxKey, results);
      }

      // Add current key if within range
      if (
        (minKey === null || this.compare(key, minKey) >= 0) &&
        (maxKey === null || this.compare(key, maxKey) <= 0)
      ) {
        results.push([key, [...node.docIds[i]!]]);
      }

      i++;
    }

    // Visit rightmost child
    if (!node.isLeaf && i < node.children.length) {
      this.rangeSearch(node.children[i]!, minKey, maxKey, results);
    }
  }

  // --------------------------------------------------------------------------
  // Private Methods: Insertion
  // --------------------------------------------------------------------------

  /**
   * Insert into a non-full node
   */
  private insertNonFull(node: BTreeNode<K>, key: K, docId: string): void {
    let i = node.keyCount - 1;

    if (node.isLeaf) {
      // Find position and insert
      while (i >= 0 && this.compare(key, node.keys[i]!) < 0) {
        i--;
      }

      // Check if key already exists at position i+1
      if (i >= 0 && i < node.keyCount && this.compare(key, node.keys[i]!) === 0) {
        // Key exists, add docId to existing entry
        if (!node.docIds[i]!.includes(docId)) {
          node.docIds[i]!.push(docId);
        }
        return;
      }

      // Insert new key at position i+1
      node.keys.splice(i + 1, 0, key);
      node.docIds.splice(i + 1, 0, [docId]);
    } else {
      // Find child to recurse into
      while (i >= 0 && this.compare(key, node.keys[i]!) < 0) {
        i--;
      }

      // Check if key exists at this internal node
      if (i >= 0 && this.compare(key, node.keys[i]!) === 0) {
        if (!node.docIds[i]!.includes(docId)) {
          node.docIds[i]!.push(docId);
        }
        return;
      }

      i++;

      // Split child if full
      if (node.children[i]!.keyCount === 2 * this.minDegree - 1) {
        this.splitChild(node, i);
        if (this.compare(key, node.keys[i]!) > 0) {
          i++;
        } else if (this.compare(key, node.keys[i]!) === 0) {
          // Key was promoted during split, add to it
          if (!node.docIds[i]!.includes(docId)) {
            node.docIds[i]!.push(docId);
          }
          return;
        }
      }

      this.insertNonFull(node.children[i]!, key, docId);
    }
  }

  /**
   * Split a full child node
   */
  private splitChild(parent: BTreeNode<K>, childIndex: number): void {
    const fullChild = parent.children[childIndex]!;
    const t = this.minDegree;

    // Create new node for right half
    const newNode = new BTreeNode<K>(fullChild.isLeaf);

    // Move right half of keys to new node
    newNode.keys = fullChild.keys.splice(t);
    newNode.docIds = fullChild.docIds.splice(t);

    // Get median key (will be promoted to parent)
    const medianKey = fullChild.keys.pop()!;
    const medianDocIds = fullChild.docIds.pop()!;

    // Move right half of children if internal node
    if (!fullChild.isLeaf) {
      newNode.children = fullChild.children.splice(t);
    }

    // Insert median key into parent
    parent.keys.splice(childIndex, 0, medianKey);
    parent.docIds.splice(childIndex, 0, medianDocIds);

    // Insert new node as parent's child
    parent.children.splice(childIndex + 1, 0, newNode);
  }

  // --------------------------------------------------------------------------
  // Private Methods: Deletion
  // --------------------------------------------------------------------------

  /**
   * Delete a key from a subtree
   */
  private deleteFromNode(
    node: BTreeNode<K>,
    key: K,
    docId?: string
  ): boolean {
    let i = 0;
    while (i < node.keyCount && this.compare(key, node.keys[i]!) > 0) {
      i++;
    }

    // Case 1: Key is in this node
    if (i < node.keyCount && this.compare(key, node.keys[i]!) === 0) {
      if (docId) {
        // Remove specific docId
        const docIndex = node.docIds[i]!.indexOf(docId);
        if (docIndex === -1) return false;

        node.docIds[i]!.splice(docIndex, 1);

        // If docIds array is empty, remove the key entirely
        if (node.docIds[i]!.length === 0) {
          return this.deleteKey(node, i);
        }
        return true;
      } else {
        // Remove all docIds (entire key)
        return this.deleteKey(node, i);
      }
    }

    // Case 2: Key is not in this leaf node
    if (node.isLeaf) {
      return false;
    }

    // Case 3: Key may be in child subtree
    const isLastChild = i === node.keyCount;

    // Ensure child has enough keys before descending
    if (node.children[i]!.keyCount < this.minDegree) {
      this.fillChild(node, i);
    }

    // If last child was merged with previous child, descend into previous
    if (isLastChild && i > node.keyCount) {
      return this.deleteFromNode(node.children[i - 1]!, key, docId);
    }

    return this.deleteFromNode(node.children[i]!, key, docId);
  }

  /**
   * Delete a key at a specific index in a node
   */
  private deleteKey(node: BTreeNode<K>, index: number): boolean {
    if (node.isLeaf) {
      // Simply remove the key
      node.keys.splice(index, 1);
      node.docIds.splice(index, 1);
      return true;
    }

    const key = node.keys[index]!;

    // If left child has enough keys, find predecessor
    if (node.children[index]!.keyCount >= this.minDegree) {
      const [predKey, predDocIds] = this.findPredecessor(node.children[index]!);
      node.keys[index] = predKey;
      node.docIds[index] = predDocIds;
      return this.deleteFromNode(node.children[index]!, predKey);
    }

    // If right child has enough keys, find successor
    if (node.children[index + 1]!.keyCount >= this.minDegree) {
      const [succKey, succDocIds] = this.findSuccessor(node.children[index + 1]!);
      node.keys[index] = succKey;
      node.docIds[index] = succDocIds;
      return this.deleteFromNode(node.children[index + 1]!, succKey);
    }

    // Both children have minimum keys, merge them
    this.mergeChildren(node, index);
    return this.deleteFromNode(node.children[index]!, key);
  }

  /**
   * Find predecessor (rightmost key in left subtree)
   */
  private findPredecessor(node: BTreeNode<K>): [K, string[]] {
    while (!node.isLeaf) {
      node = node.children[node.children.length - 1]!;
    }
    const lastIndex = node.keyCount - 1;
    return [node.keys[lastIndex]!, [...node.docIds[lastIndex]!]];
  }

  /**
   * Find successor (leftmost key in right subtree)
   */
  private findSuccessor(node: BTreeNode<K>): [K, string[]] {
    while (!node.isLeaf) {
      node = node.children[0]!;
    }
    return [node.keys[0]!, [...node.docIds[0]!]];
  }

  /**
   * Ensure a child has at least minDegree keys
   */
  private fillChild(parent: BTreeNode<K>, childIndex: number): void {
    // Try to borrow from left sibling
    if (childIndex > 0 && parent.children[childIndex - 1]!.keyCount >= this.minDegree) {
      this.borrowFromPrev(parent, childIndex);
    }
    // Try to borrow from right sibling
    else if (
      childIndex < parent.keyCount &&
      parent.children[childIndex + 1]!.keyCount >= this.minDegree
    ) {
      this.borrowFromNext(parent, childIndex);
    }
    // Merge with a sibling
    else {
      if (childIndex < parent.keyCount) {
        this.mergeChildren(parent, childIndex);
      } else {
        this.mergeChildren(parent, childIndex - 1);
      }
    }
  }

  /**
   * Borrow a key from the previous sibling
   */
  private borrowFromPrev(parent: BTreeNode<K>, childIndex: number): void {
    const child = parent.children[childIndex]!;
    const sibling = parent.children[childIndex - 1]!;

    // Move parent key down to child
    child.keys.unshift(parent.keys[childIndex - 1]!);
    child.docIds.unshift(parent.docIds[childIndex - 1]!);

    // Move sibling's last key up to parent
    parent.keys[childIndex - 1] = sibling.keys.pop()!;
    parent.docIds[childIndex - 1] = sibling.docIds.pop()!;

    // Move sibling's last child to child
    if (!child.isLeaf) {
      child.children.unshift(sibling.children.pop()!);
    }
  }

  /**
   * Borrow a key from the next sibling
   */
  private borrowFromNext(parent: BTreeNode<K>, childIndex: number): void {
    const child = parent.children[childIndex]!;
    const sibling = parent.children[childIndex + 1]!;

    // Move parent key down to child
    child.keys.push(parent.keys[childIndex]!);
    child.docIds.push(parent.docIds[childIndex]!);

    // Move sibling's first key up to parent
    parent.keys[childIndex] = sibling.keys.shift()!;
    parent.docIds[childIndex] = sibling.docIds.shift()!;

    // Move sibling's first child to child
    if (!child.isLeaf) {
      child.children.push(sibling.children.shift()!);
    }
  }

  /**
   * Merge child at index with child at index+1
   */
  private mergeChildren(parent: BTreeNode<K>, index: number): void {
    const leftChild = parent.children[index]!;
    const rightChild = parent.children[index + 1]!;

    // Move parent key to left child
    leftChild.keys.push(parent.keys[index]!);
    leftChild.docIds.push(parent.docIds[index]!);

    // Move all keys from right child to left child
    leftChild.keys.push(...rightChild.keys);
    leftChild.docIds.push(...rightChild.docIds);

    // Move all children from right child to left child
    if (!leftChild.isLeaf) {
      leftChild.children.push(...rightChild.children);
    }

    // Remove key and right child from parent
    parent.keys.splice(index, 1);
    parent.docIds.splice(index, 1);
    parent.children.splice(index + 1, 1);
  }

  // --------------------------------------------------------------------------
  // Private Methods: Utilities
  // --------------------------------------------------------------------------

  /**
   * Count total keys in a subtree
   */
  private countKeys(node: BTreeNode<K>): number {
    let count = node.keyCount;

    if (!node.isLeaf) {
      for (const child of node.children) {
        count += this.countKeys(child);
      }
    }

    return count;
  }

  /**
   * Collect all nodes for serialization
   */
  private collectNodes(node: BTreeNode<K>, nodes: SerializedNode<K>[]): void {
    nodes.push(node.serialize());

    for (const child of node.children) {
      this.collectNodes(child, nodes);
    }
  }
}

// ============================================================================
// Exports
// ============================================================================

export default BTree;
