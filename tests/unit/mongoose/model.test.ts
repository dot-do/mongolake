/**
 * Mongoose Model Unit Tests
 *
 * Tests for the MongoLake Mongoose Model implementation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  Schema,
  Model,
  model,
  getModel,
  deleteModel,
  modelNames,
  MongooseDocument,
  ObjectId,
} from '../../../src/mongoose/index.js';
import type { Document } from '../../../src/types.js';

// ============================================================================
// Mock Collection
// ============================================================================

function createMockCollection<T extends Document = Document>() {
  const docs = new Map<string, T>();

  return {
    name: 'test-collection',
    insertOne: vi.fn(async (doc: T) => {
      const id = (doc._id as string) || new ObjectId().toString();
      docs.set(String(id), { ...doc, _id: id } as T);
      return { acknowledged: true, insertedId: id };
    }),
    insertMany: vi.fn(async (docsToInsert: T[]) => {
      const insertedIds: { [key: number]: string | ObjectId } = {};
      docsToInsert.forEach((doc, i) => {
        const id = (doc._id as string) || new ObjectId().toString();
        docs.set(String(id), { ...doc, _id: id } as T);
        insertedIds[i] = id;
      });
      return { acknowledged: true, insertedCount: docsToInsert.length, insertedIds };
    }),
    findOne: vi.fn(async (filter?: { _id?: string | ObjectId }) => {
      if (filter?._id) {
        return docs.get(String(filter._id)) || null;
      }
      const first = docs.values().next();
      return first.done ? null : first.value;
    }),
    find: vi.fn((filter?: { _id?: string | ObjectId }) => {
      const results: T[] = [];
      if (filter?._id) {
        const doc = docs.get(String(filter._id));
        if (doc) results.push(doc);
      } else {
        for (const doc of docs.values()) {
          results.push(doc);
        }
      }
      return {
        toArray: async () => results,
        sort: () => ({ toArray: async () => results, limit: () => ({ toArray: async () => results }) }),
        limit: () => ({ toArray: async () => results }),
        skip: () => ({ toArray: async () => results }),
        project: () => ({ toArray: async () => results }),
        [Symbol.asyncIterator]: async function* () {
          for (const doc of results) yield doc;
        },
      };
    }),
    updateOne: vi.fn(async (filter: { _id?: string | ObjectId }, update: { $set?: Partial<T> }) => {
      const id = filter._id ? String(filter._id) : null;
      if (id && docs.has(id)) {
        const existing = docs.get(id)!;
        if (update.$set) {
          docs.set(id, { ...existing, ...update.$set } as T);
        }
        return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    }),
    updateMany: vi.fn(async () => {
      return { acknowledged: true, matchedCount: docs.size, modifiedCount: docs.size, upsertedCount: 0 };
    }),
    replaceOne: vi.fn(async (filter: { _id?: string | ObjectId }, replacement: T) => {
      const id = filter._id ? String(filter._id) : null;
      if (id && docs.has(id)) {
        docs.set(id, { ...replacement, _id: filter._id } as T);
        return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    }),
    deleteOne: vi.fn(async (filter: { _id?: string | ObjectId }) => {
      const id = filter._id ? String(filter._id) : null;
      if (id && docs.delete(id)) {
        return { acknowledged: true, deletedCount: 1 };
      }
      return { acknowledged: true, deletedCount: 0 };
    }),
    deleteMany: vi.fn(async () => {
      const count = docs.size;
      docs.clear();
      return { acknowledged: true, deletedCount: count };
    }),
    countDocuments: vi.fn(async () => docs.size),
    estimatedDocumentCount: vi.fn(async () => docs.size),
    distinct: vi.fn(async <K extends keyof T>(field: K) => {
      const values = new Set<T[K]>();
      for (const doc of docs.values()) {
        if (doc[field] !== undefined) values.add(doc[field]);
      }
      return Array.from(values);
    }),
    aggregate: vi.fn(() => ({
      toArray: async () => [],
      [Symbol.asyncIterator]: async function* () {},
    })),
    createIndex: vi.fn(async (_spec: object, options?: { name?: string }) => options?.name || 'test_index'),
    createIndexes: vi.fn(async () => ['test_index']),
    dropIndex: vi.fn(async () => {}),
    listIndexes: vi.fn(async () => [{ name: '_id_', key: { _id: 1 } }]),
    _docs: docs,
  };
}

function createMockConnection() {
  const collections = new Map<string, ReturnType<typeof createMockCollection>>();

  return {
    collection: <T extends Document = Document>(name: string) => {
      if (!collections.has(name)) {
        collections.set(name, createMockCollection<T>());
      }
      return collections.get(name)!;
    },
  };
}

// ============================================================================
// Model Creation Tests
// ============================================================================

describe('Model Creation', () => {
  afterEach(() => {
    // Clean up models
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('creates a model from schema', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();

    const UserModel = new Model('User', schema, { connection });

    expect(UserModel.modelName).toBe('User');
    expect(UserModel.schema).toBe(schema);
  });

  it('registers model globally', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();

    new Model('TestModel', schema, { connection });

    expect(getModel('TestModel')).toBeDefined();
  });

  it('deletes model from registry', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();

    new Model('ToDelete', schema, { connection });
    deleteModel('ToDelete');

    expect(getModel('ToDelete')).toBeUndefined();
  });

  it('returns model names', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();

    new Model('Model1', schema, { connection });
    new Model('Model2', schema, { connection });

    const names = modelNames();
    expect(names).toContain('Model1');
    expect(names).toContain('Model2');
  });

  it('model() factory returns existing model', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();

    const Model1 = new Model('Existing', schema, { connection });
    const Model2 = model('Existing');

    expect(Model2).toBe(Model1);
  });
});

// ============================================================================
// Static Methods Tests
// ============================================================================

describe('Model Static Methods from Schema', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('applies static methods from schema to model', () => {
    const schema = new Schema({ name: String, status: String });

    schema.static('findActive', async function (this: Model<Document>) {
      return this.find({ status: 'active' });
    });

    const connection = createMockConnection();
    const TestModel = new Model('StaticTest', schema, { connection }) as Model<Document> & {
      findActive: () => Promise<Document[]>;
    };

    expect(typeof TestModel.findActive).toBe('function');
  });
});

// ============================================================================
// Document Creation Tests
// ============================================================================

describe('Document Creation', () => {
  let UserModel: Model<Document>;
  let connection: ReturnType<typeof createMockConnection>;

  beforeEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }

    const schema = new Schema({
      name: { type: String, required: true },
      age: Number,
      email: String,
    });

    connection = createMockConnection();
    UserModel = new Model('User', schema, { connection });
  });

  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('creates a new document with new()', () => {
    const doc = UserModel.new({ name: 'Alice', age: 30 });

    expect(doc).toBeDefined();
    expect(doc.isNew).toBe(true);
    expect(doc.get('name')).toBe('Alice');
  });

  it('applies default values to new documents', () => {
    const schema = new Schema({
      name: String,
      status: { type: String, default: 'pending' },
    });
    const connection = createMockConnection();
    const TestModel = new Model('DefaultTest', schema, { connection });

    const doc = TestModel.new({ name: 'Test' });

    expect(doc.get('status')).toBe('pending');
  });

  it('generates _id by default', () => {
    const doc = UserModel.new({ name: 'Alice' });

    expect(doc.get('_id')).toBeDefined();
    expect(doc.get('_id')).toBeInstanceOf(ObjectId);
  });
});

// ============================================================================
// CRUD Operations Tests
// ============================================================================

describe('Model CRUD Operations', () => {
  let UserModel: Model<Document>;
  let connection: ReturnType<typeof createMockConnection>;

  beforeEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }

    const schema = new Schema({
      name: String,
      email: String,
      age: Number,
    });

    connection = createMockConnection();
    UserModel = new Model('User', schema, { connection });
  });

  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  describe('create()', () => {
    it('creates a single document', async () => {
      const doc = await UserModel.create({ name: 'Alice', email: 'alice@test.com' });

      expect(doc).toBeDefined();
      expect(doc.isNew).toBe(false);
      expect(doc.get('name')).toBe('Alice');
    });

    it('creates multiple documents', async () => {
      const docs = await UserModel.create([
        { name: 'Alice' },
        { name: 'Bob' },
      ]);

      expect(docs).toHaveLength(2);
      expect(docs[0]!.get('name')).toBe('Alice');
      expect(docs[1]!.get('name')).toBe('Bob');
    });

    it('runs pre/post save middleware', async () => {
      const schema = new Schema({ name: String });
      const preSpy = vi.fn((next: Function) => next());
      const postSpy = vi.fn((_doc: unknown, next: Function) => next());

      schema.pre('save', preSpy);
      schema.post('save', postSpy);

      const conn = createMockConnection();
      const TestModel = new Model('MiddlewareTest', schema, { connection: conn });

      await TestModel.create({ name: 'Test' });

      expect(preSpy).toHaveBeenCalled();
      expect(postSpy).toHaveBeenCalled();
    });
  });

  describe('insertMany()', () => {
    it('inserts multiple documents', async () => {
      const docs = await UserModel.insertMany([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ]);

      expect(docs).toHaveLength(3);
    });
  });

  describe('find()', () => {
    it('returns a query', () => {
      const query = UserModel.find({ name: 'Alice' });

      expect(query).toBeDefined();
      expect(typeof query.exec).toBe('function');
    });

    it('supports chaining', () => {
      const query = UserModel.find()
        .where('name', 'Alice')
        .sort({ createdAt: -1 })
        .limit(10);

      expect(query).toBeDefined();
    });
  });

  describe('findOne()', () => {
    it('returns a query for single document', () => {
      const query = UserModel.findOne({ name: 'Alice' });

      expect(query).toBeDefined();
      expect(typeof query.exec).toBe('function');
    });
  });

  describe('findById()', () => {
    it('creates a query with _id filter', () => {
      const id = new ObjectId();
      const query = UserModel.findById(id);

      expect(query.getFilter()).toEqual({ _id: id });
    });

    it('accepts string id', () => {
      const query = UserModel.findById('507f1f77bcf86cd799439011');

      expect(query.getFilter()._id).toBeInstanceOf(ObjectId);
    });
  });

  describe('updateOne()', () => {
    it('returns an update query', () => {
      const query = UserModel.updateOne({ name: 'Alice' }, { $set: { age: 31 } });

      expect(query).toBeDefined();
      expect(query.getUpdate()).toEqual({ $set: { age: 31 } });
    });
  });

  describe('updateMany()', () => {
    it('returns an update query for multiple documents', () => {
      const query = UserModel.updateMany({}, { $set: { active: true } });

      expect(query).toBeDefined();
    });
  });

  describe('deleteOne()', () => {
    it('returns a delete query', () => {
      const query = UserModel.deleteOne({ name: 'Alice' });

      expect(query).toBeDefined();
    });
  });

  describe('deleteMany()', () => {
    it('returns a delete query for multiple documents', () => {
      const query = UserModel.deleteMany({ status: 'inactive' });

      expect(query).toBeDefined();
    });
  });

  describe('countDocuments()', () => {
    it('returns a count query', () => {
      const query = UserModel.countDocuments({ status: 'active' });

      expect(query).toBeDefined();
    });
  });

  describe('distinct()', () => {
    it('returns a distinct query', () => {
      const query = UserModel.distinct('status' as keyof Document);

      expect(query).toBeDefined();
    });
  });

  describe('exists()', () => {
    it('checks if document exists', async () => {
      await UserModel.create({ name: 'Alice' });

      const exists = await UserModel.exists({ name: 'Alice' });

      expect(exists).not.toBeNull();
    });
  });
});

// ============================================================================
// Population Tests
// ============================================================================

describe('Population', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('populates referenced documents', async () => {
    const userSchema = new Schema({ name: String });
    const postSchema = new Schema({
      title: String,
      author: { type: 'ObjectId', ref: 'PopUser' },
    });

    const connection = createMockConnection();
    const UserModel = new Model('PopUser', userSchema, { connection });
    const PostModel = new Model('Post', postSchema, { connection });

    // Create a user
    const user = await UserModel.create({ name: 'Alice' });
    const userId = user.get('_id') as ObjectId;

    // Create a post
    await PostModel.create({ title: 'Hello World', author: userId });

    // Populate
    const posts = await PostModel.find().populate('author');

    expect(posts).toBeDefined();
  });
});

// ============================================================================
// Discriminator Tests
// ============================================================================

describe('Discriminators', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('creates a discriminator model', () => {
    const eventSchema = new Schema({ timestamp: Date });
    const connection = createMockConnection();
    const EventModel = new Model('Event', eventSchema, { connection });

    const clickSchema = new Schema({ element: String });
    const ClickEvent = EventModel.discriminator('ClickEvent', clickSchema);

    expect(ClickEvent.modelName).toBe('ClickEvent');
    expect(ClickEvent.schema.path('element')).toBeDefined();
    expect(ClickEvent.schema.path('timestamp')).toBeDefined();
  });

  it('adds discriminator key', () => {
    const eventSchema = new Schema({ timestamp: Date }, { discriminatorKey: 'type' });
    const connection = createMockConnection();
    const EventModel = new Model('BaseEvent', eventSchema, { connection });

    const clickSchema = new Schema({ element: String });
    const ClickEvent = EventModel.discriminator('Click', clickSchema);

    expect(ClickEvent.schema.path('type')).toBeDefined();
  });
});

// ============================================================================
// Index Operations Tests
// ============================================================================

describe('Index Operations', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('createIndexes() creates schema indexes', async () => {
    const schema = new Schema({ email: String });
    schema.index({ email: 1 }, { unique: true });

    const connection = createMockConnection();
    const TestModel = new Model('IndexTest', schema, { connection });

    await TestModel.createIndexes();

    expect(connection.collection('indextests').createIndex).toHaveBeenCalled();
  });

  it('syncIndexes() syncs with database', async () => {
    const schema = new Schema({ email: String });
    schema.index({ email: 1 });

    const connection = createMockConnection();
    const TestModel = new Model('SyncIndexTest', schema, { connection });

    const created = await TestModel.syncIndexes();

    expect(Array.isArray(created)).toBe(true);
  });

  it('listIndexes() returns indexes', async () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('ListIndexTest', schema, { connection });

    const indexes = await TestModel.listIndexes();

    expect(indexes).toBeDefined();
    expect(indexes[0]!.name).toBe('_id_');
  });
});

// ============================================================================
// Hydration Tests
// ============================================================================

describe('Hydration', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('hydrate() converts plain object to document', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();
    const TestModel = new Model('HydrateTest', schema, { connection });

    const doc = TestModel.hydrate({ _id: '123', name: 'Alice', age: 30 });

    expect(doc.get('name')).toBe('Alice');
    expect(doc.isNew).toBe(false);
  });
});

// ============================================================================
// Aggregation Tests
// ============================================================================

describe('Aggregation', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('aggregate() returns aggregation builder', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();
    const TestModel = new Model('AggTest', schema, { connection });

    const agg = TestModel.aggregate([{ $match: { age: { $gte: 18 } } }]);

    expect(agg).toBeDefined();
    expect(typeof agg.match).toBe('function');
    expect(typeof agg.group).toBe('function');
    expect(typeof agg.sort).toBe('function');
  });

  it('aggregation supports chaining', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();
    const TestModel = new Model('AggChainTest', schema, { connection });

    const agg = TestModel.aggregate([])
      .match({ age: { $gte: 18 } })
      .group({ _id: '$status', count: { $sum: 1 } })
      .sort({ count: -1 })
      .limit(10);

    expect(agg).toBeDefined();
  });
});
