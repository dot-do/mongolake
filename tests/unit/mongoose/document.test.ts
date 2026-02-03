/**
 * Mongoose Document Unit Tests
 *
 * Tests for the MongoLake Mongoose Document implementation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  Schema,
  Model,
  MongooseDocument,
  ObjectId,
  deleteModel,
  modelNames,
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
    find: vi.fn(() => ({
      toArray: async () => Array.from(docs.values()),
    })),
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
    countDocuments: vi.fn(async () => docs.size),
    estimatedDocumentCount: vi.fn(async () => docs.size),
    distinct: vi.fn(async () => []),
    aggregate: vi.fn(() => ({
      toArray: async () => [],
    })),
    createIndex: vi.fn(async () => 'test_index'),
    createIndexes: vi.fn(async () => ['test_index']),
    dropIndex: vi.fn(async () => {}),
    listIndexes: vi.fn(async () => []),
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
// Document Get/Set Tests
// ============================================================================

describe('Document Get/Set', () => {
  let TestModel: Model<Document>;

  beforeEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }

    const schema = new Schema({
      name: String,
      age: Number,
      email: String,
      address: {
        street: String,
        city: String,
      },
    });

    const connection = createMockConnection();
    TestModel = new Model('DocTest', schema, { connection });
  });

  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('get() returns field value', () => {
    const doc = TestModel.new({ name: 'Alice', age: 30 });

    expect(doc.get('name')).toBe('Alice');
    expect(doc.get('age')).toBe(30);
  });

  it('get() returns nested field value', () => {
    const doc = TestModel.new({
      name: 'Alice',
      address: { street: '123 Main St', city: 'Boston' },
    });

    expect(doc.get('address.street')).toBe('123 Main St');
    expect(doc.get('address.city')).toBe('Boston');
  });

  it('set() updates field value', () => {
    const doc = TestModel.new({ name: 'Alice' });

    doc.set('name', 'Bob');

    expect(doc.get('name')).toBe('Bob');
  });

  it('set() updates nested field value', () => {
    const doc = TestModel.new({
      name: 'Alice',
      address: { street: '123 Main St', city: 'Boston' },
    });

    doc.set('address.city', 'New York');

    expect(doc.get('address.city')).toBe('New York');
  });

  it('set() accepts object for multiple fields', () => {
    const doc = TestModel.new({ name: 'Alice' });

    doc.set({ name: 'Bob', age: 25 });

    expect(doc.get('name')).toBe('Bob');
    expect(doc.get('age')).toBe(25);
  });

  it('unset() removes field', () => {
    const doc = TestModel.new({ name: 'Alice', age: 30 });

    doc.unset('age');

    expect(doc.get('age')).toBeUndefined();
  });
});

// ============================================================================
// Document Modification Tracking Tests
// ============================================================================

describe('Document Modification Tracking', () => {
  let TestModel: Model<Document>;

  beforeEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }

    const schema = new Schema({
      name: String,
      age: Number,
      email: String,
    });

    const connection = createMockConnection();
    TestModel = new Model('ModTest', schema, { connection });
  });

  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('isModified() returns false for new document without changes', () => {
    const doc = TestModel.new({ name: 'Alice' });

    expect(doc.isModified()).toBe(false);
  });

  it('isModified() returns true after set()', () => {
    const doc = TestModel.new({ name: 'Alice' });
    doc.set('name', 'Bob');

    expect(doc.isModified()).toBe(true);
  });

  it('isModified(path) checks specific path', () => {
    const doc = TestModel.new({ name: 'Alice', age: 30 });
    doc.set('name', 'Bob');

    expect(doc.isModified('name')).toBe(true);
    expect(doc.isModified('age')).toBe(false);
  });

  it('isModified() accepts array of paths', () => {
    const doc = TestModel.new({ name: 'Alice', age: 30, email: 'alice@test.com' });
    doc.set('name', 'Bob');

    expect(doc.isModified(['name', 'age'])).toBe(true);
    expect(doc.isModified(['age', 'email'])).toBe(false);
  });

  it('isDirectModified() checks direct modification only', () => {
    const doc = TestModel.new({ name: 'Alice' });
    doc.set('name', 'Bob');

    expect(doc.isDirectModified('name')).toBe(true);
  });

  it('markModified() marks path as modified', () => {
    const doc = TestModel.new({ name: 'Alice' });
    doc.markModified('name');

    expect(doc.isModified('name')).toBe(true);
  });

  it('modifiedPaths() returns list of modified paths', () => {
    const doc = TestModel.new({ name: 'Alice', age: 30 });
    doc.set('name', 'Bob');
    doc.set('age', 31);

    const modified = doc.modifiedPaths();

    expect(modified).toContain('name');
    expect(modified).toContain('age');
  });
});

// ============================================================================
// Document Validation Tests
// ============================================================================

describe('Document Validation', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('validate() passes for valid document', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
      age: { type: Number, min: 0 },
    });

    const connection = createMockConnection();
    const TestModel = new Model('ValidTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice', age: 25 });

    await expect(doc.validate()).resolves.not.toThrow();
  });

  it('validate() fails for missing required field', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('RequiredTest', schema, { connection });
    const doc = TestModel.new({});

    await expect(doc.validate()).rejects.toThrow();
  });

  it('validate() fails for invalid value', async () => {
    const schema = new Schema({
      age: { type: Number, min: 0 },
    });

    const connection = createMockConnection();
    const TestModel = new Model('MinTest', schema, { connection });
    const doc = TestModel.new({ age: -5 });

    await expect(doc.validate()).rejects.toThrow();
  });

  it('validate() accepts specific paths to validate', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
      email: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('PathValidTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice' }); // email missing

    // Validate only name path - should pass
    await expect(doc.validate(['name'])).resolves.not.toThrow();
  });

  it('validateSync() returns undefined for valid document', () => {
    const schema = new Schema({
      name: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('SyncValidTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice' });

    expect(doc.validateSync()).toBeUndefined();
  });

  it('validateSync() returns error for invalid document', () => {
    const schema = new Schema({
      name: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('SyncInvalidTest', schema, { connection });
    const doc = TestModel.new({});

    const error = doc.validateSync();
    expect(error).toBeDefined();
    expect(error!.errors.name).toBeDefined();
  });

  it('invalidate() manually adds validation error', () => {
    const schema = new Schema({ name: String });

    const connection = createMockConnection();
    const TestModel = new Model('InvalidateTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice' });

    const error = doc.invalidate('name', 'Name is invalid');

    expect(error.errors.name).toBeDefined();
    expect(error.errors.name!.message).toBe('Name is invalid');
  });

  it('$ignore() excludes path from validation', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
      email: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('IgnoreTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice' }); // email missing

    doc.$ignore('email');

    await expect(doc.validate()).resolves.not.toThrow();
  });
});

// ============================================================================
// Document Save Tests
// ============================================================================

describe('Document Save', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('save() inserts new document', async () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('SaveNewTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();

    expect(doc.isNew).toBe(false);
    expect(connection.collection('savenewtests').insertOne).toHaveBeenCalled();
  });

  it('save() updates existing document', async () => {
    const schema = new Schema({ name: String, __v: Number });
    const connection = createMockConnection();
    const TestModel = new Model('SaveExistingTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();

    doc.set('name', 'Bob');
    await doc.save();

    expect(connection.collection('saveexistingtests').replaceOne).toHaveBeenCalled();
  });

  it('save() validates by default', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('SaveValidateTest', schema, { connection });
    const doc = TestModel.new({});

    await expect(doc.save()).rejects.toThrow();
  });

  it('save() skips validation when validateBeforeSave is false', async () => {
    const schema = new Schema({
      name: { type: String, required: true },
    });

    const connection = createMockConnection();
    const TestModel = new Model('SaveNoValidateTest', schema, { connection });
    const doc = TestModel.new({});

    await expect(doc.save({ validateBeforeSave: false })).resolves.not.toThrow();
  });

  it('save() runs pre/post middleware', async () => {
    const schema = new Schema({ name: String });
    const preSpy = vi.fn((next: Function) => next());
    const postSpy = vi.fn((_doc: unknown, next: Function) => next());

    schema.pre('save', preSpy);
    schema.post('save', postSpy);

    const connection = createMockConnection();
    const TestModel = new Model('SaveMiddlewareTest', schema, { connection });
    const doc = TestModel.new({ name: 'Alice' });

    await doc.save();

    expect(preSpy).toHaveBeenCalled();
    expect(postSpy).toHaveBeenCalled();
  });

  it('save() increments version key', async () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('SaveVersionTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();

    const v1 = doc.get('__v') as number;

    doc.set('name', 'Bob');
    await doc.save();

    const v2 = doc.get('__v') as number;

    expect(v2).toBe(v1 + 1);
  });
});

// ============================================================================
// Document Remove Tests
// ============================================================================

describe('Document Remove', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('remove() deletes document', async () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('RemoveTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();
    await doc.remove();

    expect(doc.$isDeleted()).toBe(true);
    expect(connection.collection('removetests').deleteOne).toHaveBeenCalled();
  });

  it('deleteOne() deletes document', async () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('DeleteOneTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();
    await doc.deleteOne();

    expect(doc.$isDeleted()).toBe(true);
  });

  it('remove() runs pre/post middleware', async () => {
    const schema = new Schema({ name: String });
    const preSpy = vi.fn((next: Function) => next());
    const postSpy = vi.fn((_doc: unknown, next: Function) => next());

    schema.pre('remove', preSpy);
    schema.post('remove', postSpy);

    const connection = createMockConnection();
    const TestModel = new Model('RemoveMiddlewareTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    await doc.save();
    await doc.remove();

    expect(preSpy).toHaveBeenCalled();
    expect(postSpy).toHaveBeenCalled();
  });
});

// ============================================================================
// Document Utilities Tests
// ============================================================================

describe('Document Utilities', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('toObject() returns plain object', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();
    const TestModel = new Model('ToObjectTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice', age: 30 });
    const obj = doc.toObject();

    expect(obj.name).toBe('Alice');
    expect(obj.age).toBe(30);
  });

  it('toObject() includes virtuals when option is set', () => {
    const schema = new Schema({ firstName: String, lastName: String });
    schema.virtual('fullName').get(function (this: { firstName: string; lastName: string }) {
      return `${this.firstName} ${this.lastName}`;
    });

    const connection = createMockConnection();
    const TestModel = new Model('ToObjectVirtualTest', schema, { connection });

    const doc = TestModel.new({ firstName: 'Alice', lastName: 'Smith' });
    const obj = doc.toObject({ virtuals: true });

    expect(obj.fullName).toBe('Alice Smith');
  });

  it('toJSON() returns JSON-serializable object', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('ToJSONTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    const json = doc.toJSON();

    expect(JSON.stringify(json)).toBeDefined();
  });

  it('equals() compares documents by _id', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('EqualsTest', schema, { connection });

    const id = new ObjectId();
    const doc1 = TestModel.new({ _id: id, name: 'Alice' });
    const doc2 = TestModel.new({ _id: id, name: 'Alice' });
    const doc3 = TestModel.new({ name: 'Bob' });

    expect(doc1.equals(doc2)).toBe(true);
    expect(doc1.equals(doc3)).toBe(false);
  });

  it('$clone() creates a copy of the document', () => {
    const schema = new Schema({ name: String, age: Number });
    const connection = createMockConnection();
    const TestModel = new Model('CloneTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice', age: 30 });
    const cloned = doc.$clone();

    expect(cloned.get('name')).toBe('Alice');
    expect(cloned.get('age')).toBe(30);
    expect(cloned).not.toBe(doc);
  });

  it('$isEmpty() checks if document is empty', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('IsEmptyTest', schema, { connection });

    const emptyDoc = TestModel.new({});
    const nonEmptyDoc = TestModel.new({ name: 'Alice' });

    // Note: _id is auto-generated, so checking specific path
    expect(nonEmptyDoc.$isEmpty('age')).toBe(true);
    expect(nonEmptyDoc.$isEmpty('name')).toBe(false);
  });

  it('$isDefault() checks if path has default value', () => {
    const schema = new Schema({
      status: { type: String, default: 'pending' },
    });
    const connection = createMockConnection();
    const TestModel = new Model('IsDefaultTest', schema, { connection });

    const doc = TestModel.new({});

    expect(doc.$isDefault('status')).toBe(true);

    doc.set('status', 'active');
    expect(doc.$isDefault('status')).toBe(false);
  });

  it('overwrite() replaces document data', () => {
    const schema = new Schema({ name: String, age: Number, email: String });
    const connection = createMockConnection();
    const TestModel = new Model('OverwriteTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice', age: 30, email: 'alice@test.com' });
    doc.overwrite({ name: 'Bob', age: 25 });

    expect(doc.get('name')).toBe('Bob');
    expect(doc.get('age')).toBe(25);
    expect(doc.get('email')).toBeUndefined();
  });

  it('increment() increments version key', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('IncrementTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    const v1 = doc.get('__v') as number;

    doc.increment();
    const v2 = doc.get('__v') as number;

    expect(v2).toBe(v1 + 1);
  });

  it('$model() returns the model', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('ModelRefTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });
    const model = doc.$model();

    expect(model).toBe(TestModel);
  });
});

// ============================================================================
// Document Instance Methods Tests
// ============================================================================

describe('Document Instance Methods', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('instance methods from schema are available', () => {
    const schema = new Schema({ name: String });

    schema.method('greet', function (this: MongooseDocument<Document>) {
      return `Hello, ${this.get('name')}!`;
    });

    const connection = createMockConnection();
    const TestModel = new Model('MethodTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' }) as MongooseDocument<Document> & {
      greet: () => string;
    };

    expect(doc.greet()).toBe('Hello, Alice!');
  });

  it('instance methods have access to document context', () => {
    const schema = new Schema({ age: Number });

    schema.method('isAdult', function (this: MongooseDocument<Document>) {
      return (this.get('age') as number) >= 18;
    });

    const connection = createMockConnection();
    const TestModel = new Model('ContextTest', schema, { connection });

    const adult = TestModel.new({ age: 25 }) as MongooseDocument<Document> & {
      isAdult: () => boolean;
    };
    const minor = TestModel.new({ age: 15 }) as MongooseDocument<Document> & {
      isAdult: () => boolean;
    };

    expect(adult.isAdult()).toBe(true);
    expect(minor.isAdult()).toBe(false);
  });
});

// ============================================================================
// Document Virtual Access Tests
// ============================================================================

describe('Document Virtual Access', () => {
  afterEach(() => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('virtual getters are accessible on document', () => {
    const schema = new Schema({ firstName: String, lastName: String });

    schema.virtual('fullName').get(function (this: { firstName: string; lastName: string }) {
      return `${this.firstName} ${this.lastName}`;
    });

    const connection = createMockConnection();
    const TestModel = new Model('VirtualGetterTest', schema, { connection });

    const doc = TestModel.new({ firstName: 'Alice', lastName: 'Smith' }) as MongooseDocument<Document> & {
      fullName: string;
    };

    expect(doc.fullName).toBe('Alice Smith');
  });

  it('virtual setters modify document', () => {
    const schema = new Schema({ firstName: String, lastName: String });

    schema.virtual('fullName')
      .get(function (this: { firstName: string; lastName: string }) {
        return `${this.firstName} ${this.lastName}`;
      })
      .set(function (this: Record<string, unknown>, v: unknown) {
        const parts = (v as string).split(' ');
        this.firstName = parts[0];
        this.lastName = parts[1];
      });

    const connection = createMockConnection();
    const TestModel = new Model('VirtualSetterTest', schema, { connection });

    const doc = TestModel.new({}) as MongooseDocument<Document> & {
      fullName: string;
    };

    doc.fullName = 'Bob Jones';

    expect(doc.get('firstName')).toBe('Bob');
    expect(doc.get('lastName')).toBe('Jones');
  });

  it('id virtual returns string version of _id', () => {
    const schema = new Schema({ name: String });
    const connection = createMockConnection();
    const TestModel = new Model('IdVirtualTest', schema, { connection });

    const doc = TestModel.new({ name: 'Alice' });

    expect(doc.id).toBe(String(doc._id));
  });
});
