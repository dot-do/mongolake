/**
 * Mongoose Schema Unit Tests
 *
 * Tests for the MongoLake Mongoose Schema implementation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  Schema,
  SchemaPath,
  Virtual,
  ObjectId,
} from '../../../src/mongoose/index.js';

// ============================================================================
// Schema Type Support Tests
// ============================================================================

describe('Schema Type Support', () => {
  describe('Basic Types', () => {
    it('supports String type', () => {
      const schema = new Schema({
        name: String,
      });

      const path = schema.path('name');
      expect(path).toBeDefined();
      expect(path!.instance).toBe('String');
    });

    it('supports Number type', () => {
      const schema = new Schema({
        age: Number,
      });

      const path = schema.path('age');
      expect(path!.instance).toBe('Number');
    });

    it('supports Boolean type', () => {
      const schema = new Schema({
        active: Boolean,
      });

      const path = schema.path('active');
      expect(path!.instance).toBe('Boolean');
    });

    it('supports Date type', () => {
      const schema = new Schema({
        createdAt: Date,
      });

      const path = schema.path('createdAt');
      expect(path!.instance).toBe('Date');
    });

    it('supports ObjectId type', () => {
      const schema = new Schema({
        userId: ObjectId,
      });

      const path = schema.path('userId');
      expect(path!.instance).toBe('ObjectId');
    });

    it('supports Buffer type', () => {
      const schema = new Schema({
        data: Buffer,
      });

      const path = schema.path('data');
      expect(path!.instance).toBe('Buffer');
    });

    it('supports Mixed type', () => {
      const schema = new Schema({
        metadata: Object,
      });

      const path = schema.path('metadata');
      expect(path!.instance).toBe('Mixed');
    });

    it('supports Array type', () => {
      const schema = new Schema({
        tags: [String],
      });

      const path = schema.path('tags');
      expect(path!.instance).toBe('Array');
    });

    it('supports Map type', () => {
      const schema = new Schema({
        attributes: Map,
      });

      const path = schema.path('attributes');
      expect(path!.instance).toBe('Map');
    });
  });

  describe('String Type Names', () => {
    it('supports "String" type name', () => {
      const schema = new Schema({
        name: { type: 'String' },
      });

      expect(schema.path('name')!.instance).toBe('String');
    });

    it('supports "Number" type name', () => {
      const schema = new Schema({
        age: { type: 'Number' },
      });

      expect(schema.path('age')!.instance).toBe('Number');
    });

    it('supports "ObjectId" type name', () => {
      const schema = new Schema({
        userId: { type: 'ObjectId' },
      });

      expect(schema.path('userId')!.instance).toBe('ObjectId');
    });

    it('supports "Decimal128" type name', () => {
      const schema = new Schema({
        price: { type: 'Decimal128' },
      });

      expect(schema.path('price')!.instance).toBe('Decimal128');
    });

    it('supports "UUID" type name', () => {
      const schema = new Schema({
        uuid: { type: 'UUID' },
      });

      expect(schema.path('uuid')!.instance).toBe('UUID');
    });

    it('supports "BigInt" type name', () => {
      const schema = new Schema({
        bigNumber: { type: 'BigInt' },
      });

      expect(schema.path('bigNumber')!.instance).toBe('BigInt');
    });
  });

  describe('Nested Schema', () => {
    it('supports nested object definitions', () => {
      const schema = new Schema({
        address: {
          street: String,
          city: String,
          zipCode: String,
        },
      });

      expect(schema.path('address.street')!.instance).toBe('String');
      expect(schema.path('address.city')!.instance).toBe('String');
    });

    it('supports deeply nested objects', () => {
      const schema = new Schema({
        user: {
          profile: {
            settings: {
              theme: String,
            },
          },
        },
      });

      expect(schema.path('user.profile.settings.theme')!.instance).toBe('String');
    });
  });
});

// ============================================================================
// Schema Options Tests
// ============================================================================

describe('Schema Options', () => {
  describe('timestamps option', () => {
    it('adds createdAt and updatedAt fields when timestamps is true', () => {
      const schema = new Schema({ name: String }, { timestamps: true });

      expect(schema.path('createdAt')).toBeDefined();
      expect(schema.path('updatedAt')).toBeDefined();
    });

    it('allows custom timestamp field names', () => {
      const schema = new Schema(
        { name: String },
        {
          timestamps: {
            createdAt: 'created_at',
            updatedAt: 'updated_at',
          },
        }
      );

      expect(schema.path('created_at')).toBeDefined();
      expect(schema.path('updated_at')).toBeDefined();
    });
  });

  describe('_id option', () => {
    it('adds _id field by default', () => {
      const schema = new Schema({ name: String });

      expect(schema.path('_id')).toBeDefined();
    });

    it('does not add _id when _id is false', () => {
      const schema = new Schema({ name: String }, { _id: false });

      expect(schema.path('_id')).toBeUndefined();
    });
  });

  describe('versionKey option', () => {
    it('adds __v field by default', () => {
      const schema = new Schema({ name: String });

      expect(schema.path('__v')).toBeDefined();
    });

    it('allows custom version key name', () => {
      const schema = new Schema({ name: String }, { versionKey: '_version' });

      expect(schema.path('_version')).toBeDefined();
      expect(schema.path('__v')).toBeUndefined();
    });

    it('does not add version key when versionKey is false', () => {
      const schema = new Schema({ name: String }, { versionKey: false });

      expect(schema.path('__v')).toBeUndefined();
    });
  });

  describe('id virtual option', () => {
    it('adds id virtual by default', () => {
      const schema = new Schema({ name: String });

      expect(schema.virtuals.has('id')).toBe(true);
    });

    it('does not add id virtual when id is false', () => {
      const schema = new Schema({ name: String }, { id: false });

      expect(schema.virtuals.has('id')).toBe(false);
    });
  });
});

// ============================================================================
// Schema Path Tests
// ============================================================================

describe('SchemaPath', () => {
  describe('Required validator', () => {
    it('creates required validator', async () => {
      const path = new SchemaPath('name', { type: String, required: true });

      const result = await path.validate(null);
      expect(result.valid).toBe(false);
      expect(result.errors).toHaveLength(1);
    });

    it('passes for non-null values', async () => {
      const path = new SchemaPath('name', { type: String, required: true });

      const result = await path.validate('test');
      expect(result.valid).toBe(true);
    });

    it('supports custom required message', async () => {
      const path = new SchemaPath('name', {
        type: String,
        required: [true, 'Name is required'],
      });

      const result = await path.validate(null);
      expect(result.errors[0]).toBe('Name is required');
    });
  });

  describe('Enum validator', () => {
    it('validates enum values', async () => {
      const path = new SchemaPath('status', {
        type: String,
        enum: ['pending', 'active', 'inactive'],
      });

      const valid = await path.validate('active');
      expect(valid.valid).toBe(true);

      const invalid = await path.validate('unknown');
      expect(invalid.valid).toBe(false);
    });
  });

  describe('Min/Max validators', () => {
    it('validates min for numbers', async () => {
      const path = new SchemaPath('age', { type: Number, min: 0 });

      const valid = await path.validate(25);
      expect(valid.valid).toBe(true);

      const invalid = await path.validate(-5);
      expect(invalid.valid).toBe(false);
    });

    it('validates max for numbers', async () => {
      const path = new SchemaPath('age', { type: Number, max: 120 });

      const valid = await path.validate(25);
      expect(valid.valid).toBe(true);

      const invalid = await path.validate(150);
      expect(invalid.valid).toBe(false);
    });
  });

  describe('String validators', () => {
    it('validates minlength', async () => {
      const path = new SchemaPath('password', { type: String, minlength: 8 });

      const valid = await path.validate('longpassword');
      expect(valid.valid).toBe(true);

      const invalid = await path.validate('short');
      expect(invalid.valid).toBe(false);
    });

    it('validates maxlength', async () => {
      const path = new SchemaPath('username', { type: String, maxlength: 20 });

      const valid = await path.validate('user123');
      expect(valid.valid).toBe(true);

      const invalid = await path.validate('verylongusernamethatexceedslimit');
      expect(invalid.valid).toBe(false);
    });

    it('validates match regex', async () => {
      const path = new SchemaPath('email', {
        type: String,
        match: /^[\w-]+@[\w-]+\.\w+$/,
      });

      const valid = await path.validate('test@example.com');
      expect(valid.valid).toBe(true);

      const invalid = await path.validate('invalid-email');
      expect(invalid.valid).toBe(false);
    });
  });

  describe('Custom validators', () => {
    it('supports custom validator function', async () => {
      const path = new SchemaPath('age', {
        type: Number,
        validate: (v) => v > 0 && v < 150,
      });

      const valid = await path.validate(25);
      expect(valid.valid).toBe(true);

      const invalid = await path.validate(200);
      expect(invalid.valid).toBe(false);
    });

    it('supports async validator', async () => {
      const path = new SchemaPath('email', {
        type: String,
        validate: {
          validator: async (v) => {
            await new Promise((r) => setTimeout(r, 1));
            return v.includes('@');
          },
          message: 'Invalid email',
        },
      });

      const valid = await path.validate('test@example.com');
      expect(valid.valid).toBe(true);
    });
  });

  describe('Default values', () => {
    it('supports static default value', () => {
      const path = new SchemaPath('status', { type: String, default: 'pending' });

      expect(path.getDefault()).toBe('pending');
    });

    it('supports function default value', () => {
      const path = new SchemaPath('createdAt', { type: Date, default: () => new Date() });

      const defaultVal = path.getDefault();
      expect(defaultVal).toBeInstanceOf(Date);
    });
  });

  describe('Getters and Setters', () => {
    it('applies getter function', () => {
      const path = new SchemaPath('name', {
        type: String,
        get: (v) => (v as string).toUpperCase(),
      });

      expect(path.applyGetters('test')).toBe('TEST');
    });

    it('applies setter function', () => {
      const path = new SchemaPath('name', {
        type: String,
        set: (v) => (v as string).trim(),
      });

      expect(path.applySetters('  test  ')).toBe('test');
    });

    it('applies lowercase transformation', () => {
      const path = new SchemaPath('email', { type: String, lowercase: true });

      expect(path.applySetters('Test@Example.COM')).toBe('test@example.com');
    });

    it('applies uppercase transformation', () => {
      const path = new SchemaPath('code', { type: String, uppercase: true });

      expect(path.applySetters('abc')).toBe('ABC');
    });

    it('applies trim transformation', () => {
      const path = new SchemaPath('name', { type: String, trim: true });

      expect(path.applySetters('  test  ')).toBe('test');
    });
  });

  describe('Type casting', () => {
    it('casts to String', () => {
      const path = new SchemaPath('name', String);

      expect(path.cast(123)).toBe('123');
    });

    it('casts to Number', () => {
      const path = new SchemaPath('age', Number);

      expect(path.cast('42')).toBe(42);
    });

    it('casts to Boolean', () => {
      const path = new SchemaPath('active', Boolean);

      expect(path.cast(1)).toBe(true);
      expect(path.cast(0)).toBe(false);
    });

    it('casts to Date', () => {
      const path = new SchemaPath('date', Date);

      const result = path.cast('2024-01-01');
      expect(result).toBeInstanceOf(Date);
    });

    it('casts to ObjectId', () => {
      const path = new SchemaPath('userId', ObjectId);

      const result = path.cast('507f1f77bcf86cd799439011');
      expect(result).toBeInstanceOf(ObjectId);
    });
  });
});

// ============================================================================
// Virtual Fields Tests
// ============================================================================

describe('Virtual Fields', () => {
  it('defines a getter virtual', () => {
    const schema = new Schema({
      firstName: String,
      lastName: String,
    });

    schema.virtual('fullName').get(function (this: { firstName: string; lastName: string }) {
      return `${this.firstName} ${this.lastName}`;
    });

    const virtual = schema.virtuals.get('fullName');
    expect(virtual).toBeDefined();
    expect(virtual!.hasGetter()).toBe(true);
  });

  it('defines a setter virtual', () => {
    const schema = new Schema({
      firstName: String,
      lastName: String,
    });

    schema.virtual('fullName').set(function (this: { firstName: string; lastName: string }, v: string) {
      const parts = (v as string).split(' ');
      this.firstName = parts[0] || '';
      this.lastName = parts[1] || '';
    });

    const virtual = schema.virtuals.get('fullName');
    expect(virtual).toBeDefined();
    expect(virtual!.hasSetter()).toBe(true);
  });

  it('virtual applies getter correctly', () => {
    const schema = new Schema({
      age: Number,
    });

    schema.virtual('isAdult').get(function (this: { age: number }) {
      return this.age >= 18;
    });

    const virtual = schema.virtuals.get('isAdult')!;
    expect(virtual.applyGetters({ age: 25 })).toBe(true);
    expect(virtual.applyGetters({ age: 15 })).toBe(false);
  });

  it('virtual supports population options', () => {
    const schema = new Schema({
      posts: [{ type: 'ObjectId', ref: 'Post' }],
    });

    schema.virtual('postCount', {
      ref: 'Post',
      localField: 'posts',
      foreignField: '_id',
      count: true,
    });

    const virtual = schema.virtuals.get('postCount')!;
    expect(virtual.options.ref).toBe('Post');
    expect(virtual.options.count).toBe(true);
  });
});

// ============================================================================
// Instance Methods Tests
// ============================================================================

describe('Instance Methods', () => {
  it('adds instance method via method()', () => {
    const schema = new Schema({ name: String });

    schema.method('greet', function (this: { name: string }) {
      return `Hello, ${this.name}!`;
    });

    expect(schema.methods.has('greet')).toBe(true);
  });

  it('adds multiple methods via object', () => {
    const schema = new Schema({ name: String, age: Number });

    schema.method({
      greet() {
        return `Hello!`;
      },
      isAdult(this: { age: number }) {
        return this.age >= 18;
      },
    });

    expect(schema.methods.has('greet')).toBe(true);
    expect(schema.methods.has('isAdult')).toBe(true);
  });
});

// ============================================================================
// Static Methods Tests
// ============================================================================

describe('Static Methods', () => {
  it('adds static method via static()', () => {
    const schema = new Schema({ name: String });

    schema.static('findByName', function (name: string) {
      return this.findOne({ name });
    });

    expect(schema.statics.has('findByName')).toBe(true);
  });

  it('adds multiple statics via object', () => {
    const schema = new Schema({ status: String });

    schema.static({
      findActive() {
        return this.find({ status: 'active' });
      },
      findInactive() {
        return this.find({ status: 'inactive' });
      },
    });

    expect(schema.statics.has('findActive')).toBe(true);
    expect(schema.statics.has('findInactive')).toBe(true);
  });
});

// ============================================================================
// Middleware Hooks Tests
// ============================================================================

describe('Middleware Hooks', () => {
  describe('Pre middleware', () => {
    it('adds pre save middleware', () => {
      const schema = new Schema({ name: String });

      schema.pre('save', function (next) {
        next();
      });

      const middleware = schema.getPreMiddleware('save');
      expect(middleware).toHaveLength(1);
    });

    it('adds multiple pre hooks for same event', () => {
      const schema = new Schema({ name: String });

      schema.pre('save', function (next) {
        next();
      });
      schema.pre('save', function (next) {
        next();
      });

      const middleware = schema.getPreMiddleware('save');
      expect(middleware).toHaveLength(2);
    });

    it('supports array of hooks', () => {
      const schema = new Schema({ name: String });

      schema.pre(['save', 'validate'], function (next) {
        next();
      });

      expect(schema.getPreMiddleware('save')).toHaveLength(1);
      expect(schema.getPreMiddleware('validate')).toHaveLength(1);
    });
  });

  describe('Post middleware', () => {
    it('adds post save middleware', () => {
      const schema = new Schema({ name: String });

      schema.post('save', function (doc, next) {
        next();
      });

      const middleware = schema.getPostMiddleware('save');
      expect(middleware).toHaveLength(1);
    });
  });

  describe('Middleware execution', () => {
    it('runs pre middleware in order', async () => {
      const schema = new Schema({ name: String });
      const order: number[] = [];

      schema.pre('save', function (next) {
        order.push(1);
        next();
      });
      schema.pre('save', function (next) {
        order.push(2);
        next();
      });

      const context = { name: 'test' };
      await schema.runPreMiddleware('save', context);

      expect(order).toEqual([1, 2]);
    });

    it('runs post middleware in order', async () => {
      const schema = new Schema({ name: String });
      const order: number[] = [];

      schema.post('save', function (doc, next) {
        order.push(1);
        next();
      });
      schema.post('save', function (doc, next) {
        order.push(2);
        next();
      });

      const context = { name: 'test' };
      await schema.runPostMiddleware('save', context, context);

      expect(order).toEqual([1, 2]);
    });

    it('stops on middleware error', async () => {
      const schema = new Schema({ name: String });

      schema.pre('save', function (next) {
        next(new Error('Middleware error'));
      });

      const context = { name: 'test' };

      await expect(schema.runPreMiddleware('save', context)).rejects.toThrow('Middleware error');
    });
  });
});

// ============================================================================
// Plugin Support Tests
// ============================================================================

describe('Plugin Support', () => {
  it('applies plugin to schema', () => {
    const schema = new Schema({ name: String });

    const plugin = (schema: Schema) => {
      schema.method('pluginMethod', function () {
        return 'from plugin';
      });
    };

    schema.plugin(plugin);

    expect(schema.methods.has('pluginMethod')).toBe(true);
  });

  it('passes options to plugin', () => {
    const schema = new Schema({ name: String });

    const plugin = (schema: Schema, options?: Record<string, unknown>) => {
      schema.virtual('pluginField').get(() => options?.defaultValue);
    };

    schema.plugin(plugin, { defaultValue: 'test' });

    const virtual = schema.virtuals.get('pluginField')!;
    expect(virtual.applyGetters({})).toBe('test');
  });

  it('returns plugins list', () => {
    const schema = new Schema({ name: String });

    const plugin1 = () => {};
    const plugin2 = () => {};

    schema.plugin(plugin1);
    schema.plugin(plugin2, { option: true });

    const plugins = schema.getPlugins();
    expect(plugins).toHaveLength(2);
  });
});

// ============================================================================
// Index Support Tests
// ============================================================================

describe('Index Support', () => {
  it('adds single field index', () => {
    const schema = new Schema({ email: String });

    schema.index({ email: 1 });

    expect(schema.indexes).toHaveLength(1);
    expect(schema.indexes[0]!.fields).toEqual({ email: 1 });
  });

  it('adds compound index', () => {
    const schema = new Schema({ firstName: String, lastName: String });

    schema.index({ firstName: 1, lastName: 1 });

    expect(schema.indexes[0]!.fields).toEqual({ firstName: 1, lastName: 1 });
  });

  it('adds index with options', () => {
    const schema = new Schema({ email: String });

    schema.index({ email: 1 }, { unique: true, sparse: true });

    expect(schema.indexes[0]!.options).toEqual({ unique: true, sparse: true });
  });

  it('adds text index', () => {
    const schema = new Schema({ content: String });

    schema.index({ content: 'text' });

    expect(schema.indexes[0]!.fields).toEqual({ content: 'text' });
  });
});

// ============================================================================
// Schema Utilities Tests
// ============================================================================

describe('Schema Utilities', () => {
  it('clone() creates a copy of the schema', () => {
    const schema = new Schema({ name: String });
    schema.method('test', () => {});
    schema.virtual('virt').get(() => 'value');

    const cloned = schema.clone();

    expect(cloned.paths.has('name')).toBe(true);
    expect(cloned.methods.has('test')).toBe(true);
    expect(cloned.virtuals.has('virt')).toBe(true);
  });

  it('pick() creates schema with selected paths', () => {
    const schema = new Schema({
      name: String,
      email: String,
      age: Number,
    });

    const picked = schema.pick(['name', 'email']);

    expect(picked.paths.has('name')).toBe(true);
    expect(picked.paths.has('email')).toBe(true);
    expect(picked.paths.has('age')).toBe(false);
  });

  it('omit() creates schema without specified paths', () => {
    const schema = new Schema({
      name: String,
      email: String,
      password: String,
    });

    const omitted = schema.omit(['password']);

    expect(omitted.paths.has('name')).toBe(true);
    expect(omitted.paths.has('email')).toBe(true);
    expect(omitted.paths.has('password')).toBe(false);
  });

  it('pathNames() returns all path names', () => {
    const schema = new Schema({ name: String, age: Number });

    const names = schema.pathNames();

    expect(names).toContain('name');
    expect(names).toContain('age');
    expect(names).toContain('_id');
  });

  it('requiredPaths() returns only required paths', () => {
    const schema = new Schema({
      name: { type: String, required: true },
      email: { type: String, required: true },
      age: Number,
    });

    const required = schema.requiredPaths();

    expect(required).toContain('name');
    expect(required).toContain('email');
    expect(required).not.toContain('age');
  });

  it('pathType() identifies path types correctly', () => {
    const schema = new Schema({
      name: String,
      address: {
        street: String,
      },
    });
    schema.virtual('displayName').get(() => '');

    expect(schema.pathType('name')).toBe('real');
    expect(schema.pathType('displayName')).toBe('virtual');
    expect(schema.pathType('address')).toBe('nested');
    expect(schema.pathType('unknown')).toBe('adhocOrUndefined');
  });

  it('toJSON() returns schema structure', () => {
    const schema = new Schema({ name: String });
    schema.method('test', () => {});
    schema.virtual('virt').get(() => '');

    const json = schema.toJSON();

    expect(json.paths).toBeDefined();
    expect(json.virtuals).toContain('virt');
    expect(json.methods).toContain('test');
  });
});
