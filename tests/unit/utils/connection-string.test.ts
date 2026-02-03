/**
 * Tests for MongoDB connection string parser
 */

import { describe, it, expect } from 'vitest';
import {
  parseConnectionString,
  buildConnectionString,
  isConnectionString,
  ConnectionStringParseError,
  type ParsedConnectionString,
} from '../../../src/utils/connection-string.js';

describe('parseConnectionString', () => {
  describe('basic parsing', () => {
    it('should parse simple mongodb:// URI', () => {
      const result = parseConnectionString('mongodb://localhost:27017');
      expect(result.scheme).toBe('mongodb');
      expect(result.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
      expect(result.username).toBeUndefined();
      expect(result.password).toBeUndefined();
      expect(result.database).toBeUndefined();
    });

    it('should parse mongodb:// URI with database', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb');
      expect(result.scheme).toBe('mongodb');
      expect(result.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
      expect(result.database).toBe('mydb');
    });

    it('should parse mongodb+srv:// URI', () => {
      const result = parseConnectionString('mongodb+srv://cluster.mongodb.net/mydb');
      expect(result.scheme).toBe('mongodb+srv');
      expect(result.hosts).toEqual([{ host: 'cluster.mongodb.net', port: 27017 }]);
      expect(result.database).toBe('mydb');
    });

    it('should use default port 27017 when not specified', () => {
      const result = parseConnectionString('mongodb://localhost');
      expect(result.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
    });
  });

  describe('authentication', () => {
    it('should parse username and password', () => {
      const result = parseConnectionString('mongodb://user:pass@localhost:27017/mydb');
      expect(result.username).toBe('user');
      expect(result.password).toBe('pass');
      expect(result.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
      expect(result.database).toBe('mydb');
    });

    it('should parse username without password', () => {
      const result = parseConnectionString('mongodb://user@localhost:27017/mydb');
      expect(result.username).toBe('user');
      expect(result.password).toBeUndefined();
    });

    it('should decode URL-encoded username and password', () => {
      const result = parseConnectionString('mongodb://user%40name:p%40ss%3Aword@localhost:27017');
      expect(result.username).toBe('user@name');
      expect(result.password).toBe('p@ss:word');
    });

    it('should handle special characters in password', () => {
      const result = parseConnectionString('mongodb://user:p%2Fass%3Fw%26ord@localhost:27017');
      expect(result.password).toBe('p/ass?w&ord');
    });

    it('should mask password in returned URI', () => {
      const result = parseConnectionString('mongodb://user:secretpass@localhost:27017/mydb');
      expect(result.uri).toBe('mongodb://user:****@localhost:27017/mydb');
      expect(result.uri).not.toContain('secretpass');
    });
  });

  describe('hosts', () => {
    it('should parse multiple hosts (replica set)', () => {
      const result = parseConnectionString(
        'mongodb://host1:27017,host2:27017,host3:27017/mydb'
      );
      expect(result.hosts).toEqual([
        { host: 'host1', port: 27017 },
        { host: 'host2', port: 27017 },
        { host: 'host3', port: 27017 },
      ]);
    });

    it('should parse hosts with different ports', () => {
      const result = parseConnectionString('mongodb://host1:27017,host2:27018,host3:27019');
      expect(result.hosts).toEqual([
        { host: 'host1', port: 27017 },
        { host: 'host2', port: 27018 },
        { host: 'host3', port: 27019 },
      ]);
    });

    it('should parse IPv4 addresses', () => {
      const result = parseConnectionString('mongodb://192.168.1.100:27017');
      expect(result.hosts).toEqual([{ host: '192.168.1.100', port: 27017 }]);
    });

    it('should parse IPv6 addresses in brackets', () => {
      const result = parseConnectionString('mongodb://[::1]:27017');
      expect(result.hosts).toEqual([{ host: '::1', port: 27017 }]);
    });

    it('should parse IPv6 address without port', () => {
      const result = parseConnectionString('mongodb://[2001:db8::1]');
      expect(result.hosts).toEqual([{ host: '2001:db8::1', port: 27017 }]);
    });
  });

  describe('options', () => {
    it('should parse authSource option', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?authSource=admin');
      expect(result.options.authSource).toBe('admin');
    });

    it('should parse replicaSet option', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?replicaSet=myrs');
      expect(result.options.replicaSet).toBe('myrs');
    });

    it('should parse ssl option as boolean', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?ssl=true');
      expect(result.options.ssl).toBe(true);
    });

    it('should parse tls option as boolean', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?tls=false');
      expect(result.options.tls).toBe(false);
    });

    it('should parse readPreference option', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?readPreference=secondaryPreferred'
      );
      expect(result.options.readPreference).toBe('secondaryPreferred');
    });

    it('should parse w option as number', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?w=2');
      expect(result.options.w).toBe(2);
    });

    it('should parse w option as majority', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?w=majority');
      expect(result.options.w).toBe('majority');
    });

    it('should parse timeout options as numbers', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?connectTimeoutMS=5000&socketTimeoutMS=30000'
      );
      expect(result.options.connectTimeoutMS).toBe(5000);
      expect(result.options.socketTimeoutMS).toBe(30000);
    });

    it('should parse pool size options', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?maxPoolSize=100&minPoolSize=5'
      );
      expect(result.options.maxPoolSize).toBe(100);
      expect(result.options.minPoolSize).toBe(5);
    });

    it('should parse retryWrites as boolean', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?retryWrites=true');
      expect(result.options.retryWrites).toBe(true);
    });

    it('should parse retryReads as boolean', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?retryReads=false');
      expect(result.options.retryReads).toBe(false);
    });

    it('should parse appName option', () => {
      const result = parseConnectionString('mongodb://localhost:27017/mydb?appName=myapp');
      expect(result.options.appName).toBe('myapp');
    });

    it('should parse compressors option as array', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?compressors=snappy,zlib'
      );
      expect(result.options.compressors).toEqual(['snappy', 'zlib']);
    });

    it('should parse authMechanism option', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?authMechanism=SCRAM-SHA-256'
      );
      expect(result.options.authMechanism).toBe('SCRAM-SHA-256');
    });

    it('should parse directConnection option', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?directConnection=true'
      );
      expect(result.options.directConnection).toBe(true);
    });

    it('should parse multiple options', () => {
      const result = parseConnectionString(
        'mongodb://user:pass@localhost:27017/mydb?authSource=admin&replicaSet=myrs&ssl=true'
      );
      expect(result.options.authSource).toBe('admin');
      expect(result.options.replicaSet).toBe('myrs');
      expect(result.options.ssl).toBe(true);
    });

    it('should decode URL-encoded option values', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?appName=my%20app%21'
      );
      expect(result.options.appName).toBe('my app!');
    });

    it('should store unknown options as strings', () => {
      const result = parseConnectionString(
        'mongodb://localhost:27017/mydb?customOption=customValue'
      );
      expect(result.options.customOption).toBe('customValue');
    });
  });

  describe('complex URIs', () => {
    it('should parse full connection string with all components', () => {
      const result = parseConnectionString(
        'mongodb://admin:secretpass@host1:27017,host2:27018/production?authSource=admin&replicaSet=rs0&ssl=true&retryWrites=true'
      );
      expect(result.scheme).toBe('mongodb');
      expect(result.username).toBe('admin');
      expect(result.password).toBe('secretpass');
      expect(result.hosts).toEqual([
        { host: 'host1', port: 27017 },
        { host: 'host2', port: 27018 },
      ]);
      expect(result.database).toBe('production');
      expect(result.options.authSource).toBe('admin');
      expect(result.options.replicaSet).toBe('rs0');
      expect(result.options.ssl).toBe(true);
      expect(result.options.retryWrites).toBe(true);
    });

    it('should parse Atlas-style connection string', () => {
      const result = parseConnectionString(
        'mongodb+srv://user:pass@cluster0.abc123.mongodb.net/mydb?retryWrites=true&w=majority'
      );
      expect(result.scheme).toBe('mongodb+srv');
      expect(result.username).toBe('user');
      expect(result.hosts).toEqual([{ host: 'cluster0.abc123.mongodb.net', port: 27017 }]);
      expect(result.database).toBe('mydb');
      expect(result.options.retryWrites).toBe(true);
      expect(result.options.w).toBe('majority');
    });

    it('should parse URI without database but with options', () => {
      const result = parseConnectionString('mongodb://localhost:27017/?ssl=true');
      expect(result.database).toBeUndefined();
      expect(result.options.ssl).toBe(true);
    });

    it('should handle whitespace in connection string', () => {
      const result = parseConnectionString('  mongodb://localhost:27017/mydb  ');
      expect(result.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
      expect(result.database).toBe('mydb');
    });
  });

  describe('error handling', () => {
    it('should throw on empty string', () => {
      expect(() => parseConnectionString('')).toThrow(ConnectionStringParseError);
      expect(() => parseConnectionString('')).toThrow('non-empty string');
    });

    it('should throw on null', () => {
      expect(() => parseConnectionString(null as unknown as string)).toThrow(
        ConnectionStringParseError
      );
    });

    it('should throw on undefined', () => {
      expect(() => parseConnectionString(undefined as unknown as string)).toThrow(
        ConnectionStringParseError
      );
    });

    it('should throw on invalid scheme', () => {
      expect(() => parseConnectionString('http://localhost:27017')).toThrow(
        ConnectionStringParseError
      );
      expect(() => parseConnectionString('http://localhost:27017')).toThrow(
        'must start with mongodb://'
      );
    });

    it('should throw on missing host', () => {
      expect(() => parseConnectionString('mongodb://')).toThrow(ConnectionStringParseError);
      expect(() => parseConnectionString('mongodb://')).toThrow('missing host');
    });

    it('should throw on empty host after credentials', () => {
      expect(() => parseConnectionString('mongodb://user:pass@')).toThrow(
        ConnectionStringParseError
      );
    });

    it('should throw on invalid port', () => {
      expect(() => parseConnectionString('mongodb://localhost:abc')).toThrow(
        ConnectionStringParseError
      );
      expect(() => parseConnectionString('mongodb://localhost:abc')).toThrow('Invalid port');
    });

    it('should throw on port out of range', () => {
      expect(() => parseConnectionString('mongodb://localhost:0')).toThrow(
        ConnectionStringParseError
      );
      expect(() => parseConnectionString('mongodb://localhost:70000')).toThrow(
        ConnectionStringParseError
      );
    });

    it('should throw on invalid IPv6 format', () => {
      expect(() => parseConnectionString('mongodb://[::1')).toThrow(ConnectionStringParseError);
      expect(() => parseConnectionString('mongodb://[::1')).toThrow('Invalid IPv6');
    });

    it('should throw on mongodb+srv with multiple hosts', () => {
      expect(() =>
        parseConnectionString('mongodb+srv://host1.example.com,host2.example.com/mydb')
      ).toThrow(ConnectionStringParseError);
      expect(() =>
        parseConnectionString('mongodb+srv://host1.example.com,host2.example.com/mydb')
      ).toThrow('must have exactly one host');
    });

    it('ConnectionStringParseError should be instanceof Error', () => {
      try {
        parseConnectionString('invalid');
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(ConnectionStringParseError);
        expect((error as Error).name).toBe('ConnectionStringParseError');
      }
    });
  });
});

describe('buildConnectionString', () => {
  it('should build simple connection string', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      options: {},
    });
    expect(result).toBe('mongodb://localhost');
  });

  it('should build connection string with database', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      database: 'mydb',
      options: {},
    });
    expect(result).toBe('mongodb://localhost/mydb');
  });

  it('should build connection string with credentials', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      username: 'user',
      password: 'pass',
      hosts: [{ host: 'localhost', port: 27017 }],
      database: 'mydb',
      options: {},
    });
    expect(result).toBe('mongodb://user:pass@localhost/mydb');
  });

  it('should encode special characters in credentials', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      username: 'user@name',
      password: 'p@ss:word',
      hosts: [{ host: 'localhost', port: 27017 }],
      options: {},
    });
    expect(result).toBe('mongodb://user%40name:p%40ss%3Aword@localhost');
  });

  it('should build connection string with multiple hosts', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [
        { host: 'host1', port: 27017 },
        { host: 'host2', port: 27018 },
        { host: 'host3', port: 27019 },
      ],
      options: {},
    });
    expect(result).toBe('mongodb://host1,host2:27018,host3:27019');
  });

  it('should build connection string with non-default port', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27018 }],
      options: {},
    });
    expect(result).toBe('mongodb://localhost:27018');
  });

  it('should build connection string with options', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      database: 'mydb',
      options: {
        authSource: 'admin',
        ssl: true,
        retryWrites: true,
      },
    });
    expect(result).toBe('mongodb://localhost/mydb?authSource=admin&ssl=true&retryWrites=true');
  });

  it('should build connection string with options but no database', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      options: {
        ssl: true,
      },
    });
    expect(result).toBe('mongodb://localhost/?ssl=true');
  });

  it('should build mongodb+srv connection string', () => {
    const result = buildConnectionString({
      scheme: 'mongodb+srv',
      username: 'user',
      password: 'pass',
      hosts: [{ host: 'cluster0.mongodb.net', port: 27017 }],
      database: 'mydb',
      options: {
        retryWrites: true,
        w: 'majority',
      },
    });
    expect(result).toBe(
      'mongodb+srv://user:pass@cluster0.mongodb.net/mydb?retryWrites=true&w=majority'
    );
  });

  it('should build connection string with IPv6 host', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: '::1', port: 27017 }],
      options: {},
    });
    // IPv6 addresses always include port since they need brackets
    expect(result).toBe('mongodb://[::1]:27017');
  });

  it('should handle array options', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      options: {
        compressors: ['snappy', 'zlib'],
      },
    });
    expect(result).toBe('mongodb://localhost/?compressors=snappy%2Czlib');
  });

  it('should skip undefined option values', () => {
    const result = buildConnectionString({
      scheme: 'mongodb',
      hosts: [{ host: 'localhost', port: 27017 }],
      options: {
        ssl: true,
        authSource: undefined,
      },
    });
    expect(result).toBe('mongodb://localhost/?ssl=true');
    expect(result).not.toContain('authSource');
  });

  it('should round-trip parse and build', () => {
    const original = 'mongodb://user:pass@host1:27017,host2:27018/mydb?authSource=admin&ssl=true';
    const parsed = parseConnectionString(original);
    const rebuilt = buildConnectionString({
      scheme: parsed.scheme,
      username: parsed.username,
      password: parsed.password,
      hosts: parsed.hosts,
      database: parsed.database,
      options: parsed.options,
    });
    // Re-parse to verify equivalence (order may differ)
    const reparsed = parseConnectionString(rebuilt);
    expect(reparsed.scheme).toBe(parsed.scheme);
    expect(reparsed.username).toBe(parsed.username);
    expect(reparsed.password).toBe(parsed.password);
    expect(reparsed.hosts).toEqual(parsed.hosts);
    expect(reparsed.database).toBe(parsed.database);
    expect(reparsed.options).toEqual(parsed.options);
  });
});

describe('isConnectionString', () => {
  it('should return true for mongodb:// URI', () => {
    expect(isConnectionString('mongodb://localhost:27017')).toBe(true);
  });

  it('should return true for mongodb+srv:// URI', () => {
    expect(isConnectionString('mongodb+srv://cluster.mongodb.net')).toBe(true);
  });

  it('should return true with leading/trailing whitespace', () => {
    expect(isConnectionString('  mongodb://localhost:27017  ')).toBe(true);
  });

  it('should return false for http:// URI', () => {
    expect(isConnectionString('http://localhost:27017')).toBe(false);
  });

  it('should return false for empty string', () => {
    expect(isConnectionString('')).toBe(false);
  });

  it('should return false for null', () => {
    expect(isConnectionString(null as unknown as string)).toBe(false);
  });

  it('should return false for undefined', () => {
    expect(isConnectionString(undefined as unknown as string)).toBe(false);
  });

  it('should return false for non-string', () => {
    expect(isConnectionString(123 as unknown as string)).toBe(false);
    expect(isConnectionString({} as unknown as string)).toBe(false);
  });

  it('should return false for partial match', () => {
    expect(isConnectionString('mongodb')).toBe(false);
    expect(isConnectionString('mongodb://')).toBe(true); // This is still a valid start
    expect(isConnectionString('my mongodb://localhost')).toBe(false);
  });
});

describe('integration scenarios', () => {
  it('should handle common MongoDB Atlas connection string', () => {
    const uri =
      'mongodb+srv://myUser:myPassword123@cluster0.abc123.mongodb.net/myDatabase?retryWrites=true&w=majority&appName=myApp';
    const parsed = parseConnectionString(uri);

    expect(parsed.scheme).toBe('mongodb+srv');
    expect(parsed.username).toBe('myUser');
    expect(parsed.password).toBe('myPassword123');
    expect(parsed.hosts[0].host).toBe('cluster0.abc123.mongodb.net');
    expect(parsed.database).toBe('myDatabase');
    expect(parsed.options.retryWrites).toBe(true);
    expect(parsed.options.w).toBe('majority');
    expect(parsed.options.appName).toBe('myApp');
  });

  it('should handle local development connection string', () => {
    const uri = 'mongodb://localhost:27017/dev';
    const parsed = parseConnectionString(uri);

    expect(parsed.scheme).toBe('mongodb');
    expect(parsed.hosts).toEqual([{ host: 'localhost', port: 27017 }]);
    expect(parsed.database).toBe('dev');
    expect(parsed.username).toBeUndefined();
  });

  it('should handle replica set connection string', () => {
    const uri =
      'mongodb://mongo1:27017,mongo2:27017,mongo3:27017/production?replicaSet=rs0&readPreference=primaryPreferred';
    const parsed = parseConnectionString(uri);

    expect(parsed.hosts).toHaveLength(3);
    expect(parsed.options.replicaSet).toBe('rs0');
    expect(parsed.options.readPreference).toBe('primaryPreferred');
  });

  it('should handle authenticated connection with authSource', () => {
    const uri = 'mongodb://appuser:appsecret@dbhost:27017/appdb?authSource=admin';
    const parsed = parseConnectionString(uri);

    expect(parsed.username).toBe('appuser');
    expect(parsed.password).toBe('appsecret');
    expect(parsed.database).toBe('appdb');
    expect(parsed.options.authSource).toBe('admin');
  });
});
