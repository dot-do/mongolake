# Storage Backend Reference

MongoLake supports multiple storage backends through a unified interface. This document covers the storage abstraction layer and available implementations.

## Table of Contents

- [StorageBackend Interface](#storagebackend-interface)
- [Multipart Upload Interface](#multipart-upload-interface)
- [Storage Implementations](#storage-implementations)
  - [FileSystemStorage](#filesystemstorage)
  - [R2Storage](#r2storage)
  - [S3Storage](#s3storage)
  - [MemoryStorage](#memorystorage)
- [Factory Function](#factory-function)
- [Helper Functions](#helper-functions)

---

## StorageBackend Interface

The core interface that all storage backends must implement.

```typescript
interface StorageBackend {
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, data: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(key: string): Promise<boolean>;
  head(key: string): Promise<{ size: number } | null>;
  createMultipartUpload(key: string): Promise<MultipartUpload>;
}
```

### Methods

#### `get(key: string): Promise<Uint8Array | null>`

Retrieve an object by its key.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The object key (path) |

**Returns:** Promise resolving to the object data as `Uint8Array`, or `null` if not found

**Example:**

```typescript
const data = await storage.get('mydb/users/_manifest.json');
if (data) {
  const manifest = JSON.parse(new TextDecoder().decode(data));
}
```

---

#### `put(key: string, data: Uint8Array): Promise<void>`

Store an object.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The object key (path) |
| `data` | `Uint8Array` | The data to store |

**Returns:** Promise that resolves when the object is stored

**Error Conditions:**

- Throws if the write fails (e.g., permission denied, storage full)

**Example:**

```typescript
const data = new TextEncoder().encode(JSON.stringify({ version: 1 }));
await storage.put('mydb/config.json', data);
```

---

#### `delete(key: string): Promise<void>`

Delete an object.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The object key to delete |

**Returns:** Promise that resolves when the object is deleted

**Note:** Does not throw if the object doesn't exist (idempotent)

**Example:**

```typescript
await storage.delete('mydb/old-data.parquet');
```

---

#### `list(prefix: string): Promise<string[]>`

List all objects with a given prefix.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | `string` | The prefix to filter objects |

**Returns:** Promise resolving to an array of object keys

**Example:**

```typescript
const files = await storage.list('mydb/users/');
// ['mydb/users/_manifest.json', 'mydb/users_1234.parquet', ...]
```

---

#### `exists(key: string): Promise<boolean>`

Check if an object exists.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The object key to check |

**Returns:** Promise resolving to `true` if the object exists, `false` otherwise

**Example:**

```typescript
if (await storage.exists('mydb/users/_manifest.json')) {
  // Collection has been initialized
}
```

---

#### `head(key: string): Promise<{ size: number } | null>`

Get object metadata without retrieving the content.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The object key |

**Returns:** Promise resolving to metadata object with `size`, or `null` if not found

**Example:**

```typescript
const meta = await storage.head('mydb/large-file.parquet');
if (meta) {
  console.log(`File size: ${meta.size} bytes`);
}
```

---

#### `createMultipartUpload(key: string): Promise<MultipartUpload>`

Create a multipart upload for large files.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `string` | The destination object key |

**Returns:** Promise resolving to a `MultipartUpload` handle

**Example:**

```typescript
const upload = await storage.createMultipartUpload('mydb/large-export.parquet');
const parts = [];

for (let i = 0; i < chunks.length; i++) {
  const part = await upload.uploadPart(i + 1, chunks[i]);
  parts.push(part);
}

await upload.complete(parts);
```

---

## Multipart Upload Interface

Interface for handling large file uploads in parts.

```typescript
interface MultipartUpload {
  uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart>;
  complete(parts: UploadedPart[]): Promise<void>;
  abort(): Promise<void>;
}

interface UploadedPart {
  partNumber: number;
  etag: string;
}
```

### Methods

#### `uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart>`

Upload a single part of the multipart upload.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `partNumber` | `number` | Part number (1-indexed, must be unique) |
| `data` | `Uint8Array` | Part data |

**Returns:** Promise resolving to `UploadedPart` with part number and etag

**Note:** Part numbers don't need to be sequential, but must be unique

---

#### `complete(parts: UploadedPart[]): Promise<void>`

Complete the multipart upload by combining all parts.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `parts` | `UploadedPart[]` | Array of all uploaded parts |

**Returns:** Promise that resolves when the upload is complete

**Note:** Parts are combined in order of part number

---

#### `abort(): Promise<void>`

Abort the multipart upload and cleanup any uploaded parts.

**Returns:** Promise that resolves when the upload is aborted

---

## Storage Implementations

### FileSystemStorage

Local filesystem storage for development and testing.

```typescript
import { FileSystemStorage } from 'mongolake/storage';

const storage = new FileSystemStorage('.mongolake');
```

#### Constructor

```typescript
new FileSystemStorage(basePath: string)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `basePath` | `string` | Base directory for storage |

#### Behavior

- Creates directories automatically when needed
- Returns `null` for `get()` if file doesn't exist (ENOENT)
- Recursively lists all files in subdirectories
- Uses lazy Node.js module loading for Cloudflare Workers compatibility

**Example:**

```typescript
const storage = new FileSystemStorage('./data');

// Stores at ./data/mydb/users.parquet
await storage.put('mydb/users.parquet', data);

// Lists all files in ./data/mydb/
const files = await storage.list('mydb/');
```

---

### R2Storage

Cloudflare R2 storage for production deployments.

```typescript
import { R2Storage } from 'mongolake/storage';

const storage = new R2Storage(env.MY_BUCKET);
```

#### Constructor

```typescript
new R2Storage(bucket: R2Bucket)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket` | `R2Bucket` | Cloudflare R2 bucket binding |

#### Behavior

- Uses native R2 multipart upload API
- Handles pagination for listing large numbers of objects
- Returns `null` for non-existent objects

**Example (wrangler.toml):**

```toml
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-mongolake-data"
```

**Example (Worker):**

```typescript
export default {
  async fetch(request: Request, env: Env) {
    const storage = new R2Storage(env.DATA_BUCKET);
    const lake = new MongoLake({ bucket: env.DATA_BUCKET });
    // ...
  }
};
```

---

### S3Storage

S3-compatible storage for AWS, MinIO, and other providers.

```typescript
import { S3Storage, S3Config } from 'mongolake/storage';

const storage = new S3Storage({
  endpoint: 'https://s3.amazonaws.com',
  accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
  secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
  bucket: 'my-bucket',
  region: 'us-east-1',
});
```

#### Constructor

```typescript
new S3Storage(config: S3Config)
```

#### S3Config

```typescript
interface S3Config {
  endpoint: string;           // S3 endpoint URL
  accessKeyId: string;        // AWS access key ID
  secretAccessKey: string;    // AWS secret access key
  bucket: string;             // Bucket name
  region?: string;            // AWS region (default: 'auto')
}
```

#### Behavior

- Uses AWS Signature Version 4 for authentication
- Properly hashes request payload for security
- Parses XML responses for list operations
- Simplified multipart upload (concatenates parts locally)

**Example (AWS S3):**

```typescript
const storage = new S3Storage({
  endpoint: 'https://s3.us-west-2.amazonaws.com',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  bucket: 'my-data-lake',
  region: 'us-west-2',
});
```

**Example (MinIO):**

```typescript
const storage = new S3Storage({
  endpoint: 'http://localhost:9000',
  accessKeyId: 'minioadmin',
  secretAccessKey: 'minioadmin',
  bucket: 'mongolake',
});
```

**Example (Backblaze B2):**

```typescript
const storage = new S3Storage({
  endpoint: 'https://s3.us-west-002.backblazeb2.com',
  accessKeyId: process.env.B2_KEY_ID!,
  secretAccessKey: process.env.B2_APPLICATION_KEY!,
  bucket: 'my-b2-bucket',
});
```

---

### MemoryStorage

In-memory storage for testing.

```typescript
import { MemoryStorage } from 'mongolake/storage';

const storage = new MemoryStorage();
```

#### Constructor

```typescript
new MemoryStorage()
```

#### Additional Methods

##### `clear(): void`

Clear all stored data. Useful for test cleanup.

```typescript
afterEach(() => {
  storage.clear();
});
```

#### Behavior

- All data stored in a `Map<string, Uint8Array>`
- Data is lost when the process exits
- Fast for unit tests

**Example:**

```typescript
import { MemoryStorage } from 'mongolake/storage';

describe('MyFeature', () => {
  const storage = new MemoryStorage();

  beforeEach(() => {
    storage.clear();
  });

  it('should store and retrieve data', async () => {
    await storage.put('test/data.json', new TextEncoder().encode('{}'));
    const data = await storage.get('test/data.json');
    expect(data).not.toBeNull();
  });
});
```

---

## Factory Function

### `createStorage(config: MongoLakeConfig): StorageBackend`

Factory function that creates the appropriate storage backend based on configuration.

```typescript
import { createStorage } from 'mongolake/storage';
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | `MongoLakeConfig` | MongoLake configuration object |

**Returns:** Appropriate `StorageBackend` implementation

**Selection Logic:**

1. If `config.local` is set: Returns `FileSystemStorage`
2. If `config.bucket` is set: Returns `R2Storage`
3. If `config.endpoint` is set: Returns `S3Storage`
4. Default: Returns `FileSystemStorage` with `.mongolake` path

**Example:**

```typescript
import { createStorage } from 'mongolake/storage';

// Auto-selects based on config
const storage = createStorage({
  local: './data',
});

// Or for R2
const storage = createStorage({
  bucket: env.R2_BUCKET,
});
```

---

## Helper Functions

### `concatenateParts(parts: Uint8Array[]): Uint8Array`

Concatenates multiple `Uint8Array` parts into a single array.

```typescript
import { concatenateParts } from 'mongolake/storage';

const combined = concatenateParts([part1, part2, part3]);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `parts` | `Uint8Array[]` | Array of parts to concatenate |

**Returns:** Single `Uint8Array` containing all parts in order

**Example:**

```typescript
const part1 = new Uint8Array([1, 2, 3]);
const part2 = new Uint8Array([4, 5, 6]);
const combined = concatenateParts([part1, part2]);
// Uint8Array([1, 2, 3, 4, 5, 6])
```

---

## Error Conditions

### Common Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `ENOENT` | File/directory not found | Check path, ensure parent directories exist |
| `EACCES` | Permission denied | Check file permissions |
| `S3 GET failed: 403` | S3 authentication error | Verify credentials and bucket permissions |
| `S3 PUT failed: 403` | S3 write permission denied | Check IAM policy allows `s3:PutObject` |

### S3 Authentication Errors

If you receive 403 errors with S3Storage:

1. Verify credentials are correct
2. Check bucket policy allows access from your IP/VPC
3. Ensure IAM user/role has required permissions:
   - `s3:GetObject`
   - `s3:PutObject`
   - `s3:DeleteObject`
   - `s3:ListBucket`

---

## Implementing Custom Storage

You can implement your own storage backend by implementing the `StorageBackend` interface:

```typescript
import { StorageBackend, MultipartUpload, UploadedPart } from 'mongolake/storage';

class MyCustomStorage implements StorageBackend {
  async get(key: string): Promise<Uint8Array | null> {
    // Your implementation
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    // Your implementation
  }

  async delete(key: string): Promise<void> {
    // Your implementation
  }

  async list(prefix: string): Promise<string[]> {
    // Your implementation
  }

  async exists(key: string): Promise<boolean> {
    // Your implementation
  }

  async head(key: string): Promise<{ size: number } | null> {
    // Your implementation
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    // Your implementation
  }
}
```

---

## See Also

- [Client Reference](./client.md) - Client API documentation
- [Types Reference](./types.md) - TypeScript types and interfaces
- [Worker Reference](./worker.md) - Worker and Durable Object exports
