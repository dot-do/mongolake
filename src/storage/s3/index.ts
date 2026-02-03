/**
 * S3 Storage Re-export
 *
 * Provides a cleaner import path for S3 storage:
 * ```ts
 * import { S3Storage } from 'mongolake/storage/s3';
 * ```
 */

export { S3Storage } from '../s3.js';
export type { S3Config } from '../s3.js';
export {
  toHex,
  sha256Hex,
  hmacSha256,
  getSigningKey,
  formatAmzDate,
  formatDateStamp,
  awsUriEncode,
  normalizeHeaderValue,
} from '../s3.js';
