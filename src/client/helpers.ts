/**
 * MongoLake Client Helpers
 *
 * Shared helper functions used across client modules.
 */

/**
 * Extract document ID as a string
 * Handles both ObjectId (objects with toString()) and primitive IDs
 */
export function extractDocumentId(doc: { _id?: unknown }): string {
  if (doc._id === undefined) {
    throw new Error('Document must have _id field');
  }
  return typeof doc._id === 'object' && doc._id !== null
    ? doc._id.toString()
    : String(doc._id);
}
