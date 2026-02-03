# API Versioning Strategy

This document describes MongoLake's approach to API versioning, deprecation policies, and migration guidelines.

## Table of Contents

1. [Versioning Scheme](#versioning-scheme)
2. [Semantic Versioning](#semantic-versioning)
3. [API Stability Levels](#api-stability-levels)
4. [Deprecation Policy](#deprecation-policy)
5. [Breaking Change Policy](#breaking-change-policy)
6. [Migration Guides](#migration-guides)
7. [Version Support Timeline](#version-support-timeline)
8. [Release Process](#release-process)

---

## Versioning Scheme

MongoLake follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html) for all releases.

### Version Format

```
MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]

Examples:
  0.1.0        - Initial alpha release
  0.2.0        - New features in alpha
  1.0.0        - First stable release
  1.1.0        - New features, backwards compatible
  1.1.1        - Bug fixes only
  2.0.0        - Breaking changes
  2.0.0-alpha  - Pre-release
  2.0.0-rc.1   - Release candidate
```

### Version Components

| Component | Meaning | When to Increment |
|-----------|---------|-------------------|
| **MAJOR** | Breaking changes | Incompatible API changes |
| **MINOR** | New features | Backwards-compatible additions |
| **PATCH** | Bug fixes | Backwards-compatible fixes |
| **PRERELEASE** | Pre-release | Alpha, beta, or release candidate |

---

## Semantic Versioning

### What Constitutes a Breaking Change (MAJOR)

The following changes require a major version increment:

1. **Removing a public API**
   - Removing a method, class, or function
   - Removing a configuration option
   - Removing an export from the package

2. **Changing API signatures**
   - Changing required parameters
   - Changing return types
   - Changing the order of parameters
   - Making optional parameters required

3. **Changing behavior**
   - Changing the meaning of existing options
   - Changing default values in ways that affect existing code
   - Changing error types or error conditions

4. **Changing storage format**
   - Changes that make existing data unreadable
   - Changes that require data migration

### What Constitutes a Minor Change (MINOR)

1. **Adding new features**
   - New methods or classes
   - New optional parameters with default values
   - New configuration options
   - New exports

2. **Extending functionality**
   - Adding new query operators
   - Adding new aggregation stages
   - Adding new index types

3. **Performance improvements**
   - Optimizations that don't change behavior

### What Constitutes a Patch Change (PATCH)

1. **Bug fixes**
   - Fixing incorrect behavior
   - Fixing edge cases
   - Fixing memory leaks

2. **Documentation fixes**
   - Correcting documentation errors
   - Adding missing documentation

3. **Internal changes**
   - Refactoring without API changes
   - Dependency updates (non-breaking)

---

## API Stability Levels

MongoLake APIs are classified into stability levels:

### Stable

Stable APIs are production-ready and follow strict versioning:

- Documented in the API reference
- Covered by comprehensive tests
- Breaking changes only in major versions
- Deprecation warnings before removal

**Stable APIs include:**
- `MongoLake` class and all public methods
- `Database` class and all public methods
- `Collection` class CRUD operations
- `FindCursor` and `AggregationCursor` classes
- Query operators (`$eq`, `$gt`, etc.)
- Update operators (`$set`, `$inc`, etc.)
- Aggregation stages (`$match`, `$group`, etc.)
- Type definitions in `types.ts`

### Experimental

Experimental APIs may change without major version bumps:

- Marked with `@experimental` in documentation
- May change or be removed in minor versions
- User feedback is actively sought

**Experimental APIs include:**
- Branching operations (`BranchStore`, `BranchManager`, `MergeEngine`)
- Distributed aggregation (`DistributedAggregator`)
- Some Iceberg integration features

### Internal

Internal APIs are not part of the public contract:

- Marked with `@internal` in source code
- May change or be removed at any time
- Not documented in the API reference
- Direct use is discouraged

**Internal APIs include:**
- Buffer and WAL managers
- Parquet encoding internals
- Shard routing internals
- Any method/class marked `@internal`

---

## Deprecation Policy

### Deprecation Timeline

```
Version N: Feature announced as deprecated
           - Console warnings emitted
           - Documentation updated
           - Alternative provided

Version N+1 or N+2: Feature may be removed
           - Depends on significance of change
           - At least one minor version grace period
           - Major features get longer grace periods
```

### Deprecation Process

1. **Announcement**
   - Added to CHANGELOG.md under "Deprecation Schedule"
   - Runtime warning when deprecated feature is used
   - Documentation marked with deprecation notice

2. **Warning Period**
   - Minimum one minor version
   - Major features: minimum two minor versions
   - Console warnings indicate removal timeline

3. **Removal**
   - Only in a major version (except experimental APIs)
   - Migration guide provided
   - Final changelog entry

### Deprecation Notice Format

```typescript
/**
 * @deprecated Since 1.2.0. Use `newMethod()` instead.
 * Will be removed in 2.0.0.
 */
function oldMethod() {
  console.warn(
    '[mongolake] oldMethod() is deprecated. ' +
    'Use newMethod() instead. ' +
    'This method will be removed in version 2.0.0.'
  );
  // ... implementation
}
```

### Deprecation Categories

| Category | Min Warning Period | Removal Version |
|----------|-------------------|-----------------|
| Core APIs (Collection methods) | 2 minor versions | Next major |
| Secondary APIs (utilities) | 1 minor version | Next major |
| Experimental APIs | Immediate | Next minor |
| Configuration options | 1 minor version | Next major |
| CLI commands | 1 minor version | Next major |

---

## Breaking Change Policy

### Planning Breaking Changes

1. **Early Communication**
   - Announced in changelog for upcoming versions
   - Discussed in GitHub issues/discussions
   - Listed in release notes

2. **Deprecation First**
   - Deprecated in minor version before removal
   - Alternative provided before deprecation
   - Migration path documented

3. **Migration Support**
   - Codemods provided when practical
   - Migration guide for each breaking change
   - Example code for common patterns

### Breaking Change Categories

#### Category 1: High Impact
Changes affecting most users:
- Core CRUD method signatures
- Default behaviors
- Required dependencies

**Policy**: 6+ month deprecation period, detailed migration guide, automated codemods when possible.

#### Category 2: Medium Impact
Changes affecting some users:
- Secondary APIs
- Configuration options
- Optional features

**Policy**: 3+ month deprecation period, migration guide, example code.

#### Category 3: Low Impact
Changes affecting few users:
- Edge cases
- Rarely used options
- Internal optimizations

**Policy**: Standard deprecation (1 minor version), changelog entry.

---

## Migration Guides

### Guide Structure

Each migration guide includes:

1. **Summary of Changes**
   - List of breaking changes
   - List of deprecations
   - New features overview

2. **Step-by-Step Migration**
   - Ordered steps to update code
   - Code examples (before/after)
   - Common pitfalls

3. **Automated Tools**
   - Codemods (when available)
   - Linting rules for deprecated patterns

4. **Testing Recommendations**
   - What to test after migration
   - Known edge cases

### Example Migration Guide Format

```markdown
# Migrating from 1.x to 2.x

## Breaking Changes

### 1. Collection.find() now returns AsyncIterator

**Before (1.x):**
```typescript
const docs = await collection.find({}).toArray();
```

**After (2.x):**
```typescript
const cursor = collection.find({});
const docs = await cursor.toArray();
// Or use async iteration:
for await (const doc of collection.find({})) {
  console.log(doc);
}
```

### 2. ObjectId constructor changes

...
```

### Migration Guide Index

| From | To | Guide |
|------|-----|-------|
| MongoDB | 0.1.x | [MIGRATION_FROM_MONGODB.md](./MIGRATION_FROM_MONGODB.md) |

---

## Version Support Timeline

### Support Levels

| Level | Description | Duration |
|-------|-------------|----------|
| **Active** | Bug fixes, security patches, new features | Current version |
| **Maintenance** | Critical bug fixes, security patches only | Previous major version |
| **End of Life** | No updates | Older versions |

### Support Schedule

```
           Active      Maintenance     EOL
           ------      -----------     ---
v0.x       Current     -               -
v1.0       (Future)    v0.x moves here When v2.0 releases
v2.0       (Future)    v1.x moves here When v3.0 releases
```

### Current Support Status

| Version | Status | Support Until |
|---------|--------|---------------|
| 0.1.x | Active | Until 1.0 release |

### Security Patches

- Critical security issues: Patched in all supported versions
- Non-critical security: Patched in active version only
- CVE announcements: Published in GitHub Security Advisories

---

## Release Process

### Release Schedule

- **Patch releases**: As needed for bug fixes
- **Minor releases**: Monthly (approximately)
- **Major releases**: Annually or as needed for breaking changes

### Pre-release Versions

1. **Alpha** (`x.y.z-alpha.n`)
   - Early feature development
   - May be unstable
   - API may change significantly

2. **Beta** (`x.y.z-beta.n`)
   - Feature complete for release
   - API is stabilizing
   - Looking for user feedback

3. **Release Candidate** (`x.y.z-rc.n`)
   - Ready for release
   - API is frozen
   - Final testing phase

### Release Checklist

- [ ] All tests passing
- [ ] CHANGELOG.md updated
- [ ] Migration guide (if breaking changes)
- [ ] Documentation updated
- [ ] npm package published
- [ ] GitHub release created
- [ ] Announcement posted

---

## Package Exports

MongoLake uses explicit package exports for version control:

```json
{
  "exports": {
    ".": "./dist/client/index.js",
    "./worker": "./dist/worker/index.js",
    "./do": "./dist/do/index.js",
    "./storage": "./dist/storage/index.js",
    "./parquet": "./dist/parquet/index.js",
    "./mongoose": "./dist/mongoose/index.js",
    "./utils": "./dist/utils/index.js",
    "./wire-protocol": "./dist/wire-protocol/index.js",
    "./index": "./dist/index/index.js",
    "./storage/s3": "./dist/storage/s3/index.js",
    "./metrics": "./dist/metrics/index.js",
    "./transaction": "./dist/transaction/index.js",
    "./session": "./dist/session/index.js"
  }
}
```

### Export Versioning Rules

1. **Adding exports**: Minor version bump
2. **Removing exports**: Major version bump (with deprecation)
3. **Changing export paths**: Major version bump

---

## TypeScript Support

### Type Definition Versioning

- Types are included in the package (`dist/*.d.ts`)
- Type changes follow the same versioning rules as runtime APIs
- Breaking type changes require major version

### Supported TypeScript Versions

| MongoLake Version | TypeScript Version |
|-------------------|-------------------|
| 0.1.x | 5.0+ |

---

## Backwards Compatibility Guarantees

### What We Guarantee

For stable APIs within a major version:

1. **Method signatures won't change** (parameters, return types)
2. **Behavior won't change** (same inputs produce same outputs)
3. **Types remain compatible** (existing type usage continues to work)
4. **Storage format is readable** (can read data written by older versions)

### What We Don't Guarantee

1. **Performance characteristics** (may improve or change)
2. **Internal implementation details** (refactoring is allowed)
3. **Debug output format** (logs, error messages may change)
4. **Experimental features** (may change in minor versions)

---

## Getting Help

- **Questions**: [GitHub Discussions](https://github.com/dot-do/mongolake/discussions)
- **Bugs**: [GitHub Issues](https://github.com/dot-do/mongolake/issues)
- **Security**: security@mongolake.com

---

## Related Documentation

- [CHANGELOG.md](./CHANGELOG.md) - Version history and breaking changes
- [MIGRATION_FROM_MONGODB.md](./MIGRATION_FROM_MONGODB.md) - MongoDB migration guide
- [API Reference](./api/client.md) - Detailed API documentation
- [Architecture](./ARCHITECTURE.md) - System design and internals
