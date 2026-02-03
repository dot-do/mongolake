# MongoLake Tests

## Structure

```
tests/
├── unit/          # Unit tests - isolated component testing
├── integration/   # Integration tests - components working together
└── e2e/           # E2E tests - full system against deployed workers
```

## Running Tests

```bash
# Unit + Integration tests (local)
npm test

# E2E tests (against deployed worker)
MONGOLAKE_E2E_URL=https://mongolake.workers.dev npm run test:e2e
```

## Test Categories

### Unit Tests (`tests/unit/`)
- Test individual functions and classes in isolation
- Mock external dependencies
- Fast, run on every commit

### Integration Tests (`tests/integration/`)
- Test components working together
- Use in-memory storage
- Test client → storage → parquet flow

### E2E Tests (`tests/e2e/`)
- Test against real deployed workers
- Verify wire protocol compatibility
- Test with mongosh and Compass
- Require `MONGOLAKE_E2E_URL` environment variable
