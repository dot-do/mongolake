# Querying MongoLake with SQL Engines

MongoLake stores data in Apache Iceberg format, making it queryable with any SQL engine that supports Iceberg tables. This guide covers configuration and usage for popular query engines.

## Overview

MongoLake exposes MongoDB collections as Iceberg tables through a REST catalog API. This enables:

- **SQL Analytics**: Query your MongoDB data with familiar SQL syntax
- **Time Travel**: Query historical versions of your data
- **Cross-Engine Compatibility**: Use the same data with Trino, Spark, Presto, DuckDB, and more
- **Data Lake Integration**: Join MongoDB data with other data lake tables

## Catalog Configuration

MongoLake provides two catalog interfaces:

1. **REST Catalog** (Iceberg REST spec) - Standard Iceberg REST API
2. **R2 Data Catalog** - Native Cloudflare R2 integration

### REST Catalog Connection

The REST catalog follows the Apache Iceberg REST Catalog specification.

| Property | Description | Example |
|----------|-------------|---------|
| `uri` | REST catalog endpoint | `https://your-mongolake.example.com/api/v1` |
| `warehouse` | S3/R2 warehouse location | `s3://bucket/warehouse` |
| `token` | Bearer authentication token | `your-api-token` |

## Trino Configuration

### Catalog Properties

Create a catalog properties file (e.g., `mongolake.properties`):

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://your-mongolake.example.com/api/v1
iceberg.rest-catalog.warehouse=s3://your-bucket/warehouse

# Authentication (choose one)
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=client_id:client_secret

# Or use bearer token
# iceberg.rest-catalog.security=OAUTH2
# iceberg.rest-catalog.oauth2.token=your-token

# S3 configuration
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
hive.s3.endpoint=https://YOUR_ACCOUNT.r2.cloudflarestorage.com
hive.s3.path-style-access=true
```

### Querying Data

```sql
-- List all databases (namespaces)
SHOW SCHEMAS FROM mongolake;

-- List collections in a database
SHOW TABLES FROM mongolake.mydb;

-- Query a collection
SELECT * FROM mongolake.mydb.users
WHERE created_at > DATE '2024-01-01'
LIMIT 100;

-- Time travel query (by snapshot ID)
SELECT * FROM mongolake.mydb.users FOR VERSION AS OF 12345678901234;

-- Time travel query (by timestamp)
SELECT * FROM mongolake.mydb.users FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:00:00';

-- Query table history
SELECT * FROM mongolake.mydb."users$snapshots";

-- Query partition metadata
SELECT * FROM mongolake.mydb."users$partitions";

-- Query file metadata
SELECT * FROM mongolake.mydb."users$files";
```

### Advanced Trino Configuration

For production deployments:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://your-mongolake.example.com/api/v1
iceberg.rest-catalog.warehouse=s3://your-bucket/warehouse

# Performance tuning
iceberg.max-partitions-per-writer=100
iceberg.target-max-file-size=128MB
iceberg.compression-codec=ZSTD

# Metadata caching
iceberg.metadata-cache-ttl=5m
iceberg.manifest-caching-enabled=true

# S3 settings for Cloudflare R2
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
hive.s3.endpoint=https://${ENV:R2_ACCOUNT_ID}.r2.cloudflarestorage.com
hive.s3.path-style-access=true
hive.s3.ssl.enabled=true
```

## Apache Spark Configuration

### Spark Session Setup

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoLake Analytics") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.mongolake", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mongolake.type", "rest") \
    .config("spark.sql.catalog.mongolake.uri", "https://your-mongolake.example.com/api/v1") \
    .config("spark.sql.catalog.mongolake.warehouse", "s3://your-bucket/warehouse") \
    .config("spark.sql.catalog.mongolake.token", "your-api-token") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://YOUR_ACCOUNT.r2.cloudflarestorage.com") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
```

### Spark SQL Queries

```python
# List databases
spark.sql("SHOW DATABASES IN mongolake").show()

# List tables
spark.sql("SHOW TABLES IN mongolake.mydb").show()

# Query data
df = spark.sql("""
    SELECT _id, name, email, created_at
    FROM mongolake.mydb.users
    WHERE created_at >= '2024-01-01'
""")
df.show()

# Time travel by snapshot
df_historical = spark.sql("""
    SELECT * FROM mongolake.mydb.users
    VERSION AS OF 12345678901234
""")

# Time travel by timestamp
df_at_time = spark.sql("""
    SELECT * FROM mongolake.mydb.users
    TIMESTAMP AS OF '2024-01-15 10:00:00'
""")

# View snapshot history
spark.sql("SELECT * FROM mongolake.mydb.users.snapshots").show()

# View table history
spark.sql("SELECT * FROM mongolake.mydb.users.history").show()
```

### Spark DataFrame API

```python
# Read current data
df = spark.read.format("iceberg").load("mongolake.mydb.users")

# Read at specific snapshot
df = spark.read.format("iceberg") \
    .option("snapshot-id", 12345678901234) \
    .load("mongolake.mydb.users")

# Read at specific timestamp
df = spark.read.format("iceberg") \
    .option("as-of-timestamp", "2024-01-15T10:00:00.000Z") \
    .load("mongolake.mydb.users")

# Incremental read (changes between snapshots)
df = spark.read.format("iceberg") \
    .option("start-snapshot-id", 12345678901234) \
    .option("end-snapshot-id", 12345678901235) \
    .load("mongolake.mydb.users")
```

## Presto Configuration

### Catalog Configuration

Create `mongolake.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://your-mongolake.example.com/api/v1
iceberg.rest-catalog.warehouse=s3://your-bucket/warehouse

# S3/R2 configuration
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
hive.s3.endpoint=https://YOUR_ACCOUNT.r2.cloudflarestorage.com
hive.s3.path-style-access=true
```

### Presto Queries

```sql
-- List schemas (databases)
SHOW SCHEMAS IN mongolake;

-- List tables (collections)
SHOW TABLES IN mongolake.mydb;

-- Query with filters
SELECT * FROM mongolake.mydb.orders
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'
  AND status = 'completed'
ORDER BY total DESC
LIMIT 100;

-- Aggregations
SELECT
    date_trunc('month', order_date) as month,
    count(*) as order_count,
    sum(total) as revenue
FROM mongolake.mydb.orders
GROUP BY 1
ORDER BY 1;

-- Join MongoDB collections
SELECT
    u.name,
    u.email,
    count(o._id) as order_count,
    sum(o.total) as total_spent
FROM mongolake.mydb.users u
LEFT JOIN mongolake.mydb.orders o ON u._id = o.user_id
GROUP BY u.name, u.email
ORDER BY total_spent DESC;
```

## DuckDB Configuration

DuckDB can query Iceberg tables directly from S3/R2:

```sql
-- Install and load extensions
INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

-- Configure S3/R2 credentials
SET s3_endpoint='YOUR_ACCOUNT.r2.cloudflarestorage.com';
SET s3_access_key_id='YOUR_ACCESS_KEY';
SET s3_secret_access_key='YOUR_SECRET_KEY';
SET s3_url_style='path';

-- Query Iceberg table directly from metadata path
SELECT * FROM iceberg_scan('s3://your-bucket/warehouse/mongolake/mydb/users/metadata/v1.metadata.json');

-- Or with the REST catalog (requires extension support)
-- ATTACH 'mongodb_lake' AS mongodb (TYPE iceberg, URL 'https://your-mongolake.example.com/api/v1');
```

## Schema Mapping

MongoLake maps MongoDB BSON types to Iceberg types:

| MongoDB Type | Iceberg Type | Notes |
|--------------|--------------|-------|
| `ObjectId` | `string` | 24-character hex string |
| `String` | `string` | UTF-8 encoded |
| `Int32` | `int` | 32-bit signed integer |
| `Int64` / `Long` | `long` | 64-bit signed integer |
| `Double` | `double` | 64-bit IEEE 754 |
| `Decimal128` | `decimal(38,18)` | High-precision decimal |
| `Boolean` | `boolean` | true/false |
| `Date` | `timestamp` | Millisecond precision |
| `Timestamp` | `timestamp` | MongoDB timestamp |
| `Binary` | `binary` | Raw bytes |
| `Array` | `list` | Typed array |
| `Object` | `struct` | Nested document |
| `Null` | nullable field | NULL value |

### Nested Document Example

MongoDB document:
```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John Doe",
  "address": {
    "street": "123 Main St",
    "city": "Anytown",
    "zip": "12345"
  },
  "tags": ["premium", "active"]
}
```

Iceberg schema:
```
_id: string (required)
name: string
address: struct<
  street: string,
  city: string,
  zip: string
>
tags: list<string>
```

SQL query:
```sql
SELECT
    _id,
    name,
    address.city as city,
    address.zip as zip_code
FROM mongolake.mydb.users
WHERE address.city = 'Anytown';
```

## Performance Tips

### Partitioning

MongoLake automatically partitions data based on common patterns:

```sql
-- Check partition structure
SELECT * FROM mongolake.mydb."orders$partitions";

-- Query specific partitions efficiently
SELECT * FROM mongolake.mydb.orders
WHERE order_date >= DATE '2024-01-01'
  AND order_date < DATE '2024-02-01';
```

### Predicate Pushdown

Iceberg supports predicate pushdown for efficient filtering:

```sql
-- These predicates are pushed down to file skipping
SELECT * FROM mongolake.mydb.users
WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'
  AND status = 'active';
```

### Column Pruning

Select only needed columns:

```sql
-- Efficient: reads only required columns
SELECT _id, name, email FROM mongolake.mydb.users;

-- Less efficient: reads all columns
SELECT * FROM mongolake.mydb.users;
```

### Time Travel Performance

- Recent snapshots are faster to query (fewer files)
- Consider snapshot retention policies for old data
- Use snapshot IDs when known (faster than timestamp lookup)

## Troubleshooting

### Common Issues

**Authentication Errors**
```
Error: 401 Unauthorized
```
- Verify your API token is valid
- Check token expiration
- Ensure correct OAuth2 credentials

**Table Not Found**
```
Error: Table 'mongolake.mydb.users' not found
```
- Verify the database and collection names
- Check namespace structure: `mongolake.<database>.<collection>`
- Ensure the table has been synced to the catalog

**S3/R2 Access Denied**
```
Error: Access Denied
```
- Verify S3/R2 credentials
- Check bucket permissions
- Ensure path-style access is enabled for R2

**Schema Mismatch**
```
Error: Column 'field' not found in schema
```
- MongoDB documents may have varying schemas
- Use `COALESCE` for optional fields
- Check if schema evolution has occurred

### Debug Queries

```sql
-- Check table metadata
DESCRIBE mongolake.mydb.users;

-- View table properties
SHOW CREATE TABLE mongolake.mydb.users;

-- Check snapshot history
SELECT * FROM mongolake.mydb."users$snapshots" ORDER BY committed_at DESC;

-- View manifest files
SELECT * FROM mongolake.mydb."users$manifests";

-- Check data files
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes
FROM mongolake.mydb."users$files";
```

## Security Considerations

1. **Network Security**: Use HTTPS for all catalog communications
2. **Authentication**: Use OAuth2 or bearer tokens, never embed credentials in queries
3. **Authorization**: Configure role-based access in your query engine
4. **Data Encryption**: Enable S3/R2 server-side encryption
5. **Audit Logging**: Enable query logging for compliance

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/spec/#rest-catalog)
