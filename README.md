# DuckDB Iggy Extension

A high-performance DuckDB extension for reading message streams directly from [Iggy](https://iggy.rs/), a blazing-fast distributed message streaming platform.

> **⚠️ EXPERIMENTAL**: This extension is in early-stage development for Iggy v0.7.0. Use in development environments only.

## Features

- Read message streams from Iggy directly in SQL queries
- HTTP-based API (version-agnostic, no binary protocol issues)
- JWT authentication support
- Partition-based message consumption
- Full DuckDB integration

---

## Quick Start

### 1. Docker Setup (macOS / Apple Silicon)

Iggy v0.7.0 on macOS requires specific Docker flags to bypass `io_uring` and core-binding restrictions:

```bash
docker run -d --name iggy-test \
  --security-opt seccomp=unconfined \
  -e IGGY_IO_URING_ENABLED=false \
  -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
  -e IGGY_ROOT_USERNAME=iggy \
  -e IGGY_ROOT_PASSWORD=iggy \
  -p 3000:3000/tcp \
  -p 8090:8090/tcp \
  apache/iggy:0.7.0
```

### 2. Initialize Streams and Topics

First, authenticate to get a JWT token:

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username": "iggy", "password": "iggy"}' \
  | jq -r '.access_token.token')
```

Create a stream:

```bash
curl -X POST http://localhost:3000/streams \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"stream_id": 1, "name": "stream1"}'
```

**Note**: The response will show `"id":0` - this is the auto-generated stream ID. Use this ID (0) in subsequent API calls.

Create a topic (all fields are mandatory in v0.7.0):

```bash
curl -X POST http://localhost:3000/streams/0/topics \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic_id": 1,
    "name": "topic1",
    "partitions_count": 1,
    "compression_algorithm": "none",
    "message_expiry": 0,
    "max_topic_size": 0,
    "replication_factor": 1
  }'
```

**Note**: Again, the response will show `"id":0` for the topic. Use this in the messages endpoint.

### 3. Publish Test Messages

Messages must include the partition ID as a Base64-encoded 4-byte little-endian integer:
- Partition 0: `AAAAAA==`
- Partition 1: `AQAAAA==`

**Important**: Use the auto-generated stream and topic IDs (typically 0 for the first created) from the API responses:

```bash
curl -X POST http://localhost:3000/streams/0/topics/0/messages \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "partitioning": {
      "kind": "partition_id",
      "value": "AAAAAA=="
    },
    "messages": [
      {"payload": "SGVsbG8gMC43LjA="}
    ]
  }'
```

### 4. Build the Extension

```bash
make debug          # Development build
# or
make release        # Optimized build
```

### 5. Query in DuckDB

Load the extension and query messages:

```sql
LOAD './build/debug/extension/read_iggy/read_iggy.duckdb_extension';

SELECT 
  "offset", 
  CAST(payload AS VARCHAR) as message 
FROM read_iggy('0', '0', 0, 'iggy://iggy:iggy@127.0.0.1:8090') 
LIMIT 5;
```

Note: Use the auto-generated IDs (typically 0 for the first stream and topic created).

---

## API Reference

### Connection String Format

```
iggy://[username:password@]host[:port]
```

Examples:
- `iggy://iggy:iggy@127.0.0.1:8090` - with credentials
- `iggy://127.0.0.1:8090` - uses default credentials (iggy/iggy)
- `iggy://127.0.0.1` - uses default port 8090

**Note**: The extension automatically routes HTTP requests to port 3000 (HTTP API), regardless of the port specified in the connection string.

### Function Signature

```sql
read_iggy(
  stream_id VARCHAR,      -- Stream name or ID
  topic_id VARCHAR,       -- Topic name or ID  
  partition_id INTEGER,   -- Partition number
  connection STRING       -- Connection string (iggy://)
)
```

Returns two columns:
- `offset` BIGINT - Message offset in the partition
- `payload` BLOB - Message payload bytes

---

## Implementation Details

### HTTP vs Binary Protocol

This extension uses the **HTTP API** (port 3000) rather than the binary protocol (port 8090) to avoid version-specific compatibility issues:

- ✅ Version-agnostic JSON serialization
- ✅ Standard HTTP semantics
- ✅ JWT-based authentication
- ✅ Stable across Iggy releases

### Authentication Flow

1. POST credentials to `/users/login` → receives JWT token
2. Include token in `Authorization: Bearer {token}` header for all API requests
3. Response structure: `access_token.token` (nested)

### Message Polling

Messages are fetched via:
```
GET /api/streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}/messages
  ?offset={current_offset}&count=1024
```

The extension tracks offset internally and automatically fetches the next batch of messages.

---

## Development

### Building

```bash
make configure      # First time setup
make debug          # Development build
make release        # Production build
```

### Testing

```bash
make test_debug     # Run tests against debug build
make test_release   # Run tests against release build
```

### Changing DuckDB Versions

```bash
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

---

## Troubleshooting

### "401 Unauthorized"
- Verify credentials in connection string
- Check that Iggy HTTP server is listening on port 3000
- Ensure stream and topic exist

### "Partition not found"
- Verify partition ID matches the topic's partition count
- Use partition ID 0 for topics with 1 partition

### "Invalid command" (binary protocol)
This extension uses HTTP API, not the binary protocol. If you see binary protocol errors, the extension version may be mismatched with your Iggy server.

---

## License

Experimental / MIT

---

## Related Links

- [DuckDB Documentation](https://duckdb.org/)
- [Iggy Documentation](https://iggy.rs/docs)
- [DuckDB Rust Extension Template](https://github.com/duckdb/duckdb-rust-extension-template)