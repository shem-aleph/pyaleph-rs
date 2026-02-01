# API Reference

pyaleph-rs provides a REST API compatible with pyaleph v0.

## Base URL

```
http://localhost:8080/api/v0
```

## Authentication

Most endpoints are public. Message submission requires a valid signature in the message body.

---

## Messages

### List Messages

```http
GET /messages.json
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `page` | int | Page number (default: 1) |
| `pagination` | int | Items per page (default: 20, max: 1000) |
| `addresses` | string | Comma-separated addresses to filter |
| `channels` | string | Comma-separated channels to filter |
| `refs` | string | Comma-separated refs to filter |
| `hashes` | string | Comma-separated item hashes |
| `msgTypes` | string | Comma-separated message types (POST, AGGREGATE, STORE, PROGRAM, INSTANCE, FORGET) |
| `startDate` | float | Unix timestamp (seconds) |
| `endDate` | float | Unix timestamp (seconds) |
| `contentKeys` | string | JSON path filter for content |

**Response:**

```json
{
  "messages": [
    {
      "item_hash": "abc123...",
      "type": "POST",
      "chain": "ETH",
      "sender": "0x...",
      "channel": "TEST",
      "time": 1706745600.0,
      "content": { ... },
      "confirmations": [
        { "chain": "ETH", "height": 12345678, "hash": "0x..." }
      ]
    }
  ],
  "pagination_page": 1,
  "pagination_total": 100,
  "pagination_per_page": 20
}
```

### Get Message

```http
GET /messages/{hash}
```

### Get Message Content

```http
GET /messages/{hash}/content
```

Returns raw message content (for STORE messages, returns the file).

### Submit Message

```http
POST /messages
Content-Type: application/json
```

**Request Body:**

```json
{
  "message": {
    "type": "POST",
    "chain": "ETH",
    "sender": "0x...",
    "channel": "TEST",
    "time": 1706745600.0,
    "item_type": "inline",
    "item_content": "{\"body\": \"Hello\"}",
    "item_hash": "abc123..."
  },
  "signature": "0x..."
}
```

**Response:**

```json
{
  "status": "success",
  "item_hash": "abc123..."
}
```

---

## Aggregates

### Get Aggregates

```http
GET /aggregates/{address}.json
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `keys` | string | Comma-separated keys to filter |
| `limit` | int | Max number of keys (default: 100) |

**Response:**

```json
{
  "address": "0x...",
  "data": {
    "profile": { "name": "Alice", "bio": "..." },
    "settings": { "theme": "dark" }
  }
}
```

### Get Aggregate Keys

```http
GET /aggregates/{address}/keys.json
```

---

## Posts

### List Posts

```http
GET /posts.json
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `addresses` | string | Filter by sender addresses |
| `channels` | string | Filter by channels |
| `types` | string | Filter by post types |
| `refs` | string | Filter by refs |
| `tags` | string | Filter by tags |
| `hashes` | string | Filter by item hashes |
| `page` | int | Page number |
| `pagination` | int | Items per page |

---

## Programs

### List Programs

```http
GET /programs/{address}
```

### Get Program

```http
GET /programs/{address}/{hash}
```

---

## Instances

### List Instances

```http
GET /instances/{address}
```

### Get Instance

```http
GET /instances/{address}/{hash}
```

### Get Allocation

```http
GET /allocation/{hash}
```

Returns CRN allocation status for a VM.

---

## Balance

### Get Balance

```http
GET /balance/{address}
```

**Response:**

```json
{
  "address": "0x...",
  "balance": 1000.5,
  "locked": 50.0,
  "available": 950.5
}
```

---

## Pricing

### Get Pricing

```http
GET /pricing
```

**Response:**

```json
{
  "compute_units": {
    "vcpu_hour": 0.0001,
    "memory_gb_hour": 0.00005,
    "storage_gb_month": 0.001
  },
  "storage": {
    "message_base": 0.0001,
    "per_mb": 0.00001
  }
}
```

### Estimate Cost

```http
POST /cost/estimate
Content-Type: application/json
```

**Request:**

```json
{
  "type": "instance",
  "vcpus": 4,
  "memory": 8192,
  "storage": 100,
  "duration_hours": 720
}
```

---

## Bulk Operations

### Check Hashes

```http
POST /hashes
Content-Type: application/json
```

**Request:**

```json
{
  "hashes": ["abc123...", "def456..."]
}
```

**Response:**

```json
{
  "existing": ["abc123..."],
  "missing": ["def456..."]
}
```

---

## Statistics

### Network Stats

```http
GET /stats
```

### Address Stats

```http
GET /stats/{address}
```

---

## Internal Endpoints

### Health Check

```http
GET /health
```

### Prometheus Metrics

```http
GET /_internal/metrics
```

### Node Status

```http
GET /_internal/status
```

### Sync Status

```http
GET /_internal/sync
```

---

## WebSocket

### Connect

```
ws://localhost:8080/ws
```

### Subscribe

```json
{
  "type": "subscribe",
  "addresses": ["0x..."],
  "channels": ["TEST"],
  "message_types": ["POST", "AGGREGATE"],
  "hashes": []
}
```

### Receive Messages

```json
{
  "type": "message",
  "message": { ... }
}
```

### Update Subscription

```json
{
  "type": "update",
  "add_addresses": ["0x..."],
  "remove_channels": ["OLD"]
}
```

### Unsubscribe

```json
{
  "type": "unsubscribe"
}
```

---

## Error Responses

All errors follow this format:

```json
{
  "error": {
    "code": 400,
    "message": "Invalid request"
  }
}
```

| Code | Meaning |
|------|---------|
| 400 | Bad Request |
| 401 | Unauthorized |
| 404 | Not Found |
| 422 | Validation Error |
| 500 | Internal Server Error |
