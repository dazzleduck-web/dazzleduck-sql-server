# 🛰️ Apache Arrow Flight Logger
**High-performance distributed logging using SLF4J + Apache Arrow + Arrow Flight RPC**
# Initializing a logger using the class name as its identifier.
- ArrowSimpleLogger logger = new ArrowSimpleLogger(MyClass.class.getName());
### `io.dazzleduck.sql.logger.server`
Contains:
- `SimpleFlightLogServer` 

---

# 🧬 Log Schema 

| Field            | Type |
|------------------|------|
| timestamp        | UTF8 |
| level            | UTF8 |
| logger           | UTF8 |
| thread           | UTF8 |
| message          | UTF8 |
| applicationId    | UTF8 |
| applicationName  | UTF8 |
| host             | UTF8 |

*Do not modify field names without updating both client & server.*

---

# ⚙️ How It Works

### 1. Log event occurs
SLF4J forwards the call to `ArrowSimpleLogger`.

### 2. Logger builds a JavaRow
Contains metadata + message.

### 3. Logs buffer into batches of 10
(batch size can be changed)

### 4. Batch serialization
Batches are written as Arrow IPC using:
- `VectorSchemaRoot`
- `ArrowStreamWriter`

### 5. `AsyncArrowFlightSender.enqueue()`
Places serialized bytes into a thread-safe queue.

### 6. Background worker thread
Merges many Arrow IPC batches → sends them as one Flight PUT.

### 7. `SimpleFlightLogServer`
Accepts Arrow stream, extracts vectors, stores logs.