# Pulsar Batch Redelivery Replication

Replication setup for Apache Pulsar issue [#5969](https://github.com/apache/pulsar/issues/5969) — Acked messages redelivered when others are negatively acked.

## Issue

**Root Cause:** Cursor tracks at batch level, not message level.

When `batchIndexAckEnabled` is disabled (default before Pulsar 4.1.0) and `acknowledgmentAtBatchIndexLevelEnabled=false` on the broker, the broker treats batches as atomic units. If any message in a batch is negatively acknowledged, **all messages in the batch are redelivered**, including those that were already successfully acknowledged.

## Architecture

This replication uses a **producer-consumer** architecture with multiple PM2 workers to simulate a production environment:

- **Producer** (`producer.js`): Sends batched messages to Pulsar
- **Workers** (`worker.js`): Multiple PM2 instances consuming from a Shared subscription
- **PM2 Config** (`pm2.config.cjs`): Manages 2 worker instances

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Start Pulsar 2.6.4 (Bug Present)

This replication includes a Docker Compose configuration for Pulsar 2.6.4, where the batch index acknowledgment bug is present:

```bash
npm run pulsar:up
```

This starts:
- Pulsar 2.6.4 broker with `acknowledgmentAtBatchIndexLevelEnabled=false`
- ZooKeeper
- BookKeeper

**Why Pulsar 2.6.4?**
- Released December 2020, shortly after batch index ack feature was added
- Bug from issues #5969 and #23436 is reproducible
- Batch index acknowledgment is disabled by default

**Alternative: Use your own Pulsar**
If you have Pulsar running elsewhere, set the `PULSAR_URL` environment variable:
```bash
export PULSAR_URL=pulsar://your-pulsar-host:6650
```

### 3. Start Workers

```bash
npm start
# Or: pm2 start pm2.config.cjs
```

This starts 2 worker instances:
- `pulsar-worker-1`
- `pulsar-worker-2`

### 4. Watch Worker Logs

```bash
npm run logs
# Or: pm2 logs
```

### 5. Send Messages

In a separate terminal:

```bash
npm run producer
# Or: node producer.js
```

## Expected Behavior

### With `acknowledgmentAtBatchIndexLevelEnabled=false` (Issue Reproduction)

1. Producer sends 5 messages in a batch (msg-1 through msg-5)
2. **Worker 1** receives the batch
3. **Worker 1** acknowledges messages 1, 2, 4, 5
4. **Worker 1** negatively acknowledges message 3
5. After ~60 seconds (negative ack redelivery delay), **all 5 messages are redelivered**
6. **Worker 2** (or Worker 1) receives all 5 messages again
7. **Duplicate detection** logs show: `⚠️  DUPLICATE DETECTED: msg-1, msg-2, msg-4, msg-5`

**Key Point:** Even though messages 1, 2, 4, 5 were successfully acknowledged, they are redelivered because the broker tracks acknowledgments at the batch level.

### With `acknowledgmentAtBatchIndexLevelEnabled=true` (Fix)

Only message 3 would be redelivered. No duplicates.

## Worker Behavior

Each worker:
- Continuously polls for messages using `batchReceive()`
- Processes messages with this pattern:
  - **ACK**: msg-1, msg-2, msg-4, msg-5
  - **NACK**: msg-3
- Tracks all processed messages to detect duplicates
- Logs duplicate detection with timestamps and worker IDs

## Commands

### Pulsar Management
```bash
# Start Pulsar 2.6.4 (with bug)
npm run pulsar:up

# Stop Pulsar
npm run pulsar:down

# View Pulsar broker logs
npm run pulsar:logs
```

### Worker Management
```bash
# Start workers
npm start

# View logs
npm run logs

# Check worker status
npm run status

# Restart workers
npm restart

# Stop workers
npm stop

# Delete workers from PM2
npm run delete
```

### Testing
```bash
# Send messages
npm run producer
```

## Configuration

Edit `pm2.config.cjs` to change:
- Number of workers (add more app entries)
- Batch size
- Timeouts
- Enable/disable `BATCH_INDEX_ACK_ENABLED`

## Observing the Issue

### Full Replication Steps

1. **Start Pulsar 2.6.4**:
   ```bash
   npm run pulsar:up
   ```

2. **Start workers** (automatically waits for Pulsar to be ready):
   ```bash
   npm start
   ```
   This will:
   - Wait for Pulsar to be ready (up to 60 seconds)
   - Start 2 PM2 worker instances

3. **Watch logs**:
   ```bash
   npm run logs
   ```

4. **Send messages** (in another terminal):
   ```bash
   npm run producer
   ```

5. **Observe**:
   - Worker 1 receives batch of 5 messages
   - Worker 1 acks messages 1, 2, 4, 5
   - Worker 1 nacks message 3
   - Wait ~60 seconds (negative ack redelivery delay)
   - Worker 2 (or Worker 1) receives **all 5 messages again**
   - See duplicate warnings in logs:
     ```
     ⚠️  DUPLICATE DETECTED: msg-1
         First processed at: 2026-03-11T...
         First processed by: Worker 1
         Redelivered after: 62450ms
     ```

6. **Stop everything**:
   ```bash
   npm stop
   npm run delete
   npm run pulsar:down
   ```

## Fix

Enable batch index acknowledgment on **both** client and broker:

### Client (worker.js)

Set environment variable in `pm2.config.cjs`:
```javascript
env: {
  BATCH_INDEX_ACK_ENABLED: 'true',  // Enable per-message ack tracking
  // ... other config
}
```

### Broker (broker.conf or Docker Compose)

```conf
acknowledgmentAtBatchIndexLevelEnabled=true
```

## Why Multiple Workers Matter

Modern Pulsar clients include **client-side deduplication** that filters duplicates within the same consumer instance. However, this state is **not shared across different worker processes**.

In production with multiple PM2 workers:
- Worker 1 processes batch, acks some messages, nacks others
- Broker (with batch-level tracking) marks entire batch as unacked
- Worker 2 (different process) receives the redelivered batch
- Worker 2 has no knowledge of Worker 1's processing
- Worker 2 processes all messages again → **Duplicates**

This replication simulates this by running 2 separate PM2 worker processes with independent consumer instances.

## References

- [Issue #5969](https://github.com/apache/pulsar/issues/5969) — Acked messages redelivered when others are negatively acked
- [Issue #23436](https://github.com/apache/pulsar/issues/23436) — Consumer receives duplicate messages even after acknowledging them
- [Issue #14982](https://github.com/apache/pulsar/issues/14982) — acknowledgmentAtBatchIndexLevelEnabled will still receive duplicate messages
- [PIP-54](https://github.com/apache/pulsar/wiki/PIP-54:-Support-acknowledgment-at-batch-index-level) — Support acknowledgment at batch index level
- [Pulsar Docs — Batching](https://pulsar.apache.org/docs/next/concepts-messaging/#batching)
