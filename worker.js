import Pulsar from 'pulsar-client';

const WORKER_ID = process.env.WORKER_ID || 'unknown';
const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const TOPIC = process.env.TOPIC || 'persistent://public/default/test-batch-redelivery-1';
const SUBSCRIPTION = process.env.SUBSCRIPTION || 'test-batch-sub';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10', 10);
const BATCH_TIMEOUT_MS = parseInt(process.env.BATCH_TIMEOUT_MS || '10000', 10);
const ACK_TIMEOUT_MS = parseInt(process.env.ACK_TIMEOUT_MS || '15000', 10);
const BATCH_INDEX_ACK_ENABLED = process.env.BATCH_INDEX_ACK_ENABLED === 'true';

// Track processed messages to detect duplicates
const processedMessages = new Map();
let isShuttingDown = false;

function log(message, ...args) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [Worker ${WORKER_ID}] ${message}`, ...args);
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    log('=== Pulsar Consumer Worker Started ===');
    log(`Config:`);
    log(`  PULSAR_URL: ${PULSAR_URL}`);
    log(`  TOPIC: ${TOPIC}`);
    log(`  SUBSCRIPTION: ${SUBSCRIPTION}`);
    log(`  BATCH_SIZE: ${BATCH_SIZE}`);
    log(`  BATCH_TIMEOUT_MS: ${BATCH_TIMEOUT_MS}`);
    log(`  ACK_TIMEOUT_MS: ${ACK_TIMEOUT_MS}`);
    log(`  BATCH_INDEX_ACK_ENABLED: ${BATCH_INDEX_ACK_ENABLED}`);
    log('');

    const client = new Pulsar.Client({
        serviceUrl: PULSAR_URL,
    });

    let consumer = null;

    try {
        // Create consumer with batch receive
        log('Creating consumer...');
        consumer = await client.subscribe({
            topic: TOPIC,
            subscription: SUBSCRIPTION,
            subscriptionType: 'Shared',
            ackTimeoutMs: ACK_TIMEOUT_MS,
            batchIndexAckEnabled: BATCH_INDEX_ACK_ENABLED,
            receiverQueueSize: BATCH_SIZE,
            nAckRedeliverTimeoutMs: 3000, // 3 seconds for testing (to confirm it was due to certain issue)
            batchReceivePolicy: {
                maxNumMessages: BATCH_SIZE,
                maxNumBytes: 1024 * 1024,
                timeout: BATCH_TIMEOUT_MS,
            },
        });
        log('✓ Consumer created');
        log('Waiting for messages...\n');

        // Main processing loop
        while (!isShuttingDown) {
            try {
                const batch = await consumer.batchReceive();
                
                if (!batch || batch.length === 0) {
                    await sleep(1000);
                    continue;
                }

                log(`📦 Received batch with ${batch.length} message(s)`);

                const receivedMessages = [];
                for (let i = 0; i < batch.length; i++) {
                    const msg = batch[i];
                    const content = msg.getData().toString();
                    const msgId = msg.getProperties().id;
                    const timestamp = msg.getProperties().timestamp;
                    
                    receivedMessages.push({ content, msgId, message: msg, timestamp });
                    
                    // Check for duplicates
                    const previousProcessing = processedMessages.get(msgId);
                    if (previousProcessing) {
                        log(`  ⚠️  DUPLICATE DETECTED: ${msgId}`);
                        log(`      First processed at: ${previousProcessing.timestamp}`);
                        log(`      First processed by: Worker ${previousProcessing.workerId}`);
                        log(`      Redelivered after: ${Date.now() - previousProcessing.processedAt}ms`);
                    } else {
                        log(`  [${i}] Received: ${content} (id: ${msgId})`);
                    }
                }

                log('');

                // Process messages with ack pattern: ack 1,2,4,5 and nack 3
                const messagesToAck = [];
                const messagesToNack = [];

                for (const { content, msgId, message } of receivedMessages) {
                    // Ack pattern: nack message 3, ack all others
                    if (msgId === 'msg-3') {
                        // Track even nacked messages to detect duplicates
                        processedMessages.set(msgId, {
                            workerId: WORKER_ID,
                            processedAt: Date.now(),
                            timestamp: new Date().toISOString(),
                            action: 'NACK'
                        });
                        messagesToNack.push({ content, msgId, message });
                    } else {
                        processedMessages.set(msgId, {
                            workerId: WORKER_ID,
                            processedAt: Date.now(),
                            timestamp: new Date().toISOString(),
                            action: 'ACK'
                        });
                        messagesToAck.push({ content, msgId, message });
                    }
                }

                // Acknowledge messages using Promise.all (production pattern)
                if (messagesToAck.length > 0) {
                    await Promise.all(messagesToAck.map(async ({ content, message }) => {
                        await consumer.acknowledge(message);
                    }));
                    
                    for (const { content, msgId } of messagesToAck) {
                        log(`  ✓ ACK: ${content} (id: ${msgId})`);
                    }
                }

                // Negative acknowledge messages
                if (messagesToNack.length > 0) {
                    for (const { content, msgId, message } of messagesToNack) {
                        await consumer.negativeAcknowledge(message);
                        log(`  ✗ NACK: ${content} (id: ${msgId})`);
                        log(`      Will be redelivered after ~60s`);
                    }
                }

                log('');

            } catch (err) {
                if (err.message && err.message.includes('timeout')) {
                    // Batch receive timeout, continue
                    continue;
                }
                log('Error processing batch:', err.message);
                await sleep(1000);
            }
        }

    } catch (error) {
        log('Fatal error:', error);
        process.exit(1);
    } finally {
        if (consumer) {
            await consumer.close();
            log('Consumer closed');
        }
        await client.close();
        log('Client closed');
    }
}

// Graceful shutdown
const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
signals.forEach(signal => {
    process.on(signal, async () => {
        log(`Received ${signal}, shutting down gracefully...`);
        isShuttingDown = true;
        
        // Give time for current batch to finish
        await sleep(2000);
        process.exit(0);
    });
});

main().catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
});
