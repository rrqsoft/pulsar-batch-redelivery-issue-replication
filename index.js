import Pulsar from 'pulsar-client';

const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const TOPIC = 'persistent://public/default/test-batch-redelivery-4';
const SUBSCRIPTION = 'test-batch-sub';

const BATCH_TIMEOUT_MS = 10000; // 10 seconds
const ACK_TIMEOUT_MS = Math.ceil(BATCH_TIMEOUT_MS * 1.5); // 15 seconds (1.5:1 ratio)

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log('=== Pulsar Batch Redelivery Replication (Issue #5969) ===\n');
    console.log(`Config:`);
    console.log(`  PULSAR_URL: ${PULSAR_URL}`);
    console.log(`  TOPIC: ${TOPIC}`);
    console.log(`  BATCH_TIMEOUT_MS: ${BATCH_TIMEOUT_MS}`);
    console.log(`  ACK_TIMEOUT_MS: ${ACK_TIMEOUT_MS}`);
    console.log(`  Ratio: ${(ACK_TIMEOUT_MS / BATCH_TIMEOUT_MS).toFixed(1)}:1\n`);

    const client = new Pulsar.Client({
        serviceUrl: PULSAR_URL,
    });

    try {
        // Step 1: Create consumer first to establish subscription
        console.log('Step 1: Creating consumer with batch receive...');
        let consumer = await client.subscribe({
            topic: TOPIC,
            subscription: SUBSCRIPTION,
            subscriptionType: 'Shared',
            ackTimeoutMs: ACK_TIMEOUT_MS,
            receiverQueueSize: 10,
            batchReceivePolicy: {
                maxNumMessages: 10,
                maxNumBytes: 1024 * 1024,
                timeout: BATCH_TIMEOUT_MS,
            },
        });
        console.log('✓ Consumer created\n');

        // Step 1.5: Clear any existing backlog
        console.log('Step 1.5: Clearing existing backlog...');
        // let cleared = 0;
        // while (true) {
        //     try {
        //         const msg = await consumer.receive(1000); // 1s timeout
        //         await consumer.acknowledge(msg.getMessageId());
        //         cleared++;
        //     } catch (err) {
        //         break; // Timeout, no more messages
        //     }
        // }
        // console.log(`✓ Cleared ${cleared} old message(s) from backlog\n`);

        // Step 2: Create producer with batching enabled
        console.log('Step 2: Creating producer with batching enabled...');
        const producer = await client.createProducer({
            topic: TOPIC,
            batchingEnabled: true,
            batchingMaxMessages: 10,
            batchingMaxPublishDelayMs: 1000,
        });
        console.log('✓ Producer created\n');

        // Step 3: Publish 5 messages
        console.log('Step 3: Publishing 5 messages...');
        const messages = [
            { data: Buffer.from('Message 1'), properties: { id: 'msg-1' } },
            { data: Buffer.from('Message 2'), properties: { id: 'msg-2' } },
            { data: Buffer.from('Message 3'), properties: { id: 'msg-3' } },
            { data: Buffer.from('Message 4'), properties: { id: 'msg-4' } },
            { data: Buffer.from('Message 5'), properties: { id: 'msg-5' } },
        ];

        for (let i = 0; i < messages.length; i++) {
            await producer.send(messages[i]);
            console.log(`  ✓ Sent: ${messages[i].data.toString()} (id: ${messages[i].properties.id})`);
        }
        console.log('✓ All messages sent\n');

        // Flush to ensure batch is sent
        await producer.flush();
        console.log('✓ Producer flushed\n');

        // Step 4: Wait a moment for messages to arrive
        console.log('Step 4: Waiting for messages to arrive in consumer queue...');
        await sleep(2000);
        console.log('✓ Ready to receive\n');

        // Step 5: Receive messages in batch
        console.log('Step 5: Receiving batch (waiting up to 10s)...');
        const batch = await consumer.batchReceive();
        console.log(`✓ Received batch with ${batch.length} message(s)\n`);

        console.log('Step 5.1: Processing messages...');
        const receivedMessages = [];
        for (let i = 0; i < batch.length; i++) {
            const msg = batch[i];
            const content = msg.getData().toString();
            const msgId = msg.getProperties().id;
            receivedMessages.push({ content, msgId, message: msg });
            console.log(`  [${i}] Received: ${content} (id: ${msgId})`);
        }
        console.log();

        // Step 6: Acknowledge messages 1, 2, 4, 5 and negatively acknowledge message 3
        console.log('Step 6: ACK messages 1,2,4,5 and NACK message 3...');
        
        if (receivedMessages.length < 5) {
            console.log(`✗ ERROR: Expected 5 messages, got ${receivedMessages.length}. Cannot proceed.\n`);
            await consumer.close();
            await producer.close();
            await client.close();
            return;
        }

        try {
            // Acknowledge messages 1, 2, 4, 5 using Promise.all (same as production code)
            const messagesToAck = [
                receivedMessages[0].message, 
                receivedMessages[1].message,
                receivedMessages[3].message,
                receivedMessages[4].message
            ];
            await Promise.all(messagesToAck.map(async msg => {
                await consumer.acknowledge(msg);
            }));
            console.log(`  ✓ ACK: ${receivedMessages[0].content}`);
            console.log(`  ✓ ACK: ${receivedMessages[1].content}`);
            console.log(`  ✓ ACK: ${receivedMessages[3].content}`);
            console.log(`  ✓ ACK: ${receivedMessages[4].content}`);
            
            // Negative acknowledge the third message (same as production code line 139, 159, 181)
            await consumer.negativeAcknowledge(receivedMessages[2].message);
            console.log(`  ✗ NACK: ${receivedMessages[2].content}`);
            console.log('  (Message 3 will be redelivered after negative ack delay: ~60s)\n');
        } catch (err) {
            console.error('  ✗ Error during acknowledge:', err.message);
            console.error(err);
            throw err;
        }

        // Step 6.5: Close consumer to simulate different worker instance
        console.log('Step 6.5: Closing consumer (simulating worker restart)...');
        await consumer.close();
        console.log('✓ Consumer closed\n');
        console.log('  This simulates a different worker instance picking up the redelivered batch.');
        console.log('  Without shared client-side state, all messages in the batch will be redelivered.\n');

        // Step 7: Wait for negative ack redelivery
        console.log('Step 7: Waiting for negative ack redelivery (65 seconds)...');
        console.log('  (Default negative ack redelivery delay is 60s)');
        
        for (let i = 65; i > 0; i -= 5) {
            console.log(`  Waiting... ${i}s remaining`);
            await sleep(5000);
        }
        console.log();

        // Step 7.5: Recreate consumer (simulating new worker instance)
        console.log('Step 7.5: Recreating consumer (new worker instance)...');
        consumer = await client.subscribe({
            topic: TOPIC,
            subscription: SUBSCRIPTION,
            subscriptionType: 'Shared',
            ackTimeoutMs: ACK_TIMEOUT_MS,
            batchIndexAckEnabled: false,
            receiverQueueSize: 10,
            batchReceivePolicy: {
                maxNumMessages: 10,
                maxNumBytes: 1024 * 1024,
                timeout: BATCH_TIMEOUT_MS,
            },
        });
        console.log('✓ New consumer created (fresh client-side state)\n');

        // Step 8: Receive redelivered batch
        console.log('Step 8: Receiving redelivered batch...');
        console.log('Expected (batch index ack disabled): All 5 messages redelivered');
        console.log('Expected (batch index ack enabled): Only Message 3 redelivered\n');
        
        const redeliveredBatch = await consumer.batchReceive();
        console.log(`✓ Received redelivered batch with ${redeliveredBatch.length} message(s)\n`);
        
        console.log('Step 8.1: Logging redelivered messages...');
        const redeliveredMessages = [];
        for (let i = 0; i < redeliveredBatch.length; i++) {
            const msg = redeliveredBatch[i];
            const content = msg.getData().toString();
            const msgId = msg.getProperties().id;
            redeliveredMessages.push({ content, msgId, message: msg });
            console.log(`  [${i}] Redelivered: ${content} (id: ${msgId})`);
        }
        console.log();
        
        // Acknowledge all redelivered messages to clean up
        if (redeliveredMessages.length > 0) {
            await Promise.all(redeliveredMessages.map(async ({ message }) => {
                await consumer.acknowledge(message);
            }));
            console.log('✓ All redelivered messages acknowledged for cleanup\n');
        }

        // Assert: Check redelivery behavior
        console.log('Assertion: Checking redelivery behavior...');
        if (redeliveredMessages.length === 5) {
            console.log('✓ ISSUE CONFIRMED: All 5 messages redelivered (batch-level ack tracking)\n');
            console.log('  Root Cause: Without batchIndexAckEnabled, Pulsar tracks acks at');
            console.log('  batch level. Even though messages 1,2,4,5 were acknowledged, the entire');
            console.log('  batch is marked as unacked because message 3 was negatively acknowledged.\n');
        } else if (redeliveredMessages.length === 1) {
            console.log('✓ PASS: Only 1 message redelivered (batch index ack is working)\n');
            console.log('  This indicates batchIndexAckEnabled is properly tracking individual');
            console.log('  message acknowledgments within the batch.\n');
        } else {
            console.log(`⚠ UNEXPECTED: Expected 5 or 1 messages, got ${redeliveredMessages.length}`);
            console.log('  This may indicate:');
            console.log('  - Messages were not batched together by producer');
            console.log('  - Partial batch redelivery occurred');
            console.log('  - Network or timing issues\n');
        }

        console.log('=== Replication Complete ===\n');
        console.log('Result Summary:');
        console.log(`  - Initial receive: ${receivedMessages.length} messages`);
        console.log(`  - Acknowledged: Messages 1, 2, 4, 5`);
        console.log(`  - Negatively acknowledged: Message 3`);
        console.log(`  - Redelivered: ${redeliveredMessages.length} messages`);
        console.log(`\nConclusion:`);
        if (redeliveredMessages.length === 5) {
            console.log(`  ✗ ISSUE REPRODUCED: Batch-level acknowledgment tracking`);
            console.log(`     Even though messages 1,2,4,5 were acked, all 5 were redelivered.`);
        } else if (redeliveredMessages.length === 1) {
            console.log(`  ✓ FIX VERIFIED: Message-level acknowledgment tracking`);
            console.log(`     Only the nacked message (3) was redelivered.`);
        }
        console.log(`\nFix: Enable batchIndexAckEnabled on consumer AND`);
        console.log(`acknowledgmentAtBatchIndexLevelEnabled on broker.\n`);

        await consumer.close();
        await producer.close();
    } catch (error) {
        console.error('Error:', error);
    } finally {
        await client.close();
    }
}

main().catch(console.error);
