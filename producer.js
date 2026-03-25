import Pulsar from 'pulsar-client';

const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const TOPIC = process.env.TOPIC || 'persistent://public/default/test-batch-redelivery-3+';
const NUM_MESSAGES = parseInt(process.env.NUM_MESSAGES || '5', 10);

async function main() {
    console.log('=== Pulsar Batch Producer ===\n');
    console.log(`Config:`);
    console.log(`  PULSAR_URL: ${PULSAR_URL}`);
    console.log(`  TOPIC: ${TOPIC}`);
    console.log(`  NUM_MESSAGES: ${NUM_MESSAGES}\n`);

    const client = new Pulsar.Client({
        serviceUrl: PULSAR_URL,
    });

    try {
        // Create producer with batching enabled and aggressive batching settings
        console.log('Creating producer with batching enabled...');
        const producer = await client.createProducer({
            topic: TOPIC,
            batchingEnabled: true,
            batchingMaxMessages: 100,  // High limit to ensure all 5 messages batch together
            batchingMaxPublishDelayMs: 100,  // Short delay to batch quickly
        });
        console.log('✓ Producer created\n');

        // Publish messages rapidly to ensure they're in the same producer batch
        console.log(`Publishing ${NUM_MESSAGES} messages (batched)...`);
        const messages = [];
        const sendPromises = [];
        
        for (let i = 1; i <= NUM_MESSAGES; i++) {
            messages.push({
                data: Buffer.from(`Message ${i}`),
                properties: { id: `msg-${i}`, timestamp: Date.now().toString() }
            });
        }

        // Send all messages rapidly without awaiting to ensure they batch together
        for (let i = 0; i < messages.length; i++) {
            sendPromises.push(producer.send(messages[i]));
            console.log(`  ✓ Queued: ${messages[i].data.toString()} (id: ${messages[i].properties.id})`);
        }
        
        // Wait for all sends to complete
        await Promise.all(sendPromises);
        console.log('✓ All messages sent\n');

        // Flush to ensure batch is sent
        await producer.flush();
        console.log('✓ Producer flushed (messages sent as single batch)\n');

        console.log('=== Producer Complete ===\n');
        console.log('Messages are now available for workers to consume.');
        console.log('Check worker logs with: pm2 logs\n');

        await producer.close();
        await client.close();
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}

main().catch(console.error);
