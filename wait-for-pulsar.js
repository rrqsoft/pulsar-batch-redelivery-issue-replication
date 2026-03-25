import Pulsar from 'pulsar-client';

const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const MAX_RETRIES = 30;
const RETRY_DELAY = 2000;

async function waitForPulsar() {
    console.log(`Waiting for Pulsar at ${PULSAR_URL}...`);
    
    for (let i = 1; i <= MAX_RETRIES; i++) {
        try {
            const client = new Pulsar.Client({
                serviceUrl: PULSAR_URL,
                operationTimeoutSeconds: 5,
            });
            
            // Try to create a test producer
            const producer = await client.createProducer({
                topic: 'persistent://public/default/test-connection',
                sendTimeoutMs: 5000,
            });
            
            await producer.close();
            await client.close();
            
            console.log('✓ Pulsar is ready!');
            process.exit(0);
        } catch (error) {
            console.log(`Attempt ${i}/${MAX_RETRIES}: Pulsar not ready yet... (${error.message})`);
            
            if (i === MAX_RETRIES) {
                console.error('✗ Failed to connect to Pulsar after maximum retries');
                process.exit(1);
            }
            
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
        }
    }
}

waitForPulsar().catch(error => {
    console.error('Error:', error);
    process.exit(1);
});
