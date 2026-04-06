module.exports = {
  apps: [
    {
      name: 'pulsar-worker-1',
      script: './worker.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        WORKER_ID: '1',
        PULSAR_URL: 'pulsar://localhost:6650',
        TOPIC: 'persistent://public/default/test-batch-redelivery-3+',
        SUBSCRIPTION: 'test-batch-sub',
        BATCH_SIZE: '5',
        BATCH_TIMEOUT_MS: '10000',
        ACK_TIMEOUT_MS: '15000',
        BATCH_INDEX_ACK_ENABLED: 'false'
      }
    },
    {
      name: 'pulsar-worker-2',
      script: './worker.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        WORKER_ID: '2',
        PULSAR_URL: 'pulsar://localhost:6650',
        TOPIC: 'persistent://public/default/test-batch-redelivery',
        SUBSCRIPTION: 'test-batch-sub',
        BATCH_SIZE: '10',
        BATCH_TIMEOUT_MS: '10000',
        ACK_TIMEOUT_MS: '15000',
        BATCH_INDEX_ACK_ENABLED: 'false'
      }
    }
  ]
};
