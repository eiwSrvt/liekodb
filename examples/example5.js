const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    autoSaveInterval: 10000,
    debug: true
});

const analytics = db.collection('analytics');

async function bulkOperations() {
    console.log('=== Bulk Insert Performance Test ===');

    // Generate large dataset
    const events = [];
    const startTime = Date.now();

    for (let i = 1; i <= 10000; i++) {
        events.push({
            eventType: ['click', 'view', 'purchase', 'signup'][Math.floor(Math.random() * 4)],
            userId: `user_${Math.floor(Math.random() * 1000)}`,
            timestamp: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000).toISOString(),
            value: Math.floor(Math.random() * 100),
            metadata: {
                device: ['mobile', 'desktop', 'tablet'][Math.floor(Math.random() * 3)],
                country: ['US', 'UK', 'FR', 'DE', 'JP'][Math.floor(Math.random() * 5)]
            }
        });
    }

    console.log(`Generated ${events.length} events in ${Date.now() - startTime}ms`);

    // Bulk insert
    const insertStart = Date.now();
    const { data: insertResult } = await analytics.insert(events);
    console.log(`Inserted ${insertResult.insertedCount} documents in ${Date.now() - insertStart}ms`);

    // Bulk update - mark recent purchases
    console.log('\n=== Bulk Update ===');
    const updateStart = Date.now();
    const recentDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

    const { data: updateResult } = await analytics.update(
        {
            eventType: 'purchase',
            timestamp: { $gte: recentDate }
        },
        { $set: { processed: true } },
        { returnType: 'count' }
    );
    console.log(`Updated ${updateResult.updatedCount} documents in ${Date.now() - updateStart}ms`);

    // Aggregate queries
    console.log('\n=== Statistics ===');

    const { data: byType } = await analytics.find();
    const typeCount = {};
    byType.foundDocuments.forEach(event => {
        typeCount[event.eventType] = (typeCount[event.eventType] || 0) + 1;
    });
    console.log('Events by type:', typeCount);

    const { data: mobileUsers } = await analytics.find({
        'metadata.device': 'mobile',
        eventType: 'purchase'
    });
    console.log(`Mobile purchases: ${mobileUsers.foundCount}`);

    // Cleanup old events
    console.log('\n=== Cleanup ===');
    const oldDate = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
    const { data: deleteResult } = await analytics.delete({
        timestamp: { $lt: oldDate }
    });
    console.log(`Deleted ${deleteResult.deletedCount} old events`);

    const finalCount = await analytics.count();
    console.log(`Final count: ${finalCount.data} events`);

    await db.close();
}

bulkOperations().catch(console.error);