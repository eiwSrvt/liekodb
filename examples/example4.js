const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const profiles = db.collection('profiles');

async function updateOperations() {
    // Insert initial profile
    console.log('=== Inserting profile ===');
    const { data: insertResult } = await profiles.insert({
        username: 'john_doe',
        email: 'john@example.com',
        stats: {
            loginCount: 0,
            lastLogin: null,
            points: 100
        },
        tags: ['new'],
        preferences: {
            theme: 'light',
            notifications: true
        }
    });

    const userId = insertResult.insertedIds[0];

    // $inc - Increment login count
    console.log('\n=== Incrementing login count ===');
    await profiles.updateById(userId, {
        $inc: { 'stats.loginCount': 1 },
        $set: { 'stats.lastLogin': new Date().toISOString() }
    });

    // $push - Add tags
    console.log('\n=== Adding tags ===');
    await profiles.updateById(userId, {
        $push: { tags: 'active' }
    });

    // $addToSet - Add tag without duplicates
    console.log('\n=== Adding unique tag ===');
    await profiles.updateById(userId, {
        $addToSet: { tags: 'verified' }
    });
    await profiles.updateById(userId, {
        $addToSet: { tags: 'verified' }  // Won't create duplicate
    });

    // Multiple operations
    console.log('\n=== Multiple updates ===');
    await profiles.updateById(userId, {
        $inc: { 'stats.points': 50, 'stats.loginCount': 1 },
        $set: { 'preferences.theme': 'dark' },
        $push: { tags: 'premium' }
    });

    // $pull - Remove tag
    console.log('\n=== Removing tag ===');
    await profiles.updateById(userId, {
        $pull: { tags: 'new' }
    });

    // Get final state
    const { data: finalProfile } = await profiles.findById(userId);
    console.log('\n=== Final profile ===');
    console.log(JSON.stringify(finalProfile, null, 2));

    await db.close();
}

updateOperations().catch(console.error);