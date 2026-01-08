const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const users = db.collection('users');

async function basicCRUD() {
    // Insert users
    console.log('=== Inserting users ===');
    await users.insert([
        { username: 'alice', email: 'alice@example.com', age: 28, role: 'admin' },
        { username: 'bob', email: 'bob@example.com', age: 32, role: 'user' },
        { username: 'charlie', email: 'charlie@example.com', age: 25, role: 'user' }
    ]);

    // Find all users
    console.log('\n=== Finding all users ===');
    const { data: allUsers } = await users.find();
    console.log(`Found ${allUsers.foundCount} users`);

    // Find specific user
    console.log('\n=== Finding user by email ===');
    const { data: alice } = await users.findOne({ email: 'alice@example.com' });
    console.log('User:', alice);

    // Update user
    console.log('\n=== Updating user ===');
    await users.update(
        { username: 'alice' },
        { $set: { age: 29 } }
    );

    // Delete user
    console.log('\n=== Deleting user ===');
    await users.delete({ username: 'charlie' });

    // Final count
    const count = await users.count();
    console.log(`\nFinal count: ${count.data} users`);

    await db.close();
}

basicCRUD().catch(console.error);