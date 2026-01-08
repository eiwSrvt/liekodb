const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const orders = db.collection('orders');

async function advancedFiltering() {
    // Insert sample orders
    console.log('=== Inserting orders ===');
    await orders.insert([
        {
            orderId: 'ORD001',
            customer: { name: 'Alice', email: 'alice@example.com', vip: true },
            items: ['laptop', 'mouse', 'keyboard'],
            total: 1250.50,
            status: 'completed',
            shippedAt: '2025-01-05T10:00:00Z'
        },
        {
            orderId: 'ORD002',
            customer: { name: 'Bob', email: 'bob@example.com', vip: false },
            items: ['phone'],
            total: 899.99,
            status: 'pending',
            shippedAt: null
        },
        {
            orderId: 'ORD003',
            customer: { name: 'Charlie', email: 'charlie@gmail.com', vip: true },
            items: ['tablet', 'case'],
            total: 450.00,
            status: 'completed',
            shippedAt: '2025-01-07T14:30:00Z'
        },
        {
            orderId: 'ORD004',
            customer: { name: 'Diana', email: 'diana@example.com', vip: false },
            items: ['headphones'],
            total: 150.00,
            status: 'cancelled',
            shippedAt: null
        },
        {
            orderId: 'ORD005',
            customer: { name: 'Eve', email: 'eve@gmail.com', vip: true },
            items: ['monitor', 'cable', 'adapter'],
            total: 650.00,
            status: 'pending',
            shippedAt: null
        }
    ]);

    // Complex filter 1: VIP customers with completed orders over $500
    console.log('\n=== VIP customers with completed orders > $500 ===');
    const { data: vipOrders } = await orders.find({
        'customer.vip': true,
        status: 'completed',
        total: { $gt: 500 }
    });
    console.log(`Found ${vipOrders.foundCount} orders`);
    vipOrders.foundDocuments.forEach(order => {
        console.log(`- ${order.orderId}: ${order.customer.name} - $${order.total}`);
    });

    // Complex filter 2: Gmail users OR orders with multiple items
    console.log('\n=== Gmail users OR orders with 3+ items ===');
    const { data: complexOrders } = await orders.find({
        $or: [
            { 'customer.email': { $regex: /@gmail\.com$/ } },
            { items: { $exists: true } }
        ]
    });

    const filtered = complexOrders.foundDocuments.filter(order =>
        order.customer.email.includes('@gmail.com') || order.items.length >= 3
    );

    console.log(`Found ${filtered.length} orders`);
    filtered.forEach(order => {
        console.log(`- ${order.orderId}: ${order.items.length} items, ${order.customer.email}`);
    });

    // Complex filter 3: Pending or cancelled, not shipped, under $1000
    console.log('\n=== Pending/Cancelled, not shipped, under $1000 ===');
    const { data: pendingOrders } = await orders.find({
        status: { $in: ['pending', 'cancelled'] },
        shippedAt: null,
        total: { $lt: 1000 }
    });
    console.log(`Found ${pendingOrders.foundCount} orders`);
    pendingOrders.foundDocuments.forEach(order => {
        console.log(`- ${order.orderId}: ${order.status} - $${order.total}`);
    });

    // Complex filter 4: NOT cancelled AND (VIP OR total > $800)
    console.log('\n=== Active orders: VIP or high value ===');
    const { data: activeOrders } = await orders.find({
        status: { $ne: 'cancelled' },
        $or: [
            { 'customer.vip': true },
            { total: { $gte: 800 } }
        ]
    });
    console.log(`Found ${activeOrders.foundCount} orders`);
    activeOrders.foundDocuments.forEach(order => {
        console.log(`- ${order.orderId}: ${order.customer.name} (VIP: ${order.customer.vip}) - $${order.total}`);
    });

    // Complex filter 5: NOR query - exclude both cancelled and low-value orders
    console.log('\n=== Exclude cancelled AND orders under $200 ===');
    const { data: qualityOrders } = await orders.find({
        $nor: [
            { status: 'cancelled' },
            { total: { $lt: 200 } }
        ]
    });
    console.log(`Found ${qualityOrders.foundCount} orders`);
    qualityOrders.foundDocuments.forEach(order => {
        console.log(`- ${order.orderId}: ${order.status} - $${order.total}`);
    });

    await db.close();
}

advancedFiltering().catch(console.error);