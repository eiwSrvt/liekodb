const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const products = db.collection('products');
const customers = db.collection('customers');
const orders = db.collection('orders');

async function ecommerceExample() {
    // Setup products
    console.log('=== Setting up products ===');
    await products.insert([
        { sku: 'LAPTOP-001', name: 'Pro Laptop', price: 1299.99, stock: 15, category: 'Electronics' },
        { sku: 'MOUSE-001', name: 'Wireless Mouse', price: 29.99, stock: 50, category: 'Accessories' },
        { sku: 'KEYBOARD-001', name: 'Mechanical Keyboard', price: 89.99, stock: 30, category: 'Accessories' },
        { sku: 'MONITOR-001', name: '4K Monitor', price: 449.99, stock: 20, category: 'Electronics' },
        { sku: 'HEADSET-001', name: 'Gaming Headset', price: 79.99, stock: 25, category: 'Accessories' }
    ]);

    // Register customers
    console.log('\n=== Registering customers ===');
    const { data: customer1 } = await customers.insert({
        name: 'Alice Johnson',
        email: 'alice@example.com',
        address: {
            street: '123 Main St',
            city: 'New York',
            country: 'US'
        },
        loyaltyPoints: 0,
        orders: []
    });

    const customerId = customer1.insertedIds[0];

    // Create order
    console.log('\n=== Creating order ===');
    const orderItems = [
        { sku: 'LAPTOP-001', quantity: 1, price: 1299.99 },
        { sku: 'MOUSE-001', quantity: 2, price: 29.99 }
    ];

    const orderTotal = orderItems.reduce((sum, item) =>
        sum + (item.price * item.quantity), 0
    );

    const { data: orderResult } = await orders.insert({
        customerId: customerId,
        items: orderItems,
        total: orderTotal,
        status: 'pending',
        shippingAddress: {
            street: '123 Main St',
            city: 'New York',
            country: 'US'
        }
    });

    const orderId = orderResult.insertedIds[0];

    // Update inventory
    console.log('\n=== Updating inventory ===');
    for (const item of orderItems) {
        await products.update(
            { sku: item.sku },
            { $inc: { stock: -item.quantity } }
        );
    }

    // Add loyalty points (10% of total)
    const pointsEarned = Math.floor(orderTotal * 0.1);
    await customers.updateById(customerId, {
        $inc: { loyaltyPoints: pointsEarned },
        $push: { orders: orderId }
    });

    // Process order
    console.log('\n=== Processing order ===');
    await orders.updateById(orderId, {
        $set: {
            status: 'shipped',
            shippedAt: new Date().toISOString()
        }
    });

    // Generate reports
    console.log('\n=== Sales Report ===');

    // Low stock products
    const { data: lowStock } = await products.find(
        { stock: { $lt: 20 } },
        { sort: { stock: 1 } }
    );
    console.log(`\nLow stock products (${lowStock.foundCount}):`);
    lowStock.foundDocuments.forEach(product => {
        console.log(`- ${product.name}: ${product.stock} units`);
    });

    // Customer summary
    const { data: customer } = await customers.findById(customerId);
    console.log(`\nCustomer: ${customer.name}`);
    console.log(`Loyalty Points: ${customer.loyaltyPoints}`);
    console.log(`Total Orders: ${customer.orders.length}`);

    // Orders by status
    const { data: pendingOrders } = await orders.count({ status: 'pending' });
    const { data: shippedOrders } = await orders.count({ status: 'shipped' });
    console.log(`\nOrders Summary:`);
    console.log(`- Pending: ${pendingOrders}`);
    console.log(`- Shipped: ${shippedOrders}`);

    await db.close();
}

ecommerceExample().catch(console.error);