const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const products = db.collection('products');

async function paginationExample() {
    // Insert sample products
    console.log('=== Inserting products ===');
    const productList = [];
    for (let i = 1; i <= 50; i++) {
        productList.push({
            name: `Product ${i}`,
            price: Math.floor(Math.random() * 1000) + 10,
            category: ['Electronics', 'Clothing', 'Books', 'Home'][Math.floor(Math.random() * 4)],
            stock: Math.floor(Math.random() * 100)
        });
    }
    await products.insert(productList);

    // Paginated query
    console.log('\n=== Page 1 (10 items per page) ===');
    const { data: page1 } = await products.find(
        { price: { $gte: 100 } },
        {
            sort: { price: -1 },
            limit: 10,
            page: 1,
            fields: { name: 1, price: 1, category: 1 }
        }
    );

    console.log(`Found ${page1.foundCount} products on this page`);
    console.log(`Total matching: ${page1.pagination.total}`);
    console.log(`Page ${page1.pagination.page} of ${page1.pagination.totalPages}`);
    console.log(`Has next page: ${page1.pagination.hasNext}`);

    page1.foundDocuments.forEach((product, idx) => {
        console.log(`${idx + 1}. ${product.name} - $${product.price}`);
    });

    // Get next page
    if (page1.pagination.hasNext) {
        console.log('\n=== Page 2 ===');
        const { data: page2 } = await products.find(
            { price: { $gte: 100 } },
            {
                sort: { price: -1 },
                limit: 10,
                page: 2,
                fields: { name: 1, price: 1 }
            }
        );

        page2.foundDocuments.forEach((product, idx) => {
            console.log(`${idx + 1}. ${product.name} - $${product.price}`);
        });
    }

    await db.close();
}

paginationExample().catch(console.error);