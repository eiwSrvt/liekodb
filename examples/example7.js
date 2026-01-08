const LiekoDB = require('liekodb');

const db = new LiekoDB({
    storagePath: './storage',
    debug: true
});

const sessions = db.collection('sessions');
const pageViews = db.collection('pageViews');

async function analyticsExample() {
    // Simulate user sessions
    console.log('=== Generating session data ===');

    const users = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5'];
    const pages = ['/home', '/products', '/about', '/contact', '/checkout'];

    // Create sessions
    for (let i = 0; i < 20; i++) {
        const userId = users[Math.floor(Math.random() * users.length)];
        const startTime = new Date(Date.now() - Math.random() * 24 * 60 * 60 * 1000);
        const duration = Math.floor(Math.random() * 1800) + 60; // 1-30 min

        await sessions.insert({
            userId,
            startTime: startTime.toISOString(),
            endTime: new Date(startTime.getTime() + duration * 1000).toISOString(),
            duration,
            device: ['mobile', 'desktop'][Math.floor(Math.random() * 2)],
            browser: ['Chrome', 'Firefox', 'Safari'][Math.floor(Math.random() * 3)],
            pagesVisited: Math.floor(Math.random() * 10) + 1
        });
    }

    // Generate page views
    for (let i = 0; i < 100; i++) {
        await pageViews.insert({
            userId: users[Math.floor(Math.random() * users.length)],
            page: pages[Math.floor(Math.random() * pages.length)],
            timestamp: new Date(Date.now() - Math.random() * 24 * 60 * 60 * 1000).toISOString(),
            duration: Math.floor(Math.random() * 300) + 10
        });
    }

    // Analytics queries
    console.log('\n=== Dashboard Analytics ===');

    // Total sessions by device
    const { data: allSessions } = await sessions.find();
    const deviceStats = {};
    allSessions.foundDocuments.forEach(session => {
        deviceStats[session.device] = (deviceStats[session.device] || 0) + 1;
    });
    console.log('\nSessions by device:', deviceStats);

    // Active users (sessions in last hour)
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const { data: recentSessions } = await sessions.find({
        startTime: { $gte: oneHourAgo }
    });
    const activeUsers = new Set(recentSessions.foundDocuments.map(s => s.userId));
    console.log(`\nActive users (last hour): ${activeUsers.size}`);

    // Average session duration
    const totalDuration = allSessions.foundDocuments.reduce((sum, s) => sum + s.duration, 0);
    const avgDuration = Math.floor(totalDuration / allSessions.foundCount);
    console.log(`Average session duration: ${Math.floor(avgDuration / 60)}m ${avgDuration % 60}s`);

    // Most visited pages
    const { data: allViews } = await pageViews.find();
    const pageStats = {};
    allViews.foundDocuments.forEach(view => {
        pageStats[view.page] = (pageStats[view.page] || 0) + 1;
    });

    console.log('\nMost visited pages:');
    Object.entries(pageStats)
        .sort((a, b) => b[1] - a[1])
        .forEach(([page, count]) => {
            console.log(`  ${page}: ${count} views`);
        });

    // Long sessions (> 10 minutes)
    const { data: longSessions } = await sessions.find({
        duration: { $gt: 600 }
    });
    console.log(`\nEngaged users (sessions > 10min): ${longSessions.foundCount}`);

    // Mobile vs Desktop engagement
    const { data: mobileSessions } = await sessions.find({ device: 'mobile' });
    const mobileAvg = mobileSessions.foundDocuments.reduce((sum, s) => sum + s.pagesVisited, 0)
        / mobileSessions.foundCount;

    const { data: desktopSessions } = await sessions.find({ device: 'desktop' });
    const desktopAvg = desktopSessions.foundDocuments.reduce((sum, s) => sum + s.pagesVisited, 0)
        / desktopSessions.foundCount;

    console.log(`\nAverage pages per session:`);
    console.log(`  Mobile: ${mobileAvg.toFixed(1)}`);
    console.log(`  Desktop: ${desktopAvg.toFixed(1)}`);

    await db.close();
}

analyticsExample().catch(console.error);