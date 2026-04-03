const youtube = require('youtube-transcript-api');
const mysql = require('mysql2/promise');

/**
 * FAIL-SAFE: This line ensures we find the correct 'fetchTranscript' method
 * regardless of whether the library uses 'default', 'YoutubeTranscript', or direct export.
 */
const getFetchMethod = () => {
    if (youtube.fetchTranscript) return youtube.fetchTranscript;
    if (youtube.default && youtube.default.fetchTranscript) return youtube.default.fetchTranscript;
    if (youtube.YoutubeTranscript && youtube.YoutubeTranscript.fetchTranscript) return youtube.YoutubeTranscript.fetchTranscript;
    return null;
};

const fetchTranscript = getFetchMethod();

// ---------------- CONFIG ---------------- //
const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    connectTimeout: 20000
};

const log = (msg) => console.log(`[${new Date().toLocaleTimeString()}] ${msg}`);

function extractVideoId(url) {
    const pattern = /(?:v=|\/)([0-9A-Za-z_-]{11})/;
    const match = url.match(pattern);
    return match ? match[1] : null;
}

async function runTranscriptJob(videoUrl) {
    const videoId = extractVideoId(videoUrl);
    if (!videoId) return log("❌ Invalid URL.");

    if (!fetchTranscript) {
        return log("❌ ERROR: Could not find fetchTranscript in the library. Please check your package.json.");
    }

    let connection;
    try {
        log(`🔍 Fetching transcript for ID: ${videoId}`);
        
        // Use the safely extracted fetch method
        const transcriptData = await fetchTranscript(videoId);
        const fullText = transcriptData.map(entry => entry.text).join(' ');

        log("✅ Transcript fetched. Connecting to Database...");

        connection = await mysql.createConnection(dbConfig);
        await connection.execute(`
            CREATE TABLE IF NOT EXISTS transcript (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id VARCHAR(50) UNIQUE,
                video_url VARCHAR(255),
                content LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await connection.execute(
            "INSERT INTO transcript (video_id, video_url, content) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE content = VALUES(content)",
            [videoId, videoUrl, fullText]
        );

        log("🚀 Success: Transcript saved to Database.");
    } catch (error) {
        log(`❌ ERROR: ${error.message}`);
    } finally {
        if (connection) await connection.end();
    }
}

runTranscriptJob(process.argv[2]);
