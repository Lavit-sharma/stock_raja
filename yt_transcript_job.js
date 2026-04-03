import * as yt from 'youtube-transcript';
import mysql from 'mysql2/promise';

// Extract the class from the namespace
const YoutubeTranscript = yt.YoutubeTranscript || yt.default?.YoutubeTranscript || yt.default;

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

    let connection;
    try {
        log(`🔍 Fetching transcript for ID: ${videoId}`);
        
        // This specific library call
        const transcriptData = await YoutubeTranscript.fetchTranscript(videoId);
        
        if (!transcriptData || transcriptData.length === 0) {
            throw new Error("No transcript data found.");
        }

        const fullText = transcriptData.map(entry => entry.text).join(' ');
        log(`✅ Successfully fetched ${transcriptData.length} lines.`);

        log("🗄️ Connecting to Database...");
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

        const sql = `
            INSERT INTO transcript (video_id, video_url, content)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE content = VALUES(content)
        `;

        await connection.execute(sql, [videoId, videoUrl, fullText]);
        log("🚀 SUCCESS: Transcript saved to Database.");

    } catch (error) {
        log(`❌ ERROR: ${error.message}`);
    } finally {
        if (connection) {
            await connection.end();
            log("🔌 DB Connection closed.");
        }
    }
}

const videoUrl = process.argv[2];
if (videoUrl) {
    runTranscriptJob(videoUrl);
} else {
    log("❌ No URL provided.");
}
