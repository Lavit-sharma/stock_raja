const scrapeWellfound = require("./scraperWellfound");
const scrapeCutshort = require("./scraperCutshort");
const writeToSheet = require("./sheets1");

const keywords = [
  "product-manager",
  "engineering-manager",
  "edge-ai",
];

const locations = [
  "delhi",
  "bengaluru",
  "mumbai",
  "hyderabad",
];

(async () => {
  try {
    console.log("🚀 Starting Job Scraping Pipeline (Wellfound + Cutshort)...");

    let allCollectedJobs = [];

    for (const keyword of keywords) {
      for (const location of locations) {
        console.log(`\n📌 Processing Combination: ${keyword} | ${location}`);

        try {
          // 1. Scrape Wellfound
          const wellfoundJobs = await scrapeWellfound(keyword, location, 1);
          allCollectedJobs.push(...wellfoundJobs);

          // 2. Scrape Cutshort
          const cutshortJobs = await scrapeCutshort(keyword, location);
          allCollectedJobs.push(...cutshortJobs);

        } catch (taskError) {
          console.error(`❌ Task failed for ${keyword} | ${location}: ${taskError.message}`);
        }
      }
    }

    // Deduplicate across all platforms based on job link
    const uniqueMap = new Map();
    for (const job of allCollectedJobs) {
      if (job.link && !uniqueMap.has(job.link)) {
        uniqueMap.set(job.link, job);
      }
    }

    const finalJobs = Array.from(uniqueMap.values());

    console.log("\n======================================");
    console.log(`📦 Total Raw Collected: ${allCollectedJobs.length}`);
    console.log(`🔗 Total Unique Jobs: ${finalJobs.length}`);
    console.log("======================================\n");

    // Send results to Google Sheets
    await writeToSheet(finalJobs);

    console.log("🎉 Pipeline completed successfully.");
  } catch (error) {
    console.error("❌ Main pipeline error:", error);
    process.exitCode = 1;
  }
})();
