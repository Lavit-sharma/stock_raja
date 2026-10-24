const scrapeWellfound = require("./scraperWellfound");
const scrapeCutshort = require("./scraperCutshort");
const scrapeYC = require("./scraperYC");
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
    console.log("🚀 Starting Multi-Platform Job Scraping Pipeline (Wellfound + Cutshort + YC)...");

    let allCollectedJobs = [];

    for (const keyword of keywords) {
      for (const location of locations) {
        console.log(`\n📌 Processing Task: ${keyword} | ${location}`);

        try {
          // 1. Scrape Wellfound
          const wellfoundJobs = await scrapeWellfound(keyword, location, 1);
          allCollectedJobs.push(...wellfoundJobs);

          // 2. Scrape Cutshort
          const cutshortJobs = await scrapeCutshort(keyword, location);
          allCollectedJobs.push(...cutshortJobs);

          // 3. Scrape YC (Location independent search)
          const ycJobs = await scrapeYC(keyword, location);
          allCollectedJobs.push(...ycJobs);

        } catch (err) {
          console.error(`❌ Error in task ${keyword} | ${location}: ${err.message}`);
        }
      }
    }

    // Deduplicate across all scraped items based on link
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

    // Write to Google Sheets (Sheet3)
    await writeToSheet(finalJobs);

    console.log("🎉 Complete Multi-Platform Pipeline Finished Successfully.");
  } catch (error) {
    console.error("❌ Main pipeline error:", error);
    process.exitCode = 1;
  }
})();
