const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");

const keywords = [
  "developer",
  "engineer",
  "software",
  "data analyst",
  "java",
  "python",
  "frontend",
  "backend"
];

const locations = [
  "delhi",
  "mumbai",
  "bangalore",
  "hyderabad",
  "pune",
  "chennai",
  "remote"
];

(async () => {
  let finalJobs = [];
  const seen = new Set();

  for (let keyword of keywords) {
    for (let location of locations) {
      const jobs = await scrapeJobs(keyword, location, 5); // more pages

      const enriched = jobs.map(j => ({
        ...j,
        keyword,
        searchLocation: location
      }));

      const unique = enriched.filter(j => {
        if (!j.link || seen.has(j.link)) return false;
        seen.add(j.link);
        return true;
      });

      finalJobs.push(...unique);
    }
  }

  await writeToSheet(finalJobs);
})();
