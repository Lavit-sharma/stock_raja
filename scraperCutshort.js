const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());
const limit = pLimit(1);
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const RANDOM_CITIES = {
  delhi: "Delhi",
  bengaluru: "Bangalore",
  mumbai: "Mumbai",
  hyderabad: "Hyderabad",
};

const randomSleep = (min, max) =>
  new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min));

async function scrapeCutshort(keyword, location) {
  const roleQuery = keyword.replace("-", " ");
  const cityQuery = RANDOM_CITIES[location] || "Bangalore";
  const url = `https://cutshort.io/jobs?query=${encodeURIComponent(roleQuery)}&location=${encodeURIComponent(cityQuery)}`;

  console.log(`🔍 [Cutshort] Opening: ${url}`);
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox", "--disable-setuid-sandbox"] });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await randomSleep(4000, 6000);

    const jobs = await page.evaluate(() => {
      const clean = (v) => (v || "").replace(/\s+/g, " ").trim();
      const cards = Array.from(document.querySelectorAll("div[class*='job-card'], div[class*='JobCard']"));
      const output = [];

      for (const card of cards) {
        const titleEl = card.querySelector("a[href*='/jobs/'], h2, h3");
        const title = clean(titleEl?.innerText);
        const link = titleEl?.tagName === "A" ? titleEl.href : card.querySelector("a[href]")?.href || "";
        const company = clean(card.querySelector("div[class*='company'], span[class*='company']")?.innerText);
        const locationText = clean(card.querySelector("div[class*='location']")?.innerText);
        const expText = clean(card.querySelector("div[class*='experience']")?.innerText);

        if (title && link) {
          output.push({ title, company, location: locationText, experience: expText, link: link.startsWith("http") ? link : `https://cutshort.io${link}` });
        }
      }
      return output;
    });

    await page.close().catch(() => {});
    console.log(`🔗 [Cutshort] ${keyword} | ${location}: ${jobs.length} jobs found.`);

    // Fetch JDs
    return await Promise.all(
      jobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();
          await detailPage.setUserAgent(USER_AGENT);
          try {
            await detailPage.goto(job.link, { waitUntil: "domcontentloaded", timeout: 30000 });
            await randomSleep(2000, 3000);
            const fullDesc = await detailPage.evaluate(() => {
              const el = document.querySelector("div[class*='job-description'], div[class*='description']");
              return el ? el.innerText.replace(/\s+/g, " ").trim() : "";
            });
            return { ...job, companyProfile: "", fullDesc: fullDesc || "No description found", platform: "Cutshort" };
          } catch {
            return { ...job, companyProfile: "", fullDesc: "", platform: "Cutshort" };
          } finally {
            await detailPage.close().catch(() => {});
          }
        })
      )
    );
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeCutshort;
