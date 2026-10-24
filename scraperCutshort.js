const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 1; // Rate limit detail page fetching to avoid blocks
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const LOCATION_MAPPING = {
  delhi: "Delhi",
  bengaluru: "Bangalore",
  bangalore: "Bangalore",
  mumbai: "Mumbai",
  hyderabad: "Hyderabad",
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomSleep = (min, max) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );

function buildCutshortUrl(keyword, location) {
  const roleQuery = keyword.replace(/-/g, " ");
  const cityQuery = LOCATION_MAPPING[location] || "Bangalore";
  return `https://cutshort.io/jobs?query=${encodeURIComponent(roleQuery)}&location=${encodeURIComponent(cityQuery)}`;
}

async function scrapeCutshortSearch(page, url) {
  console.log(`🔍 [Cutshort] Opening search URL: ${url}`);

  try {
    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 45000,
    });

    console.log(`🌐 [Cutshort] HTTP Status: ${response?.status() ?? "No response"}`);
    await randomSleep(4000, 6000); // Allow dynamic JavaScript/Angular app to render

    // Scroll down to trigger lazy loading of job cards
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let totalHeight = 0;
        const distance = 400;
        const timer = setInterval(() => {
          window.scrollBy(0, distance);
          totalHeight += distance;
          if (totalHeight >= 2000 || totalHeight >= document.body.scrollHeight) {
            clearInterval(timer);
            resolve();
          }
        }, 300);
      });
    });

    await randomSleep(2000, 3000);

    return await page.evaluate(() => {
      const clean = (v) => (v || "").replace(/\s+/g, " ").trim();
      const jobCards = Array.from(document.querySelectorAll("div[class*='job-card'], div[class*='JobCard'], article"));
      const output = [];

      for (const card of jobCards) {
        const titleEl = card.querySelector("a[href*='/jobs/'], h2, h3, a[class*='title']");
        const title = clean(titleEl?.innerText);
        
        let link = "";
        const anchor = card.querySelector("a[href*='/jobs/']") || card.querySelector("a[href]");
        if (anchor && anchor.href) {
          link = anchor.href;
        }

        if (!title || !link) continue;

        const companyEl = card.querySelector("div[class*='company'], span[class*='company'], a[class*='company']");
        const company = clean(companyEl?.innerText);

        const locationEl = card.querySelector("div[class*='location'], span[class*='location']");
        const location = clean(locationEl?.innerText);

        const expEl = card.querySelector("div[class*='experience'], span[class*='experience']");
        const experience = clean(expEl?.innerText);

        output.push({
          title,
          company,
          location,
          experience,
          link: link.startsWith("http") ? link : `https://cutshort.io${link}`,
        });
      }

      // Deduplicate by link
      const uniqueMap = new Map();
      for (const job of output) {
        if (!uniqueMap.has(job.link)) uniqueMap.set(job.link, job);
      }
      return Array.from(uniqueMap.values());
    });
  } catch (error) {
    console.error(`❌ [Cutshort] Search failed for ${url}: ${error.message}`);
    return [];
  }
}

async function scrapeCutshortDetail(page, url) {
  try {
    console.log(`📄 [Cutshort] Fetching JD: ${url}`);
    
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 35000,
    });

    await randomSleep(2500, 4000);

    return await page.evaluate(() => {
      const clean = (v) => (v || "").replace(/\s+/g, " ").trim();

      const pick = (selectors) => {
        for (const s of selectors) {
          const el = document.querySelector(s);
          const t = clean(el?.innerText);
          if (t && t.length > 20) return t;
        }
        return "";
      };

      const companyProfile = pick([
        "[class*='company-overview']",
        "[class*='about-company']",
        "[class*='companyProfile']",
      ]);

      let fullDesc = pick([
        "[class*='job-description']",
        "[class*='jobDescription']",
        "[class*='description-wrapper']",
        "div[class*='details']",
      ]);

      if (!fullDesc) {
        const main = document.querySelector("main") || document.body;
        const text = clean(main?.innerText);
        if (text.length > 200) {
          fullDesc = text.slice(0, 15000);
        }
      }

      return {
        companyProfile,
        fullDesc: fullDesc || "No detailed JD found.",
      };
    });
  } catch (error) {
    console.error(`⚠️ [Cutshort] Detail page error (${url}): ${error.message}`);
    return {
      companyProfile: "",
      fullDesc: "Failed to load job description",
    };
  }
}

async function scrapeCutshort(keyword, location) {
  const url = buildCutshortUrl(keyword, location);
  
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--window-size=1366,768",
    ],
  });

  try {
    const searchPage = await browser.newPage();
    await searchPage.setUserAgent(USER_AGENT);
    await searchPage.setViewport({ width: 1366, height: 768 });

    const rawJobs = await scrapeCutshortSearch(searchPage, url);
    await searchPage.close().catch(() => {});

    console.log(`🔗 [Cutshort] ${keyword} | ${location}: Found ${rawJobs.length} listings. Fetching JDs...`);

    const detailedJobs = await Promise.all(
      rawJobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();
          await detailPage.setUserAgent(USER_AGENT);
          await detailPage.setViewport({ width: 1366, height: 768 });

          const details = await scrapeCutshortDetail(detailPage, job.link);
          await detailPage.close().catch(() => {});

          return {
            ...job,
            ...details,
            keyword,
            searchLocation: location,
            platform: "Cutshort",
          };
        })
      )
    );

    return detailedJobs;
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeCutshort;
