const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 3; // parallel deep-scrape limit
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function delay(min = 2000, max = 5000) {
  return new Promise((r) => setTimeout(r, Math.random() * (max - min) + min));
}

/**
 * Scrape a single job's detail page.
 */
async function scrapeJobDetail(page, url) {
  try {
    // FIX: Brief delay to let the frame settle before navigating
    await new Promise((r) => setTimeout(r, 600));

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 35000 });
    await delay(1500, 3000);

    return await page.evaluate(() => {
      const pick = (selectors) => {
        for (const sel of selectors) {
          try {
            const el = document.querySelector(sel);
            if (el && el.innerText && el.innerText.trim()) {
              return el.innerText.trim();
            }
          } catch (e) {
            // ignore invalid selectors
          }
        }
        return "";
      };

      const companyProfile = pick([
        ".company-description",
        ".comp-desc",
        "[class*='comp-profile']",
        "[class*='about-company']",
      ]);

      const fullDesc = pick([
        ".job-desc",
        ".dang-inner-html",
        "[class*='dang-inner-html']",
        "[class*='JDC__dang']",
      ]);

      return { companyProfile, fullDesc };
    });
  } catch (err) {
    return { companyProfile: "", fullDesc: "", detailError: err.message };
  }
}

async function scrapeJobs(keyword, location, pages = 3) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-accelerated-2d-canvas",
      "--disable-gpu",
    ],
  });

  let allJobs = [];

  try {
    const page = await browser.newPage();
    
    // FIX: Prevent "Requesting main frame too early!" crash in GitHub Actions
    await new Promise((r) => setTimeout(r, 1000));

    await page.setUserAgent(USER_AGENT);

    for (let p = 1; p <= pages; p++) {
      const url = `https://www.naukri.com/${keyword}-jobs-in-${location}-${p}?jobAge=3`;
      console.log(`🔍 Opening: ${url}`);

      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 40000 });

        await page
          .waitForSelector(".srp-jobtuple-wrapper", { timeout: 10000 })
          .catch(() => {});

        const jobs = await page.evaluate(() => {
          const cards = document.querySelectorAll(".srp-jobtuple-wrapper");
          return Array.from(cards)
            .map((job) => ({
              title: job.querySelector(".title")?.innerText?.trim() || "",
              company: job.querySelector(".comp-name")?.innerText?.trim() || "",
              location: job.querySelector(".locWdth")?.innerText?.trim() || "",
              experience: job.querySelector(".expwdth")?.innerText?.trim() || "",
              link: job.querySelector("a.title")?.href || "",
            }))
            .filter((j) => j.link !== "");
        });

        allJobs.push(...jobs);
        console.log(`📄 Page ${p}: Found ${jobs.length} jobs`);
        await delay();
      } catch (err) {
        console.log(`⚠️ Page failed or blocked: ${p} (${err.message})`);
      }
    }

    // Deduplicate jobs by link
    const uniqueJobs = Array.from(new Map(allJobs.map((j) => [j.link, j])).values());
    console.log(`🔗 Total unique jobs to deep-scrape: ${uniqueJobs.length}`);

    // Parallel deep scraping of individual job detail pages
    const detailedJobs = await Promise.all(
      uniqueJobs.map((job) =>
        limit(async () => {
          const newPage = await browser.newPage();
          
          // FIX: Prevent main frame timing crash on parallel sub-pages
          await new Promise((r) => setTimeout(r, 800));

          try {
            await newPage.setUserAgent(USER_AGENT);
            const details = await scrapeJobDetail(newPage, job.link);
            return { ...job, ...details };
          } finally {
            await newPage.close().catch(() => {});
          }
        })
      )
    );

    return detailedJobs;
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeJobs;
