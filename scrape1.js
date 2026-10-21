const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 2; // parallel deep-scrape limit for detail pages
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function delay(min = 2500, max = 5000) {
  return new Promise((r) => setTimeout(r, Math.random() * (max - min) + min));
}

/**
 * Scrape a single job's detail page on Wellfound.
 */
async function scrapeJobDetail(page, url) {
  try {
    await new Promise((r) => setTimeout(r, 800));

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
        "[class*='companyOverview']",
        "[class*='styles_description']",
        "[data-test='CompanyProfile']",
        ".about-company"
      ]);

      const fullDesc = pick([
        "[class*='styles_jobDescription']",
        "[class*='jobDescription']",
        "[class*='styles_descriptionWrapper']",
        ".job-description"
      ]);

      return { companyProfile, fullDesc };
    });
  } catch (err) {
    return { companyProfile: "", fullDesc: "", detailError: err.message };
  }
}

async function scrapeJobs(keyword, location, pages = 2) {
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
    await new Promise((r) => setTimeout(r, 1000));
    await page.setUserAgent(USER_AGENT);

    // Formulate Wellfound URL target
    const url = `https://wellfound.com/jobs?q=${encodeURIComponent(keyword)}&l=${encodeURIComponent(location)}`;
    console.log(`🔍 Opening: ${url}`);

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 40000 });

      // Wait for job cards to render on page
      await page
        .waitForSelector("[class*='styles_component']", { timeout: 12000 })
        .catch(() => {});

      const jobs = await page.evaluate(() => {
        // Target job listing container cards on Wellfound
        const cards = document.querySelectorAll("[class*='styles_jobListing'], [class*='styles_component__']");
        
        return Array.from(cards)
          .map((job) => {
            const titleEl = job.querySelector("a[class*='styles_title']");
            const companyEl = job.querySelector("h2[class*='styles_title']");
            const locEl = job.querySelector("[class*='styles_location']");
            const expEl = job.querySelector("[class*='styles_experience']");

            return {
              title: titleEl?.innerText?.trim() || "",
              company: companyEl?.innerText?.trim() || "",
              location: locEl?.innerText?.trim() || "",
              experience: expEl?.innerText?.trim() || "",
              link: titleEl?.href || "",
            };
          })
          .filter((j) => j.link !== "");
      });

      allJobs.push(...jobs);
      console.log(`📄 Search result: Found ${jobs.length} jobs`);
      await delay();
    } catch (err) {
      console.log(`⚠️ Page failed or blocked: (${err.message})`);
    }

    // Deduplicate jobs by link
    const uniqueJobs = Array.from(new Map(allJobs.map((j) => [j.link, j])).values());
    console.log(`🔗 Total unique jobs to deep-scrape: ${uniqueJobs.length}`);

    // Parallel deep scraping of individual job detail pages
    const detailedJobs = await Promise.all(
      uniqueJobs.map((job) =>
        limit(async () => {
          const newPage = await browser.newPage();
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
