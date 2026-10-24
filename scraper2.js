const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 2; // Increased slightly for better throughput
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const LOCATION_SLUGS = {
  delhi: "delhi-ncr",
  bengaluru: "bengaluru",
  bangalore: "bengaluru",
  mumbai: "mumbai",
  hyderabad: "hyderabad",
};

const ROLE_SLUGS = {
  "product-manager": "product-manager-jobs",
  "engineering-manager": "engineering-manager-jobs",
  "edge-ai": "artificial-intelligence-ai-jobs",
};

const randomSleep = (min, max) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );

function buildSearchUrl(keyword, location) {
  const role = ROLE_SLUGS[keyword];
  const place = LOCATION_SLUGS[location];

  if (!role) throw new Error(`No Cutshort role mapping for: ${keyword}`);
  if (!place) throw new Error(`No Cutshort location mapping for: ${location}`);

  return `https://cutshort.io/jobs/${role}-in-${place}`;
}

async function scrapeSearchPage(page, url, keyword) {
  console.log(`🔍 Opening Cutshort URL: ${url}`);

  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });

  console.log(`🌐 HTTP: ${response?.status() ?? "No response"} | ${page.url()}`);

  if (response?.status() === 403) {
    console.error("🛡️ Cloudflare/Security Block encountered on search page.");
    return [];
  }

  // Scroll down to trigger lazy loading of job cards
  try {
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let totalHeight = 0;
        const distance = 500;
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
  } catch (e) {
    // Ignore scroll errors
  }

  // Wait for dynamic React container content to render
  try {
    await page.waitForFunction(
      () => document.querySelectorAll("a[href*='/job/']").length > 0,
      { timeout: 12000 }
    );
  } catch (e) {
    console.log("⚠️ Job links took too long to appear, attempting fallback parse...");
  }

  await randomSleep(2000, 3000);

  return await page.evaluate(() => {
    const clean = (val) => (val || "").replace(/\s+/g, " ").trim();
    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const output = [];

    for (const anchor of anchors) {
      const link = anchor.href;
      // Match Cutshort job link pattern
      if (!/^https:\/\/(www\.)?cutshort\.io\/job\/[^/?#]+/i.test(link)) {
        continue;
      }

      const title = clean(anchor.innerText);
      if (!title || title.length > 180) continue;

      let card = anchor;
      let cardText = "";

      for (let level = 0; level < 6 && card; level++) {
        const text = clean(card.innerText);
        if (
          text.length >= title.length &&
          text.length < 4000 &&
          /exp\b|years|full.?time|remote|hybrid|onsite|salary|lac|lpa/i.test(text)
        ) {
          cardText = text;
          break;
        }
        card = card.parentElement;
      }

      if (!cardText) {
        cardText = clean(anchor.parentElement?.innerText);
      }

      const headings = card
        ? Array.from(card.querySelectorAll("h2, h3, h4, span, div"))
            .map((el) => clean(el.innerText))
            .filter(Boolean)
        : [];

      const company = headings.find((h) => h !== title && h.length < 50) || "";
      const experienceMatch = cardText.match(/\b\d+(?:-\d+)?\s*(?:\+)?\s*years?\b/i);
      const locationMatch = cardText.match(/(?:Remote|Hybrid|Onsite|In office)\s*[•·|-]?\s*([^•·|\n]{2,100})/i);

      output.push({
        title,
        company,
        location: locationMatch ? clean(locationMatch[0]) : "",
        experience: experienceMatch ? experienceMatch[0] : "",
        link,
      });
    }

    const unique = new Map();
    for (const job of output) {
      if (!unique.has(job.link)) unique.set(job.link, job);
    }
    return Array.from(unique.values());
  });
}

async function scrapeJobDetail(page, url) {
  try {
    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 35000,
    });

    if (response && response.status() === 403) {
      return { companyProfile: "", fullDesc: "403 Forbidden", detailError: "403" };
    }

    await randomSleep(1500, 2500);

    return await page.evaluate(() => {
      const clean = (val) => (val || "").replace(/\s+/g, " ").trim();
      const main = document.querySelector("main") || document.body;
      const text = clean(main?.innerText);

      return {
        companyProfile: "",
        fullDesc: text.length > 100 ? text : "No detailed JD found.",
      };
    });
  } catch (error) {
    return { companyProfile: "", fullDesc: "", detailError: error.message };
  }
}

async function createStabilizedPage(browser) {
  const page = await browser.newPage();
  // Safe initialization waiting for target frames to attach fully
  try {
    await page.setCacheEnabled(false);
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1366, height: 768 });
  } catch (err) {
    // Fallback if initial target setup needs a tick
    await new Promise((r) => setTimeout(r, 200));
  }
  return page;
}

async function scrapeJobs(keyword, location) {
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
    const searchPage = await createStabilizedPage(browser);
    let uniqueJobs = [];

    try {
      const url = buildSearchUrl(keyword, location);
      const jobs = await scrapeSearchPage(searchPage, url, keyword);
      
      uniqueJobs =
        keyword === "edge-ai"
          ? jobs.filter((job) =>
              /\bedge\b|\bai\b|artificial intelligence|machine learning|computer vision|embedded/i.test(
                job.title
              )
            )
          : jobs;
    } catch (error) {
      console.error(`❌ Search failed (${keyword} | ${location}): ${error.message}`);
    } finally {
      await searchPage.close().catch(() => {});
    }

    console.log(`🔗 ${keyword} | ${location}: ${uniqueJobs.length} unique Cutshort jobs found.`);

    const detailedJobs = await Promise.all(
      uniqueJobs.map((job) =>
        limit(async () => {
          const detailPage = await createStabilizedPage(browser);
          try {
            const details = await scrapeJobDetail(detailPage, job.link);
            return { ...job, ...details };
          } finally {
            await detailPage.close().catch(() => {});
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
