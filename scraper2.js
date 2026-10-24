const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 1;
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

  if (!role) {
    throw new Error(`No Cutshort role mapping for: ${keyword}`);
  }

  if (!place) {
    throw new Error(`No Cutshort location mapping for: ${location}`);
  }

  return `https://cutshort.io/jobs/${role}-in-${place}`;
}

async function scrapeSearchPage(page, url, keyword) {
  console.log(`🔍 Opening Cutshort URL: ${url}`);

  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });

  console.log(
    `🌐 HTTP: ${response?.status() ?? "No response"} | ${page.url()}`
  );

  if (response?.status() === 403) {
    console.error("🛡️ Cloudflare/Security Block encountered on search page.");
  }

  await randomSleep(3000, 5000);

  const result = await page.evaluate(() => {
    const clean = (value) => (value || "").replace(/\s+/g, " ").trim();

    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const output = [];

    for (const anchor of anchors) {
      const link = anchor.href;

      if (!/^https:\/\/(www\.)?cutshort\.io\/job\/[^/?#]+/i.test(link)) {
        continue;
      }

      const title = clean(anchor.innerText);
      if (!title || title.length > 180) continue;

      let card = anchor;
      let cardText = "";

      for (let level = 0; level < 7 && card; level++) {
        const text = clean(card.innerText);

        if (
          text.length >= title.length &&
          text.length < 3500 &&
          /exp\b|years|full.?time|remote|hybrid|onsite|salary|lac|lpa/i.test(
            text
          )
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
        ? Array.from(card.querySelectorAll("h2, h3, h4"))
            .map((el) => clean(el.innerText))
            .filter(Boolean)
        : [];

      const company =
        headings.find((heading) => heading !== title) || "";

      const experienceMatch = cardText.match(
        /\b\d+(?:-\d+)?\s*(?:\+)?\s*years?\b/i
      );

      const locationMatch = cardText.match(
        /(?:Remote|Hybrid|Onsite|In office)\s*[•·|-]?\s*([^•·|\n]{2,100})/i
      );

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

    return {
      pageTitle: document.title,
      pageText: clean(document.body?.innerText).slice(0, 600),
      jobs: Array.from(unique.values()),
    };
  });

  console.log(`📄 Page title: ${result.pageTitle}`);
  console.log(`📄 Candidate jobs: ${result.jobs.length}`);

  if (result.jobs.length === 0) {
    console.log(`⚠️ No job links found on ${url}`);
    console.log(`Page preview: ${result.pageText}`);
  }

  const filtered =
    keyword === "edge-ai"
      ? result.jobs.filter((job) =>
          /\bedge\b|\bai\b|artificial intelligence|machine learning|computer vision|embedded/i.test(
            job.title
          )
        )
      : result.jobs;

  return filtered;
}

async function scrapeJobDetail(page, url) {
  try {
    console.log(`📄 Fetching Job Description: ${url}`);

    await page.setExtraHTTPHeaders({
      "accept-language": "en-US,en;q=0.9",
      "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="8", "Google Chrome";v="122"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
    });

    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 40000,
    });

    if (response && response.status() === 403) {
      console.error(`🛡️ Cloudflare 403 Block encountered on detail page: ${url}`);
      return {
        companyProfile: "",
        fullDesc: "Cloudflare 403 Security Check Blocked",
        detailError: "403 Forbidden",
      };
    }

    await randomSleep(2500, 4500);

    return await page.evaluate(() => {
      const clean = (value) => (value || "").replace(/\s+/g, " ").trim();

      const pick = (selectors) => {
        for (const selector of selectors) {
          const element = document.querySelector(selector);
          const text = clean(element?.innerText);
          if (text && text.length > 20) return text;
        }
        return "";
      };

      const companyProfile = pick([
        "[class*='company-info']",
        "[class*='aboutCompany']",
        "[class*='companyOverview']",
        "section[class*='company']",
      ]);

      let fullDesc = pick([
        "[class*='job-description']",
        "[class*='jobDescription']",
        "[class*='description']",
        "div[class*='jd']",
        "section[class*='description']",
      ]);

      if (!fullDesc) {
        const main = document.querySelector("main") || document.body;
        const text = clean(main?.innerText);

        const lowerText = text.toLowerCase();
        const marker = lowerText.indexOf("about the job");
        const roleMarker = lowerText.indexOf("about the role");

        const startIdx = marker !== -1 ? marker : roleMarker;

        if (startIdx >= 0) {
          fullDesc = text.slice(startIdx, startIdx + 15000);
        } else if (text.length > 200) {
          fullDesc = text;
        }
      }

      return {
        companyProfile,
        fullDesc: fullDesc || "No detailed JD found.",
      };
    });
  } catch (error) {
    console.error(`⚠️ Detail page failed: ${url} - ${error.message}`);

    return {
      companyProfile: "",
      fullDesc: "",
      detailError: error.message,
    };
  }
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
    const searchPage = await browser.newPage();
    await searchPage.setCacheEnabled(false);
    await searchPage.setUserAgent(USER_AGENT);
    await searchPage.setViewport({ width: 1366, height: 768 });

    const found = new Map();

    try {
      const url = buildSearchUrl(keyword, location);
      let jobs = [];

      try {
        jobs = await scrapeSearchPage(searchPage, url, keyword);
      } catch (error) {
        console.error(
          `❌ Search failed (${keyword} | ${location}): ${error.message}`
        );
      }

      for (const job of jobs) {
        if (!found.has(job.link)) found.set(job.link, job);
      }
    } finally {
      await searchPage.close().catch(() => {});
    }

    const uniqueJobs = Array.from(found.values());

    console.log(
      `🔗 ${keyword} | ${location}: ${uniqueJobs.length} unique Cutshort jobs found. Scraping detailed JDs...`
    );

    const detailedJobs = await Promise.all(
      uniqueJobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();

          try {
            await detailPage.setCacheEnabled(false);
            await detailPage.setUserAgent(USER_AGENT);
            await detailPage.setViewport({ width: 1366, height: 768 });

            const details = await scrapeJobDetail(
              detailPage,
              job.link
            );

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
