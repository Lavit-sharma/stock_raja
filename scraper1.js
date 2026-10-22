
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 2;
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const LOCATION_SLUGS = {
  delhi: "new-delhi",
  bengaluru: "bangalore",
  bangalore: "bangalore",
  mumbai: "mumbai",
  hyderabad: "hyderabad",
};

// Map search terms to supported Wellfound role pages.
// edge-ai is approximated using the AI engineering role page.
const ROLE_SLUGS = {
  "product-manager": "product-manager",
  "engineering-manager": "engineering-manager",
  "edge-ai": "artificial-intelligence-engineer",
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function buildSearchUrl(keyword, location, pageNumber = 1) {
  const role = ROLE_SLUGS[keyword];
  const place = LOCATION_SLUGS[location];

  if (!role) {
    throw new Error(`No Wellfound role mapping for: ${keyword}`);
  }

  if (!place) {
    throw new Error(`No Wellfound location mapping for: ${location}`);
  }

  const base = `https://wellfound.com/role/l/${role}/${place}`;

  return pageNumber > 1
    ? `${base}?page=${pageNumber}`
    : base;
}

async function scrapeSearchPage(page, url, keyword) {
  console.log(`🔍 Opening: ${url}`);

  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });

  console.log(
    `🌐 HTTP: ${response?.status() ?? "No response"} | ${page.url()}`
  );

  await sleep(2500);

  const result = await page.evaluate(() => {
    const clean = (value) =>
      (value || "").replace(/\s+/g, " ").trim();

    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const output = [];

    for (const anchor of anchors) {
      const link = anchor.href;

      // Wellfound individual job URLs use /jobs/<job-id>-<slug>.
      if (!/^https:\/\/wellfound\.com\/jobs\/[^/?#]+/i.test(link)) {
        continue;
      }

      const title = clean(anchor.innerText);

      if (!title || title.length > 180) continue;

      // Find the closest ancestor that resembles a job card.
      let card = anchor;
      let cardText = "";

      for (let level = 0; level < 7 && card; level++) {
        const text = clean(card.innerText);

        if (
          text.length >= title.length &&
          text.length < 3500 &&
          (/exp\b|full.?time|part.?time|remote|in office|salary|equity/i.test(text))
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

      const company = headings.find(
        (heading) => heading !== title
      ) || "";

      const experienceMatch = cardText.match(
        /\b\d+\+?\s+years?\s+of\s+exp(?:erience)?\b/i
      );

      const locationMatch = cardText.match(
        /(?:Remote only|Remote|In office|Hybrid)\s*[•·-]?\s*([^•·\n]{2,100})/i
      );

      output.push({
        title,
        company,
        location: locationMatch
          ? clean(locationMatch[0])
          : "",
        experience: experienceMatch
          ? experienceMatch[0]
          : "",
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

  // For edge-ai, keep AI/edge-related engineering titles.
  const filtered = keyword === "edge-ai"
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
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 40000,
    });

    await sleep(1200);

    return await page.evaluate(() => {
      const clean = (value) =>
        (value || "").replace(/\s+/g, " ").trim();

      const pick = (selectors) => {
        for (const selector of selectors) {
          const element = document.querySelector(selector);
          const text = clean(element?.innerText);

          if (text) return text;
        }

        return "";
      };

      const companyProfile = pick([
        "[data-test='CompanyProfile']",
        "[class*='companyOverview']",
        "[class*='company_description']",
        "[class*='aboutCompany']",
      ]);

      let fullDesc = pick([
        "[data-test='JobDescription']",
        "[class*='jobDescription']",
        "[class*='job-description']",
        "[class*='descriptionWrapper']",
        "[class*='description-wrapper']",
      ]);

      // Fallback: use the main content if CSS classes change.
      if (!fullDesc) {
        const main = document.querySelector("main");
        const text = clean(main?.innerText);

        const marker = text.toLowerCase().indexOf("about the job");

        if (marker >= 0) {
          fullDesc = text.slice(marker, marker + 15000);
        } else if (text.length > 300) {
          fullDesc = text.slice(0, 15000);
        }
      }

      if (!fullDesc) {
        fullDesc = pick(["main", "article"]);
      }

      return { companyProfile, fullDesc };
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

async function scrapeJobs(keyword, location, pages = 2) {
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
    ],
  });

  try {
    const searchPage = await browser.newPage();
    await searchPage.setUserAgent(USER_AGENT);

    const found = new Map();

    try {
      for (let pageNumber = 1; pageNumber <= pages; pageNumber++) {
        const url = buildSearchUrl(keyword, location, pageNumber);

        let jobs = [];

        try {
          jobs = await scrapeSearchPage(searchPage, url, keyword);
        } catch (error) {
          console.error(
            `❌ Search failed (${keyword} | ${location} | page ${pageNumber}): ${error.message}`
          );
          break;
        }

        for (const job of jobs) {
          if (!found.has(job.link)) found.set(job.link, job);
        }

        if (jobs.length === 0) break;

        await sleep(1200);
      }
    } finally {
      await searchPage.close().catch(() => {});
    }

    const uniqueJobs = Array.from(found.values());

    console.log(
      `🔗 ${keyword} | ${location}: ${uniqueJobs.length} unique jobs`
    );

    const detailedJobs = await Promise.all(
      uniqueJobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();

          try {
            await detailPage.setUserAgent(USER_AGENT);

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
