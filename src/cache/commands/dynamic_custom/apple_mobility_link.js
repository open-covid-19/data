const fs = require('fs').promises;
const puppeteer = require('puppeteer');

// Process arguments
const args = require('yargs').parse();

(async () => {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    // Wait until the page has fully loaded
    await page.goto(args.url, { waitUntil: 'networkidle2' }).catch(console.error);

    // Get the a.href that points to a CSV file and output that
    const reportLink =
        (await page.$$eval('a', a => a.map(a => a.href))).filter(x => x.indexOf('csv') !== -1)[0];
    await fs.writeFile(args.output, reportLink);

    await browser.close();
})();