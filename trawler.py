import asyncio
import datetime
import sys
import gc
from pyppeteer import launch

async def create_tor_browser():
    browser = await launch({
        'headless': False,
        'args': [
            '--no-sandbox', 
            '--disable-setuid-sandbox',
            '--proxy-server=socks5://127.0.0.1:9150'   Adjust if your Tor is running on a different port
        ]
    })
    return browser

async def search_keywords(content, keywords):
    found_keywords = [kw for kw in keywords if kw in content.lower()]
    return found_keywords

async def extract_table_data(page):
    table_data = []
    try:
        tables = await page.querySelectorAll('table.vtable')
        for table in tables:
            rows = await table.querySelectorAll('tr')
            for row in rows[1:]:  # Skip header
                columns = await row.querySelectorAll('td')
                if len(columns) >= 3:
                    vendor_name = await page.evaluate('(element) => element.textContent', columns[0])
                    ship_from = await page.evaluate('(element) => element.textContent', columns[1])
                    ship_to = await page.evaluate('(element) => element.textContent', columns[2])
                    table_data.append((vendor_name.strip(), ship_from.strip(), ship_to.strip()))
    except Exception as e:
        print(f"Error extracting table: {e}")
    return table_data

async def crawl(url, keywords):
    browser = await create_tor_browser()
    page = await browser.newPage()
    
    try:
        await page.goto(url, {'waitUntil': 'load', 'timeout': 60000})  
        content = await page.content()

        # Log the date the data was retrieved
        date_retrieved = datetime.datetime.now().isoformat()
        found_keywords = await search_keywords(content, keywords)

        if found_keywords:
            print(f'Date Retrieved: {date_retrieved} | Keywords: {found_keywords} | URL: {url}')
            table_data = await extract_table_data(page)
            for vendor, from_location, to_location in table_data:
                print(f'  Vendor: {vendor}, Ship From: {from_location}, Ship To: {to_location}')
        
        # Optionally write to a file or a database here instead of holding all data in memory

    except Exception as e:
        print(f'Error processing {url}: {e}')
        
    finally:
        await page.close()  # Explicitly close the page to free resources
        await browser.close()  # Explicitly close the browser to free resources
        gc.collect()  # Invoke garbage collection

# Entry point
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <url>")
        sys.exit(1)

    starting_url = sys.argv[1]  # URL passed from the command line
    keywords = ['precursors', 'safroe', 'fentanyl', 'cocaine']

    asyncio.get_event_loop().run_until_complete(crawl(starting_url, keywords))
