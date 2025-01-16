import asyncio
import datetime
import sys
import sqlite3
import gc  # To invoke garbage collection
from pyppeteer import launch

# Database setup function
def setup_database():
    conn = sqlite3.connect('scrape_data.db')  # Database file
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrape_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_retrieved TEXT,
            url TEXT,
            vendor_name TEXT,
            ship_from TEXT,
            ship_to TEXT,
            keywords TEXT
        )
    ''')
    conn.commit()
    return conn, cursor

async def create_tor_browser():
    browser = await launch({
        'headless': False, 
        'args': [
            '--no-sandbox', 
            '--disable-setuid-sandbox',
            '--proxy-server=socks5://127.0.0.1:9150' 
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

async def crawl(url, keywords, cursor):
    browser = await create_tor_browser()
    page = await browser.newPage()
    
    try:
        await page.goto(url, {'waitUntil': 'load', 'timeout': 60000})  # Wait until the page loads
        content = await page.content()

        # Log the date the data was retrieved
        date_retrieved = datetime.datetime.now().isoformat()
        found_keywords = await search_keywords(content, keywords)

        if found_keywords:
            print(f'Date Retrieved: {date_retrieved} | Keywords: {found_keywords} | URL: {url}')
            table_data = await extract_table_data(page)
            for vendor, from_location, to_location in table_data:
                print(f'  Vendor: {vendor}, Ship From: {from_location}, Ship To: {to_location}')
                # Insert data into database
                cursor.execute('''
                    INSERT INTO scrape_results (date_retrieved, url, vendor_name, ship_from, ship_to, keywords)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (date_retrieved, url, vendor, from_location, to_location, ', '.join(found_keywords)))
    
    except Exception as e:
        print(f'Error processing {url}: {e}')
        
    finally:
        await page.close()  # Close the page explicitly, but keep the browser open
        gc.collect()  # Invoke garbage collection


# Entry point
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <url>")
        sys.exit(1)

    starting_url = sys.argv[1]  # URL passed from the command line
    keywords = ['precursors', 'safroe', 'fentanyl']

    # Setup the database
    conn, cursor = setup_database()

    try:
        asyncio.get_event_loop().run_until_complete(crawl(starting_url, keywords, cursor))
    finally:
        cursor.close()
        conn.commit()  # Commit changes before closing the connection
        conn.close()  # Ensure the database connection is closed
