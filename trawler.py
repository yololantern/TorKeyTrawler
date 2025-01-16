import gc
import datetime
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuration for Firefox to route through Tor
def create_tor_browser():
    options = Options()
    options.headless = True  # Set to False for visible browser
    profile = webdriver.FirefoxProfile()

    # Set the Tor proxy
    profile.set_preference("network.proxy.type", 1)
    profile.set_preference("network.proxy.socks", "127.0.0.1")
    profile.set_preference("network.proxy.socks_port", 9150)  # Adjust as necessary
    profile.set_preference("network.proxy.socks_remote_dns", True)

    service = FirefoxService(executable_path='path/to/geckodriver')  # Adjust path to geckodriver
    return webdriver.Firefox(service=service, options=options, firefox_profile=profile)

# Function to search for keywords in the content
def search_keywords(content, keywords):
    found_keywords = [kw for kw in keywords if kw in content.lower()]
    return found_keywords

# Function to extract table data
def extract_table_data(browser):
    table_data = []
    try:
        tables = browser.find_elements(By.CLASS_NAME, 'vtable')
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')[1:]  # Skip header
            for row in rows:
                columns = row.find_elements(By.TAG_NAME, 'td')
                if len(columns) >= 3:
                    vendor_name = columns[0].text.strip()
                    ship_from = columns[1].text.strip()
                    ship_to = columns[2].text.strip()
                    table_data.append((vendor_name, ship_from, ship_to))
    except Exception as e:
        print(f"Error extracting table: {e}")
    return table_data

# Main crawling function with memory management
def crawl(url, keywords, depth, visited, browser):
    if depth == 0 or url in visited:
        return

    visited.add(url)
    try:
        browser.get(url)

        # Wait for page to fully load
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        content = browser.page_source

        date_retrieved = datetime.datetime.now().isoformat()
        found_keywords = search_keywords(content, keywords)

        if found_keywords:
            print(f'Date Retrieved: {date_retrieved} | Keywords: {found_keywords} | URL: {url}')
            table_data = extract_table_data(browser)
            for vendor, from_location, to_location in table_data:
                print(f'  Vendor: {vendor}, Ship From: {from_location}, Ship To: {to_location}')
        
        # Find links to crawl further
        links = browser.find_elements(By.TAG_NAME, 'a')
        for link in links:
            link_url = link.get_attribute('href')
            if link_url and link_url.startswith('http'):
                crawl(link_url, keywords, depth-1, visited, browser)

    except Exception as e:
        print(f'Error processing {url}: {e}')

# Entry point
if __name__ == "__main__":
    starting_url = 'http://example.onion'  # Replace with the actual starting URL
    keywords = ['precursors', 'safroe', 'fentanyl']
    depth = 2
    visited = set()
    
    # Only create one instance of the browser to reuse
    browser = create_tor_browser()

    try:
        crawl(starting_url, keywords, depth, visited, browser)
    finally:
        browser.quit()  # Ensure the browser closes properly

    # Manual garbage collection
    gc.collect()
