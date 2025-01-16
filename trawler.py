import asyncio
import sys
import re
from datetime import datetime
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pyppeteer import launch
from bs4 import BeautifulSoup

# Define patterns for drug-related keywords
drug_keywords = {
    "precursor": re.compile(r"\bprecursor\b", re.IGNORECASE),
    "heroin": re.compile(r"\bheroin\b", re.IGNORECASE),
    "cocaine": re.compile(r"\bcocaine\b", re.IGNORECASE),
    
}

# SQLAlchemy setup
Base = declarative_base()

class DrugKeyword(Base):
    __tablename__ = 'drug_keywords'
    id = db.Column(db.Integer, primary_key=True)
    source_url = db.Column(db.String, nullable=False)
    keyword_name = db.Column(db.String, nullable=False)
    time_found = db.Column(db.DateTime, default=datetime.utcnow)

class PageLog(Base):
    __tablename__ = 'pagelog'
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    error = db.Column(db.String, nullable=True)
    time_logged = db.Column(db.DateTime, default=datetime.utcnow)

# Initialize database engine
engine = db.create_engine('sqlite:///drugKeywords.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

async def extract_keywords(page, source_url):
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')
    found_keywords = []

    # Check for drug-related keywords
    for keyword, pattern in drug_keywords.items():
        if pattern.search(str(soup)):
            kw_record = DrugKeyword(source_url=source_url, keyword_name=keyword)
            found_keywords.append(kw_record)

    return found_keywords

async def log_page(url, status, error=None):
    with Session() as session:
        page_log = PageLog(url=url, status=status, error=error)
        session.add(page_log)
        session.commit()

async def crawl(url, depth, visited, semaphore):
    if depth == 0 or url in visited:
        return []

    visited.add(url)
    async with semaphore:
        browser = await launch(headless=True,
                               args=['--no-sandbox',
                                     '--disable-setuid-sandbox',
                                     '--proxy-server=socks5://127.0.0.1:9050'])
        page = await browser.newPage()
        try:
            print(f"Visiting: {url}")
            await page.goto(url, timeout=60000)
            found_keywords = await extract_keywords(page, url)
            if found_keywords:
                with Session() as session:
                    for kw_record in found_keywords:
                        session.add(kw_record)
                    await log_page(url, 'success')
                    session.commit()
            else:
                await log_page(url, 'no_keywords')

            links = await page.evaluate('''() => Array.from(document.links).map(link => link.href)''')
            print(f"Finished: {url}")
            return [{"url": url, "keywords": found_keywords, "links": links}]
        except Exception as e:
            print(f"Failed to crawl {url}: {e}")
            await log_page(url, 'failed', str(e))
            return []
        finally:
            await page.close()
            await browser.close()

async def main(seed_url, depth):
    to_crawl = [(seed_url, depth)]
    crawled = []
    visited = set()
    semaphore = asyncio.Semaphore(4)  # Limit concurrent tasks to 4

    while to_crawl:
        current_url, current_depth = to_crawl.pop()
        crawl_result = await crawl(current_url, current_depth, visited, semaphore)
        if crawl_result:
            crawled.append(crawl_result[0])
            if current_depth > 1:
                to_crawl.extend([(link, current_depth - 1) for link in crawl_result[0]['links']])

    for result in crawled:
        print(f"URL: {result['url']}")
        for kw_record in result['keywords']:
            print(f"  Found keyword {kw_record.keyword_name} at {kw_record.time_found}")
        print(f"  Links found on page: {len(result['links'])}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 drugKeywordCrawler.py <seed_url> [<depth>]")
        sys.exit(1)
    seed_url = sys.argv[1]
    depth = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    asyncio.get_event_loop().run_until_complete(main(seed_url, depth))
