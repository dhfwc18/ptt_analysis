# ptt_crawler.py

# External imports
import requests
from bs4 import BeautifulSoup
import regex as re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import random

# Internal imports
from config.config import load_headers

# Logging setup
from config.logging_config import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

MAIN_URL = "https://www.ptt.cc/"

def _get_and_open(url: str, max_retries: int = 3) -> BeautifulSoup:
    headers = load_headers()

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Request failed for {url}, attempt {attempt + 1}: {e}")
                time.sleep(random.uniform(2, 5))
                continue
            logger.error(
                f"Failed to fetch {url} after {max_retries} attempts: {e}")
            raise e

def _get_content(page_url: str, party: str):
    # Initialize variables to prevent NameError
    title = None
    time = None

    # Guard against pages that deviates from the standard div structure
    # (e.g. pages with media embeddings)
    try:
        content_page = _get_and_open(page_url)
        content_block = content_page.find(
            "div", class_="bbs-screen bbs-content")

        if not content_block:
            logger.warning(f"No content block found in {page_url}")
            return None

        for div in content_block.find_all("div"):
            try:
                div_class = div.get("class", None)
                if not isinstance(div_class, list) or not div_class:
                    continue
                else:
                    first_div_class = div_class[0]
                    if not isinstance(first_div_class, str):
                        continue

                if first_div_class == "article-metaline":
                    info = div.find("span", class_="article-meta-value")
                    if not info:
                        continue

                    information_text = info.text
                    if "標題" in div.text:
                        title = information_text
                        if title and "公告" in title:
                            logger.debug(f"Page is announcement: {page_url}")
                            return None
                    elif "時間" in div.text:
                        time = information_text

                div.decompose()

            except Exception as e:
                logger.exception(
                    f"Unexpected error parsing a div in {page_url}: {e}")
                div.decompose()
                continue

        # Remove unwanted spans
        for span in content_block.find_all("span", class_="f2"):
            if ":" in span.text or "：" in span.text:
                span.decompose()

        for span in content_block.find_all("span", class_="f6"):
            span.decompose()

        # NOW extract the text after all processing
        text = content_block.text.strip() if content_block else ""

        if title is None or time is None:
            return None

        return {
            "title": title,
            "time": time,
            "content": text,
            "party": party
        }

    except Exception as e:
        logger.exception(f"Unexpected error parsing {page_url}: {e}")
        return None

def crawl(party):
    if not party or not isinstance(party, str):
        logger.error("Invalid party parameter")
        return None

    entry_url = f"{MAIN_URL}/bbs/{party}"
    try:
        init_page = _get_and_open(entry_url)
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request error for {entry_url}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Parsing error for {entry_url}: {e}")
        return None

    # The "last page" link from the index page leads to the final page
    # the sub-forum
    final_page_url = None
    for block in init_page.find_all("a", class_="btn wide"):
        if "上頁" in block.text:
            final_page_url = block.get("href", None)
            break
    else:
        logger.warning(
            "No '上頁' link found in the index page. Exiting scraping..."
        )
        return None

    if final_page_url is None:
        logger.warning(
            "The '上頁' text exists but not the associated link."
            " Exiting scraping..."
            )
        return None

    match = re.search(r"(?<=index)\d+(?=\.html)", final_page_url)
    if match:
        total_pages = match.group(0)
        logger.debug(f"Total pages: {total_pages}")
    else:
        logger.warning(
            "Cannot find total_pages from final_page_url. Exiting scraping..."
        )
        return None

    if not isinstance(total_pages, int):
        try:
            total_pages = int(total_pages)
        except Exception as e:
            logger.exception(
                f"Unexpected exception parsing total_pages for {party}: {e}"
            )
            return None

    if not total_pages:
        logger.warning(
            "Total pages cannot be extracted from the final page url."
            " Exiting scraping..."
            )
        return None

    logger.info(
        "All necessary information extracted proceed to scrape data..."
        )
    logger.debug(f"Initiating scraping of {total_pages} pages")
    all_page_urls = []
    for i in range(1, total_pages + 1):
        bulletin_url = f"{entry_url}/index{i}.html"
        try:
            bulletin_page = _get_and_open(bulletin_url)
            for block in bulletin_page.find_all("div", class_ = "title"):
                try:
                    page_endpoint = block.find("a").get("href", None)
                    if page_endpoint is not None:
                        full_url = f"{MAIN_URL}{page_endpoint}"
                        all_page_urls.append(full_url)
                    else:
                        logger.warning(f"href not found in {block}")
                        continue
                except Exception as e:
                    logger.exception(
                        f"Unexpected error processing item in page {i}: {e}"
                    )
                    continue
        except Exception as e:
            logger.exception(f"Error processing page {i}: {e}")
            continue

    if not all_page_urls:
        logger.warning("No page URLs collected")
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        data = list(
            executor.map(lambda url: _get_content(url, party), all_page_urls)
        )
        data = [item for item in data if item is not None]
        if not data:
            logger.warning("No data extracted")
            return None
        df = pd.DataFrame(data)
        return df

if __name__ == "__main__":
    crawl("KMT")