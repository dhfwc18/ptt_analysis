# pipeline/ptt_crawler.py
"""
Crawler module for scraping PTT sub-forumns.

This module scrapes the KMT and DPP sub-forums on PTT,
extracts article metadata, and saves the data to CSV files.
"""

__all__ = ["crawl"]

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

# Get logger
from config.logging_config import get_logger

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
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.error(f"404 Not Found for {url}, skipping retries")
                raise e
            # For other HTTP errors, continue with retry logic
            if attempt < max_retries - 1:
                logger.warning(
                    f"HTTP error for {url}, attempt {attempt + 1}: {e}")
                time.sleep(random.uniform(2, 4))
                continue
            logger.error(
                f"Failed to fetch {url} after {max_retries} attempts: {e}")
            raise e
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Request failed for {url}, attempt {attempt + 1}: {e}")
                time.sleep(random.uniform(2, 4))
                continue
            logger.error(
                f"Failed to fetch {url} after {max_retries} attempts: {e}")
            raise e

def _get_content(page_url: str, sub_forum: str):
    """Extract content from a PTT page."""
    logger.debug(f"Processing page: {page_url}")
    # Initialize variables to prevent NameError
    title = None
    time = None
    author = None

    # Guard against pages that deviates from the standard div structure
    # (e.g. pages with media embeddings)
    try:
        content_page = _get_and_open(page_url)
        main_content = content_page.find("div", id="main-content")
        if not main_content:
            logger.warning(f"No main content found in {page_url}")
            return None
        article_metaline = main_content.find_all("div", class_="article-metaline")
        for metaline in article_metaline:
            tag_span = metaline.find("span", class_="article-meta-tag")
            value_span = metaline.find("span", class_="article-meta-value")
            if tag_span and value_span:
                tag_text = tag_span.text.strip()
                value_text = value_span.text.strip()
                match tag_text:
                    case "標題":
                        title = value_text
                    case "時間":
                        time = value_text
                    case "作者":
                        author = value_text
                    case _:
                        continue
            else:
                continue
        if title is None or time is None:
            logger.warning(f"Missing title or time in {page_url}")
            return None
        if "公告" in title:
            logger.debug(f"Page is announcement: {page_url}")
            return None
        span_to_remove = (
            article_metaline
            + main_content.find_all("span", class_="f2")
            + main_content.find_all("span", class_="f6")
        )
        for span in span_to_remove:
            span.decompose()
        content_text = main_content.text.strip()
        if len(content_text) < 10:
            logger.warning(f"Insufficient content length in {page_url}")
            return None
        logger.debug(
            f"{page_url} parsed successfully."
            f" Content length: {len(content_text)}"
        )
        return {
            "author": author,
            "title": title,
            "time": time,
            "content": content_text,
            "url": page_url,
            "sub_forum": sub_forum
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"404 Not Found for {page_url}, skipping")
            return None
        logger.exception(f"HTTP error for {page_url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request error for {page_url}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error parsing {page_url}: {e}")
        return None

def _get_all_content_urls(page_num: int, entry_url: str):
    bulletin_url = f"{entry_url}/index{page_num}.html"
    all_urls = list()
    try:
        bulletin_page = _get_and_open(bulletin_url)
        for block in bulletin_page.find_all("div", class_ = "title"):
            try:
                page_endpoint = block.find("a").get("href", None)
                if page_endpoint is not None:
                    full_url = f"{MAIN_URL}{page_endpoint}"
                    all_urls.append(full_url)
                else:
                    logger.warning(f"href not found in {block}")
                    continue
            except Exception as e:
                logger.exception(
                    f"Error processing url in page {page_num}: {e}"
                )
                continue
        return all_urls
    except Exception as e:
        logger.exception(f"Error processing page {page_num}: {e}")
        return None

def crawl(sub_forum):
    if not sub_forum or not isinstance(sub_forum, str):
        logger.error("Invalid sub_forum parameter")
        return None

    entry_url = f"{MAIN_URL}bbs/{sub_forum}"
    try:
        init_page = _get_and_open(entry_url, max_retries=3)
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
                f"Unexpected exception parsing total_pages for {sub_forum}: {e}"
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
    with ThreadPoolExecutor(max_workers=20) as executor:
        content_urls = list(
            executor.map(
                lambda page_num: _get_all_content_urls(page_num, entry_url),
                range(1, total_pages + 1)
            )
        )
    content_urls = [
        url for sublist in content_urls if sublist is not None
        for url in sublist if url is not None
    ]

    if not content_urls:
        logger.warning("No page URLs collected")
        return None
    logger.debug(f"Collected {len(content_urls)} content page URLs")
    logger.info(f"Initiating scraping of {len(content_urls)} content pages")

    with ThreadPoolExecutor(max_workers=20) as executor:
        data = list(
            executor.map(lambda url: _get_content(url, sub_forum), content_urls)
        )
        data = [item for item in data if item is not None]
        if not data:
            logger.warning("No data extracted")
            return None
        df = pd.DataFrame(data)
        return df