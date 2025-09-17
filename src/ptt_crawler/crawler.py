# src/ptt_crawler/crawler.py
"""
Crawler module for scraping PTT sub-forumns.

This module scrapes the KMT and DPP sub-forums on PTT, extracts article
metadata, and saves the data to CSV files.
"""

__all__ = ["crawl"]

import random
import time
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Any

import pandas as pd
import regex as re
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from .config.config import load_headers

logger = getLogger(__name__)


MAIN_URL = "https://www.ptt.cc/"


def crawl(subforum: str):
    entry_url = f"{MAIN_URL}bbs/{subforum}"
    init_page = _get_soup_safe(entry_url, max_retries=3)
    # The "last page" link from the index page leads to the final page
    # the sub-forum
    final_page_url = None
    if not init_page:
        return None
    for block in init_page.find_all("a", class_="btn wide"):
        if "上頁" in block.text:
            final_page_url = block.get("href", None)
            break
    else:
        logger.warning("No '上頁' link found in the index page. Exiting scraping...")
        return None

    if final_page_url is None:
        logger.warning(
            "The '上頁' text exists but not the associated link. Exiting scraping..."
        )
        return None

    total_pages = _extract_total_pages(final_page_url=final_page_url, subforum=subforum)

    logger.info("All necessary information extracted proceed to scrape data...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        content_urls = list(
            executor.map(
                lambda page_num: _get_all_content_urls(page_num, entry_url),
                range(1, total_pages + 1),
            )
        )
    content_urls = [
        url
        for sublist in content_urls
        if sublist is not None
        for url in sublist
        if url is not None
    ]

    if not content_urls:
        logger.warning("No page URLs collected")
        return None
    logger.debug(f"Collected {len(content_urls)} content page URLs")
    logger.info(f"Initiating scraping of {len(content_urls)} content pages")

    with ThreadPoolExecutor(max_workers=20) as executor:
        data = list(executor.map(lambda url: _get_content(url, subforum), content_urls))
    data = [item for item in data if item is not None]
    if not data:
        logger.warning("No data extracted")
        return None
    df = pd.DataFrame(data)
    return df


def _extract_total_pages(final_page_url: str, subforum: str) -> int | None:
    match = re.search(r"(?<=index)\d+(?=\.html)", final_page_url)
    if not match:
        logger.warning(
            "Cannot find total_pages from final_page_url. Exiting scraping..."
        )
        return None

    try:
        total_pages = int(match.group(0))
    except Exception as e:
        logger.exception(
            f"Unexpected exception parsing total_pages for {subforum}: {e}"
        )
        return None

    if total_pages <= 0:
        logger.warning("Insufficient pages scraped, exiting scraping...")
        return None

    return total_pages


def _get_all_content_urls(page_num: int, entry_url: str):
    bulletin_url = f"{entry_url}/index{page_num}.html"
    all_urls = []
    bulletin_page = _get_soup_safe(bulletin_url)
    if not bulletin_page or not isinstance(bulletin_page, BeautifulSoup):
        return None
    for block in bulletin_page.find_all("div", class_="title"):
        try:
            page_endpoint = block.find("a").get("href", None)
            if page_endpoint is not None:
                full_url = f"{MAIN_URL}{page_endpoint}"
                all_urls.append(full_url)
            else:
                logger.warning(f"href not found in {block}")
                continue
        except Exception as e:
            logger.exception(f"Error processing url in page {page_num}: {e}")
            continue
    return all_urls


def _get_soup(url: str, max_retries: int = 3) -> BeautifulSoup:
    headers = load_headers()

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except requests.exceptions.HTTPError as e:
            error_code = 404
            if response.status_code == error_code:
                logger.error(f"404 Not Found for {url}, skipping retries")
                raise e
            # For other HTTP errors, continue with retry logic
            if attempt < max_retries - 1:
                logger.warning(f"HTTP error for {url}, attempt {attempt + 1}: {e}")
                time.sleep(random.uniform(2, 4))
                continue
            logger.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
            raise e
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Request failed for {url}, attempt {attempt + 1}: {e}")
                time.sleep(random.uniform(2, 4))
                continue
            logger.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
            raise e


def _get_soup_safe(url: str, max_retries: int = 3) -> BeautifulSoup | None:
    """Safely extract the BS content with the `_get_soup` function"""
    try:
        return _get_soup(url=url, max_retries=max_retries)
    except requests.exceptions.HTTPError as e:
        error_code = 404
        if e.response.status_code == error_code:
            logger.error(f"404 Not Found for {url}, skipping")
        else:
            logger.exception(f"HTTP error for {url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request error for {url}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error parsing {url}: {e}")
        return None


def _get_content(page_url: str, subforum: str):
    """Extract content from a PTT page."""
    logger.debug(f"Processing page: {page_url}")

    # Guard against pages that deviates from the standard div structure
    # (e.g. pages with media embeddings)
    content_page = _get_soup_safe(url=page_url)

    if content_page is not None:
        output = _process_content(
            page_url=page_url, subforum=subforum, bs_content=content_page
        )
        if output and _validate_contents(output):
            return output

    return None


def _process_content(page_url: str, subforum: str, bs_content: BeautifulSoup) -> dict:
    """Process BS content from the raw scraping results."""
    # Initialize variables to prevent NameError
    title = None
    time = None
    author = None
    main_content = bs_content.find("div", id="main-content")
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

    comments, comment_divs = _get_comments(main_content)
    if not comments:
        logger.debug(f"No comments found for {page_url}")

    span_to_remove = (
        comment_divs
        + article_metaline
        + main_content.find_all("span", class_="f2")
        + main_content.find_all("span", class_="f6")
    )
    for span in span_to_remove:
        span.decompose()
    content_text = main_content.text.strip()
    logger.debug(f"{page_url} parsed successfully. Content length: {len(content_text)}")
    return {
        "author": author,
        "title": title,
        "time": time,
        "content": content_text,
        "url": page_url,
        "subforum": subforum,
        "comments": comments,
    }


def _validate_contents(contents: dict) -> bool:
    title = contents.get("title")
    ts = contents.get("time")
    page_url = contents.get("url")
    content_text = contents.get("content") or ""

    # Benchmark content length
    benchmark_len = 10

    if title is None or ts is None:
        logger.warning(f"Missing title or time in {page_url}")
        return False
    if any(k in title for k in ["公告", "新聞", "轉錄", "轉載"]):
        logger.debug(f"Page is announcement, news or repost: {page_url}")
        return False
    if len(content_text) < benchmark_len:
        logger.warning(f"Insufficient content length in {page_url}")
        return False
    if any(k in content_text for k in ["轉錄", "轉載"]):
        logger.debug(f"{page_url} is, mentions or contains a repost.")
        return False
    return True


def _get_comments(main_content: Tag) -> tuple[list[dict[str, Any]], list[Tag]]:
    comments_data: list[dict[str, Any]] = []
    comment_divs: list[Tag] = []
    for element in main_content.find_all("div", class_="push"):
        comment_divs.append(element)
        push_tag = element.find("span", class_="push-tag")
        userid = element.find("span", class_="push-userid")
        content = element.find("span", class_="push-content")
        ipdatetime = element.find("span", class_="push-ipdatetime")

        if userid and ipdatetime:
            comments_data.append(
                {
                    "push_tag": push_tag.text.strip() if push_tag else None,
                    "userid": userid.text.strip(),
                    "content": (content.text or "").strip() if content else None,
                    "ipdatetime": ipdatetime.text.strip(),
                }
            )
        else:
            logger.warning(
                "Missing userid or ipdatetime in comment; skipping one comment."
            )
    return comments_data, comment_divs
