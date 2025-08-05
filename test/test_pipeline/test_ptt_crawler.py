# test/test_pipeline/test_ptt_crawler.py
"""Unit tests for the PTT Crawler functions."""

# External imports
import unittest
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from unittest.mock import patch, Mock

# Import the tested functions
from pipeline.ptt_crawler import (
    _get_and_open, _get_content, _get_comments, crawl, _get_all_content_urls
)

MOCK_HTML_PATH = Path(__file__).parent / "mock_html/mock_ptt_page.html"


class TestPttCrawler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Disable logging for all tests in this class."""
        logging.disable(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls):
        """Re-enable logging after tests complete."""
        logging.disable(logging.NOTSET)

    def setUp(self):
        with open(MOCK_HTML_PATH, "r", encoding="utf-8") as file:
            self.mock_html = file.read()
        self.sleep_patcher = patch('time.sleep')
        self.mock_sleep = self.sleep_patcher.start()

    def tearDown(self):
        self.sleep_patcher.stop()

    @patch("requests.get")
    def test_get_and_open_success(self, mock_get):
        mock_response = Mock()
        mock_response.content = self.mock_html
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = _get_and_open("https://test.url")
        self.assertIsInstance(result, BeautifulSoup)

    @patch("requests.get")
    def test_get_and_open_retry_on_error(self, mock_get):
        mock_get.side_effect = [
            requests.exceptions.RequestException,
            Mock(content=self.mock_html, status_code=200)
        ]

        result = _get_and_open("https://test.url")
        self.assertIsInstance(result, BeautifulSoup)
        self.assertEqual(mock_get.call_count, 2)

    def test_get_content_invalid_url(self):
        result = _get_content("invalid_url", "test_forum")
        self.assertIsNone(result)

    @patch("pipeline.ptt_crawler._get_and_open")
    def test_get_all_content_urls(self, mock_get_and_open):
        mock_soup = BeautifulSoup("""
            <div class="title">
                <a href="/test/page1.html">Test 1</a>
            </div>
            <div class="title">
                <a href="/test/page2.html">Test 2</a>
            </div>
        """, "html.parser")
        mock_get_and_open.return_value = mock_soup

        urls = _get_all_content_urls(1, "https://test.url")
        self.assertEqual(len(urls), 2)
        self.assertTrue(
            all(url.startswith("https://www.ptt.cc/") for url in urls)
        )

    def test_get_comments_empty(self):
        soup = BeautifulSoup("<div></div>", "html.parser")
        comments, divs = _get_comments(soup)
        self.assertEqual(len(comments), 0)
        self.assertEqual(len(divs), 0)

    def test_crawl_invalid_subforum(self):
        result = crawl("")
        self.assertIsNone(result)
        result = crawl(None)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()