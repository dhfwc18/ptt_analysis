# test/test_pipeline/test_tokeniser.py
"""Unit tests for the tokenisation functionality."""

import unittest
import pandas as pd
from pipeline.tokeniser import tokenise, tokenise_dataframe, PttTextTokeniser

class TestTokeniser(unittest.TestCase):
    def setUp(self):
        self.tokeniser = PttTextTokeniser()
        self.test_df = pd.DataFrame({
            "text": ["This is test text", "Another test text"]
        })

    def test_tokenise_basic(self):
        text = "Hello World"
        tokens = tokenise(text)
        self.assertIsInstance(tokens, list)
        self.assertEqual(tokens, ["Hello", "World"])

    def test_tokenise_empty_string(self):
        text = ""
        tokens = tokenise(text)
        self.assertEqual(tokens, [])

    def test_tokenise_invalid_input(self):
        with self.assertRaises(ValueError):
            tokenise(123)

    def test_tokenise_dataframe(self):
        result_df = tokenise_dataframe(self.test_df, "text")
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertTrue("text" in result_df.columns)
        self.assertIsInstance(result_df["text"].iloc[0], list)

    def test_tokenise_dataframe_invalid_column(self):
        with self.assertRaises(ValueError):
            tokenise_dataframe(self.test_df, "invalid_column")

    def test_ptt_tokeniser_basic_cleaning(self):
        text = "https://test.com\n※ [本文轉錄自test]\n作者:test\n看板:test"
        cleaned = self.tokeniser._basic_cleaning(text)
        self.assertNotIn("https://", cleaned)
        self.assertNotIn("本文轉錄自", cleaned)
        self.assertNotIn("作者:", cleaned)
        self.assertNotIn("看板:", cleaned)

    def test_ptt_tokeniser_remove_punct(self):
        tokens = ["Hello", ".", "，", "World", "!"]
        cleaned_tokens = self.tokeniser._remove_punct(tokens)
        self.assertEqual(cleaned_tokens, ["Hello", "World"])

    def test_ptt_tokeniser_tokenise_text(self):
        text = "Hello World! 你好世界。"
        tokens = self.tokeniser.tokenise_text(text, remove_punctuation=True)
        self.assertIsInstance(tokens, list)
        self.assertNotIn("!", tokens)
        self.assertNotIn("。", tokens)

    def test_ptt_tokeniser_tokenise_dataframe_column(self):
        result_df = self.tokeniser.tokenise_dataframe_column(
            self.test_df,
            "text",
            remove_punctuation=True
        )
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertTrue("text" in result_df.columns)
        self.assertIsInstance(result_df["text"].iloc[0], list)

if __name__ == "__main__":
    unittest.main()