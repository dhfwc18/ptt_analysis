# pipeline/tokeniser.py
"""Tokeniser module for PTT data."""

__all__ = ["tokenise", "tokenise_dataframe", "PttTextTokeniser"]

# Internal imports
from config.config import JIEBA_DICT_PATH

# External imports
import jieba
import pandas as pd
import re
import string
import validators


def tokenise(text: str) -> list:
    """
    Tokenise the input text using jieba.

    Parameters
    ----------
    text : str
        The text to be tokenised.

    Returns
    -------
    list
        A list of tokens.
    """
    if not isinstance(text, str):
        raise ValueError("Input must be a string.")

    # Use jieba to perform tokenisation
    tokens = jieba.lcut(text)

    return tokens

def tokenise_dataframe(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Tokenise a specific column in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the text data.
    column : str
        The name of the column to tokenise.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with the specified column tokenised.
    """
    if column not in df.columns:
        raise ValueError(
            f"Column '{column}' does not exist in DataFrame."
        )

    df_tokenised = df.copy()
    df_tokenised[column] = df_tokenised[column].apply(tokenise)

    return df_tokenised

class PttTextTokeniser:
    def __init__(self):
        self.english_punctuations = list(string.punctuation)
        self.chinese_punctuations = [
            "。", "，", "！", "？", "；", "：", "＂", "＃", "＄", "％", "＆", "＇", "＊",
            "＋", "－", "／", "：", "；", "＜", "＝", "＞", "＠", "［", "＼", "］", "＾",
            "＿", "｀", "｛", "｜", "｝", "～", "｟", "｠", "｢", "｣", "､", "、", "〃",
            "》", "「", "」", "『", "』", "【", "】", "〔", "〕", "〖", "〗", "〘", "〙",
            "〚", "〛", "〜", "〝", "〞", "〟", "〰", "〾", "〿", "–", "—", "'", "'", "‛",
            """, """, "„", "‟", "…", "‧", "﹏", "！", "？", "｡", "。", "＂", "＃", "＄",
            "％", "＆", "＇", "（", "）", "＊", "＋", "，", "－", "／", "：", "；", "＜",
            "＝", "＞", "＠", "［", "＼", "］", "＾", "＿", "｀", "｛", "｜", "｝", "～"
        ]

        jieba.set_dictionary(JIEBA_DICT_PATH)
        jieba.initialize()
        jieba.set_dictionary(JIEBA_DICT_PATH)
        jieba.initialize()

    def _basic_cleaning(self, text):
        # Remove URLs using validators library
        words = text.split()
        words = [word for word in words if not validators.url(word)]
        text = ' '.join(words)

        # Remove unwanted patterns
        text = re.sub("※\s?(\[本文轉錄自).*", "", text)
        text = re.sub("(作者:).*", "", text)
        text = re.sub("(看板:).*", "", text)
        text = re.sub("(標題:).*", "", text)
        text = re.sub("(時間:).*", "", text)
        text = re.sub(r"\n", " ", text)

        return text

    def _remove_punct(self, tokens: list) -> list:
        """Remove punctuation from a list of tokens."""
        all_punctuations = set(
            self.english_punctuations + self.chinese_punctuations
        )
        return [
            token for token in tokens if (
                token and (token not in all_punctuations)
            )
        ]

    def tokenise_text(
            self, text: str, remove_punctuation: bool = False
    ) -> list:
        """
        Tokenise the input text after basic cleaning.

        Parameters
        ----------
        text : str
            The text to be tokenised.
        remove_punctuation : bool, optional
            Whether to remove punctuation from the tokens.

        Returns
        -------
        list
            A list of tokens.
        """
        if not isinstance(text, str):
            raise ValueError("Input must be a string.")

        cleaned_text = self._basic_cleaning(text=text)
        tokens = tokenise(text=cleaned_text)
        if remove_punctuation:
            tokens = self._remove_punct(tokens=tokens)
        return tokens

    def tokenise_dataframe_column(
            self, df: pd.DataFrame,
            target_column: str,
            remove_punctuation: bool = False
        ) -> pd.DataFrame:
        """
        Perform word tokenisation on the specified column of the
        DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the text data.
        target_column : str
            The name of the column to perform word tokenisation on.
        remove_punctuation : bool, optional
            Whether to remove punctuation from the tokens.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the specified column tokenised.
        """
        try:
            df[target_column] = df[target_column].apply(self._basic_cleaning)
            df_tokenised = tokenise_dataframe(df=df, column=target_column)
            if remove_punctuation:
                df_tokenised[target_column] = (
                    df_tokenised[target_column].apply(self._remove_punct)
                )
        except ValueError as e:
            raise ValueError(f"Error in tokenisation: {e}")
        return df_tokenised
