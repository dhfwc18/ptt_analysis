# crawl_party_data.py
"""
Script to crawl PTT sub-forums for KMT and DPP data.

This script initializes the logging configuration, calls the crawler
functions, and saves the results to CSV files.
"""

from logging import getLogger
from pathlib import Path

import pandas as pd

from ptt_crawler import UserAnonymiser, crawl, filter_bbs_header, setup_logging

setup_logging()
logger = getLogger(__name__)

OUTPUT_DIR = Path(__file__).parents[1]/"output"

def main():
    user_anonymiser = UserAnonymiser()
    kmt_df = crawl("KMT")
    if kmt_df is not None:
        kmt_df = user_anonymiser.anonymise_dataframe(df=kmt_df, column_name="author")
        kmt_df["content"] = kmt_df["content"].apply(filter_bbs_header)
        kmt_df.to_csv(OUTPUT_DIR / "KMT_data.csv", index=False)
        logger.info("KMT data saved to KMT_data.csv")
    else:
        logger.error("Failed to scrape KMT data")
    dpp_df = crawl("DPP")
    if dpp_df is not None:
        dpp_df = user_anonymiser.anonymise_dataframe(df=dpp_df, column_name="author")
        dpp_df["content"] = dpp_df["content"].apply(filter_bbs_header)
        dpp_df.to_csv(OUTPUT_DIR / "DPP_data.csv", index=False)
        logger.info("DPP data saved to DPP_data.csv")
    else:
        logger.error("Failed to scrape DPP data")
    if kmt_df is None or dpp_df is None:
        logger.error("One or both dataframes are None, cannot combine")
        return None
    main_df = pd.concat([kmt_df, dpp_df], ignore_index=True)
    main_df["post_id"] = range(1, len(main_df) + 1)

    explode_comments = main_df[["post_id", "comments"]].copy().explode("comments")
    explode_comments = explode_comments.dropna(subset=["comments"])
    comments_df = pd.json_normalize(explode_comments["comments"].dropna().tolist())
    comments_df["post_id"] = explode_comments["post_id"].values
    comments_df = comments_df.reset_index(drop=True)

    main_df = main_df.drop(columns=["comments"])
    main_df.to_csv(OUTPUT_DIR / "main_data.csv", index=False)
    logger.info("Combined data saved to main_data.csv")

    comments_df = user_anonymiser.anonymise_dataframe(
        df=comments_df, column_name="userid"
    )
    comments_df.to_csv(OUTPUT_DIR / "comments_data.csv", index=False)
    logger.info("Comments data saved to comments_data.csv")

    user_anonymiser.save_mapping(OUTPUT_DIR / "user_mapping.csv")
    logger.info("User mapping saved to user_mapping.csv")
    return main_df


if __name__ == "__main__":
    main()
