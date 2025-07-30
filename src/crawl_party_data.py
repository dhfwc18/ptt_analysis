# crawl_party_data.py
"""
Entry point to crawl PTT sub-forums for KMT and DPP data.

This script initializes the logging configuration, calls the crawler
functions, and saves the results to CSV files.
"""

# Internal imports
from pipeline import ptt_crawler, anonymiser
from config.config import OUTPUT_DIR
# External imports
import pandas as pd

# Logging setup
from config.logging_config import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

def main():
    user_anonymiser = anonymiser.UserAnonymiser()
    KMT_df = ptt_crawler.crawl("KMT")
    if KMT_df is not None:
        KMT_df = user_anonymiser.anonymise_dataframe(
            df=KMT_df, column_name="author"
        )
        KMT_df.to_csv(OUTPUT_DIR / "KMT_data.csv", index=False)
        logger.info("KMT data saved to KMT_data.csv")
    else:
        logger.error("Failed to scrape KMT data")
    DPP_df = ptt_crawler.crawl("DPP")
    if DPP_df is not None:
        DPP_df = user_anonymiser.anonymise_dataframe(
            df=DPP_df, column_name="author"
        )
        DPP_df.to_csv(OUTPUT_DIR / "DPP_data.csv", index=False)
        logger.info("DPP data saved to DPP_data.csv")
    else:
        logger.error("Failed to scrape DPP data")
    if KMT_df is None or DPP_df is None:
        logger.error("One or both dataframes are None, cannot combine")
        return None
    main_df = pd.concat([KMT_df, DPP_df], ignore_index=True)
    main_df["post_id"] = range(1, len(main_df) + 1)

    explode_comments = (
        main_df[["post_id", "comments"]].copy().explode("comments")
    )
    explode_comments = explode_comments.dropna(subset=["comments"])
    comments_df = pd.json_normalize(
        explode_comments["comments"].dropna().tolist()
    )
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