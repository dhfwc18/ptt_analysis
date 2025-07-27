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
    KMT_df = ptt_crawler.crawl("KMT")
    if KMT_df is not None:
        KMT_df = anonymiser.anonymise_data(
            df=KMT_df, column_name="author", save_to_file="KMT_anonymiser.csv"
        )
        KMT_df.to_csv(OUTPUT_DIR / "KMT_data.csv", index=False)
        logger.info("KMT data saved to KMT_data.csv")
    else:
        logger.error("Failed to scrape KMT data")
    DPP_df = ptt_crawler.crawl("DPP")
    if DPP_df is not None:
        DPP_df = anonymiser.anonymise_data(
            df=DPP_df, column_name="author", save_to_file="DPP_anonymiser.csv"
        )
        DPP_df.to_csv(OUTPUT_DIR / "DPP_data.csv", index=False)
        logger.info("DPP data saved to DPP_data.csv")
    else:
        logger.error("Failed to scrape DPP data")
    if KMT_df is None or DPP_df is None:
        logger.error("One or both dataframes are None, cannot combine")
        return None
    main_df = pd.concat([KMT_df, DPP_df], ignore_index=True)
    main_df.to_csv(OUTPUT_DIR / "main_data.csv", index=False)
    logger.info("Combined data saved to main_data.csv")
    return main_df

if __name__ == "__main__":
    main()