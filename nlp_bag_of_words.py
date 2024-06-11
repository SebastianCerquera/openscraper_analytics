
import os

from dotenv import load_dotenv
load_dotenv()

from scraping_utils import HTMLAnalyzer
from implementation.default_fixed_filter import FixedTextAnalyzer


RAW_TEXT_INDEX_NAME = os.getenv("SIMPLE_TEXT_FROM_HTML_INDEX")
CLEAN_TEXT_INDEX_NAME = os.getenv("CORE_TEXT_FROM_SIMPLE_TEXT_INDEX")

endpoint = os.getenv("OPENSEARCH_ENDPOINT")

if __name__ == '__main__':
  html_archive = HTMLAnalyzer(endpoint, RAW_TEXT_INDEX_NAME)
  text_archive = FixedTextAnalyzer(endpoint, CLEAN_TEXT_INDEX_NAME)

  html_archive.build_bag_of_words().to_json("terms_table.json", orient="records")
  text_archive.build_bag_of_words().to_json("text_terms_table.json", orient="records")