import os

from dotenv import load_dotenv
load_dotenv()

from scraping_utils import ProccessorBuilder, PreprocessorRunner, HTMLAnalyzer
from implementation.default_html import DefaultBuilder
from implementation.default_fixed_filter import FixedTextAnalyzer

RAW_TEXT_INDEX_NAME = os.getenv("SIMPLE_TEXT_FROM_HTML_INDEX")
CLEAN_TEXT_INDEX_NAME = os.getenv("CORE_TEXT_FROM_SIMPLE_TEXT_INDEX")
 
if __name__ == '__main__':
  post_builder = ProccessorBuilder()
  post_builder.add_processor('default', DefaultBuilder())

  archive = HTMLAnalyzer(os.getenv("OPENSEARCH_ENDPOINT"), RAW_TEXT_INDEX_NAME)

  runner = PreprocessorRunner(os.getenv("TAR_FOLDER"), post_builder, archive)
  errors, tar_errors = runner.process()

  text_archive = FixedTextAnalyzer(os.getenv("OPENSEARCH_ENDPOINT"), CLEAN_TEXT_INDEX_NAME)
  archive.apply_to_index(text_archive)
  
    