import os

from dotenv import load_dotenv
load_dotenv()

from scraping_utils import ProccessorBuilder, PreprocessorRunner, HTMLAnalyzer
from implementation.default_html import DefaultBuilder
from implementation.default_fixed_filter import ComposedAnalyzer

 
if __name__ == '__main__':
  pipeline_archive = ComposedAnalyzer()

  post_builder = ProccessorBuilder()
  post_builder.add_processor('default', DefaultBuilder())

  runner = PreprocessorRunner(os.getenv("TAR_FOLDER"), post_builder, pipeline_archive.initial_step())
  errors, tar_errors = runner.process()

  pipeline_archive.apply_to_index(None)
  
    