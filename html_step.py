import os

from dotenv import load_dotenv
load_dotenv()

from scraping_utils import ElasticArchive, ProccessorBuilder, PreprocessorRunner
from implementation.default import DefaultBuilder

INDEX_NAME = os.getenv("SIMPLE_TEXT_FROM_HTML_INDEX")

class HTMLAnalyzer(ElasticArchive):
    
    def default_settings(self):
        return  {
           "settings": {
               "index": {
                 "number_of_shards": "1",
                 "analysis": {
                   "filter": {
                     "english_stop": {
                       "type":       "stop",
                       "stopwords":  "_english_" 
                     },
                     "spanish_stop": {
                       "type":       "stop",
                       "stopwords":  "_spanish_" 
                     }
                   },
                   "analyzer": {
                     "ma": {
                       "tokenizer": "mt",
                       "filter": [
                         "english_stop",
                         "spanish_stop"
                       ],
                       "char_filter": [
                          "html_strip"
                        ]
                     }
                   },
                   "tokenizer": {
                     "mt": {
                       "type": "char_group",
                       "tokenize_on_chars": [
                         "whitespace",
                         "\n"
                       ]
                     }
                   }
                 },
                 "number_of_replicas": "1"
               }
             },
           "mappings": {
               "properties": {
                 "url": {
                   "type": "text",
                   "fields": {
                     "keyword": {
                       "type": "keyword",
                       "ignore_above": 256
                     }
                   }
                 },
                 "path": {
                   "type": "text",
                   "fields": {
                     "keyword": {
                       "type": "keyword",
                       "ignore_above": 256
                     }
                   }
                 },
                 "body": {
                   "type": "text",
                   "term_vector": "yes",
                   "analyzer" : "ma",
                   "fields": {
                     "keyword": {
                       "type": "keyword",
                       "ignore_above": 256
                     }
                   }
                 }
               }
             }
          }
    
if __name__ == '__main__':
  post_builder = ProccessorBuilder()
  post_builder.add_processor('default', DefaultBuilder())

  archive = HTMLAnalyzer(os.getenv("OPENSEARCH_ENDPOINT"), INDEX_NAME)

  runner = PreprocessorRunner(os.getenv("TAR_FOLDER"), post_builder, archive)
  errors, tar_errors = runner.process()
  
    