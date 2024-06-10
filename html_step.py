from scraping_utils import ElasticArchive

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