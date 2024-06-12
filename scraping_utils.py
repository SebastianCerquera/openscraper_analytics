import pandas as pd

from bs4 import BeautifulSoup                                                                                                                                                                
from abc import ABC, abstractmethod

from os import listdir
from os.path import isfile, join

from opensearchpy import OpenSearch

import tarfile

class Post(ABC):
    
    def __init__(self, url, path, body):        
        self.url = url
        self.path = path
        self.body = body

    def extract_page_body(self):
        return self.body

    def extract_url(self):
        return self.url

    def get_cached_path(self):
        return self.path
    
    ## TODO it is still missing to add the images, I will need to improve the design
    def to_json(self):
        return {
            'body': self.extract_page_body(),
            'url': self.extract_url(),
            'path': self.get_cached_path()
        }
    

class PostBuilder(ABC):
    
    @abstractmethod
    def validate_post(self, path):
        pass

class ProccessorBuilder():
    
    def __init__(self):
        self.proccessors = {}
    
    def add_processor(self, identifier, proccessor):
        assert(isinstance(proccessor, PostBuilder))
        
        self.proccessors[identifier] = proccessor
    
    def build_post(self, raw, path):
        post = None
        for k,v in self.proccessors.items():
            post = v.validate_post(raw, path)
            if post is not None:
                break
        return post

class TarArchive(ABC):
    
    def __init__(self, archive, tar_path):
        full_path = join(archive, tar_path)
        self.tar = tarfile.open(full_path, "r:gz")
        self.files = self.tar.getmembers()
    
    def extract_file(self, path):
        post = self.tar.extractfile(path)
        return post.read()
    
    def list_files(self):
        return [file for file in self.files if "html" in file.name]
    
    @classmethod
    def build_archive(clazz, archive_folder, tar_path):
        return TarArchive(archive_folder, tar_path)
    
    @staticmethod
    def list_archives(archive_folder):
        return [f for f in listdir(archive_folder) if isfile(join(archive_folder, f))]
    

class ElasticArchive(ABC):

    @abstractmethod
    def default_settings(self):
        pass
    
    def __init__(self, endpoint, index_name):
        self.index_name = index_name
        self.elasticsearch = OpenSearch(endpoint, verify_certs=False)
        
    def save_post(self, post):
        if not self.elasticsearch.indices.exists(index=self.index_name):
            self.elasticsearch.indices.create(index=self.index_name, body=self.default_settings())
            
        ## TODO add validation to the response
        self.elasticsearch.index(index=self.index_name, body=post.to_json())

    def build_bag_of_words(self):
        raw_results = self.elasticsearch.search(
            index=self.index_name, body={"size": 10000, "query": {"match_all": {}}})
        
        doc_ids = list(map(lambda e: e["_id"], raw_results['hits']['hits']))
        
        term_vectors_flat = list(map(
            lambda e: self.elasticsearch.termvectors(index=self.index_name, id=e), doc_ids))
        terms_flat = list(
            map(
                lambda e: e['term_vectors']['body']['terms'] if 'body' in e['term_vectors'] else {}, 
                term_vectors_flat
                )
            )
        
        corpus_terms = {}
        for terms in terms_flat:
            for term in terms.keys():
                corpus_terms[term] = corpus_terms[term] + terms[term]['term_freq'] if term in corpus_terms else terms[term]['term_freq']
        
        return pd.DataFrame(corpus_terms.items(), columns=["terms", "count"])

    def apply_to_index(self, target_archive, query={"match_all": {}}):
        raw_results = self.elasticsearch.search(
            index=self.index_name, body={"size": 10000, "query": query})

        for doc in raw_results['hits']['hits']:
            post = Post(doc['_source']['url'], doc['_source']['path'], doc['_source']['body'])
            target_archive.save_post(post)

    def initial_step(self):
        self

    

class PreprocessorRunner(ABC):
    
    def __init__(self, archive_path, builder, elastic_archive):
        self.archive_path = archive_path
        self.samples = TarArchive.list_archives(archive_path)
        
        self.builder = builder        
        self.elastic_archive = elastic_archive
    
    def process_sample(self, sample):
        posts = sample.list_files()
    
        errors = []

        for file in posts:
            try:
                post_raw = sample.extract_file(file)
                post = self.builder.build_post(post_raw, file.path)
                self.elastic_archive.save_post(post)
            except Exception as ex:
                errors.append(file)
                continue
            
        return errors
    
    def process(self):
        errors = []
        tar_errors = []
        for tar_path in self.samples:
            try:
                tar = TarArchive.build_archive(self.archive_path, tar_path)
                errors = errors + self.process_sample(tar)
            except Exception as ex:
                tar_errors.append(tar_path)
        return errors, tar_errors
    

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
                       "tokenizer": "whitespace",
                       "filter": [
                         "english_stop",
                         "spanish_stop"
                       ],
                       "char_filter": [
                          "html_strip"
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