
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

    @abstractmethod
    def extract_page_body(self):
        pass
        
    @abstractmethod
    def extract_url(self):
        pass
    
    @abstractmethod
    def get_cached_path(self):
        pass
    
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