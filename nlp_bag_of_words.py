
import os

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from opensearchpy import OpenSearch

index_name = 'html-extracted-text'
endpoint = os.getenv("OPENSEARCH_ENDPOINT")
opensearch = OpenSearch(endpoint, verify_certs=False)

raw_results = opensearch.search(index=index_name, body={"size": 50000, "query": {"match_all": {}}})
doc_ids = list(map(lambda e: e["_id"], raw_results['hits']['hits']))

term_vectors_flat = list(map(lambda e: opensearch.termvectors(index=index_name, id=e), doc_ids))
terms_flat = list(map(lambda e: e['term_vectors']['body']['terms'] if 'body' in e['term_vectors'] else {}, term_vectors_flat))

corpus_terms = {}
for terms in terms_flat:
    for term in terms.keys():
        corpus_terms[term] = corpus_terms[term] + terms[term]['term_freq'] if term in corpus_terms else terms[term]['term_freq']

terms_table = pd.DataFrame(corpus_terms.items(), columns=["terms", "count"])
terms_table.to_json("terms_table.json", orient="records")