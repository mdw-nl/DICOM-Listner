import json
from elasticsearch import Elasticsearch
from dicomsorter.src.global_var import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL)

# Search for documents where the field exists
result = es.search(
    index="dicom",
    query={"exists": {"field": "RTReferencedSeriesSequence"}},
    _source=["RTReferencedSeriesSequence"],
    size=100  # adjust as needed
)

hits = result["hits"]["hits"]

for h in hits:
    seq_text = h["_source"].get("RTReferencedSeriesSequence", "[]")
    
    try:
        seq = json.loads(seq_text)  # parse stringified JSON
    except json.JSONDecodeError:
        print("Failed to parse JSON:", seq_text)
        continue
    
    for entry in seq:
        print("Referenced SeriesInstanceUID:", entry.get("SeriesInstanceUID"))
