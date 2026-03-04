import json
import logging

from elasticsearch import Elasticsearch

from dicomsorter.src.global_var import ELASTICSEARCH_URL

logger = logging.getLogger(__name__)

es = Elasticsearch(ELASTICSEARCH_URL)

result = es.search(
    index="dicom",
    query={"exists": {"field": "RTReferencedSeriesSequence"}},
    source=["RTReferencedSeriesSequence"],
    size=100,
)

hits = result["hits"]["hits"]

for h in hits:
    seq_text = h["_source"].get("RTReferencedSeriesSequence", "[]")

    try:
        seq = json.loads(seq_text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse JSON: %s", seq_text)
        continue

    for entry in seq:
        logger.info("Referenced SeriesInstanceUID: %s", entry.get("SeriesInstanceUID"))
