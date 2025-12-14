from elasticsearch import Elasticsearch, helpers
import pydicom
from datetime import datetime
import os 
from pydicom.multival import MultiValue
from src.global_var import ELASTICSEARCH_URL

class ElasticSearchHandler:
    
    def __init__(self):
        self.es = Elasticsearch(ELASTICSEARCH_URL)
    
    def extract_metadata(self, path):
        """Extract all the metadata from the dicom image except for the pixel data"""
        ds = pydicom.dcmread(path)
        
        # Remove pixeldata
        if 'PixelData' in ds:
            del ds.PixelData
        
        doc = {}
        for elem in ds.iterall():
            if elem.VR not in ('OB', 'OW'):  # skip pixel/binary data
                key = elem.keyword if elem.keyword else str(elem.tag)
                value = elem.value

                if isinstance(value, MultiValue):
                    value = list(value)
                elif isinstance(value, bytes):
                    value = value.decode('utf-8', errors='ignore')
                elif not isinstance(value, (str, int, float, bool, list, dict, type(None))):
                    value = str(value)
                doc[key] = value

        # Convert StudyDate/StudyTime to ISO format
        if 'StudyDate' in ds:
            d = getattr(ds, 'StudyDate', None)
            t = getattr(ds, 'StudyTime', None)
            if d:
                if t:
                    doc['study_date'] = datetime.strptime(d + t.split('.')[0], "%Y%m%d%H%M%S").isoformat() + "Z"
                else:
                    doc['study_date'] = datetime.strptime(d, "%Y%m%d").isoformat() + "T00:00:00Z"

        return doc
        
    def single_file(self, file_path, index):
        """Sends a single file to ES, index corresponds to the project in ES"""
        doc = self.extract_metadata(file_path)
        doc.update({
            "file_path": file_path,
            "created_at": datetime.utcnow().isoformat() + "Z",
        })
        
        doc_id = doc.get('SOPInstanceUID') or doc.get('SOPInstanceUID', str(datetime.utcnow().timestamp()))
        self.es.index(index=index, id=doc_id, document=doc)
    
if __name__ == "__main__":
    EShandler = ElasticSearchHandler()
    EShandler.single_file("dicomdata/RS_no_nody.dcm", "dicom")