
CREATE_DATABASE_QUERY = """
CREATE TABLE dicom_insert (
    id SERIAL  PRIMARY KEY ,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    patient_id TEXT NOT NULL,
    study_instance_uid TEXT NOT NULL,
    series_instance_uid TEXT NOT NULL,
    modality TEXT NOT NULL,
    sop_instance_uid TEXT NOT NULL UNIQUE,
    sop_class_uid TEXT NOT NULL,
    instance_number TEXT,
    file_path TEXT,
    modality_type TEXT,
    assoc_id TEXT
);
"""

CREATE_DATABASE_QUERY_2 = """
CREATE TABLE associations (
    uuid TEXT PRIMARY KEY,               
    ae_title TEXT NOT NULL,              
    ip_address TEXT NOT NULL,            
    port INTEGER NOT NULL,               
    timestamp TIMESTAMP NOT NULL          
);"""


INSERT_QUERY_DICOM_META = """
    INSERT INTO dicom_insert (
        patient_id, study_instance_uid, series_instance_uid, modality, 
        sop_instance_uid, sop_class_uid, instance_number, 
        file_path, modality_type,assoc_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_QUERY_DICOM_ASS = """
    INSERT INTO associations (
        uuid, ae_title, ip_address, port, timestamp
    ) VALUES (%s, %s, %s, %s, %s)
"""