
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
    instance_number INTEGER,
    file_path TEXT,
    study_description TEXT,
    patient_name TEXT,
    series_description TEXT,
    file_size INTEGER,
    transfer_syntax_uid TEXT,
    modality_type TEXT
);
"""