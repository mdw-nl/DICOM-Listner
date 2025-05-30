
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
    referenced_sop_class_uid TEXT,
    referenced_rt_plan_uid TEXT,
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

CREATE_DATABASE_QUERY_3 = """
CREATE TABLE calculation_status (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,     
    study_uid TEXT,       
    status BOOLEAN NOT NULL,                          
    timestamp TIMESTAMP NOT NULL          
);"""




INSERT_QUERY_DICOM_META = """
    INSERT INTO dicom_insert (
        patient_id, study_instance_uid, series_instance_uid, modality, 
        sop_instance_uid, sop_class_uid, instance_number, 
        file_path,referenced_rt_plan_uid, referenced_sop_class_uid, modality_type,assoc_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_QUERY_DICOM_ASS = """
    INSERT INTO associations (
        uuid, ae_title, ip_address, port, timestamp
    ) VALUES (%s, %s, %s, %s, %s)
"""

UNIQUE_UID_SELECT = """
    SELECT EXIST(SELECT study_instance_uid FROM public.dicom_insert WHERE study_instance_uid = %s;)
    VALUES (%s);
"""