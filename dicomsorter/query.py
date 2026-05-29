
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
CREATE TABLE IF NOT EXISTS calculation_status (
    id          SERIAL      PRIMARY KEY,
    study_uid   TEXT        NOT NULL,
    status      BOOLEAN     NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    patient_id  TEXT,
    error       JSONB
);
CREATE INDEX IF NOT EXISTS idx_calculation_status_study   ON calculation_status (study_uid);
CREATE INDEX IF NOT EXISTS idx_calculation_status_patient ON calculation_status (patient_id);
"""

CREATE_DATABASE_QUERY_DVH = """
CREATE TABLE IF NOT EXISTS dvh_results (
    id                   SERIAL PRIMARY KEY,
    patient_id           TEXT          NOT NULL,
    study_uid            TEXT,
    structure_name       TEXT          NOT NULL,
    min_dose_gy          DOUBLE PRECISION,
    mean_dose_gy         DOUBLE PRECISION,
    max_dose_gy          DOUBLE PRECISION,
    volume_cc            DOUBLE PRECISION,
    color                TEXT,
    metrics              JSONB,
    dvh_points           JSONB,
    payload              JSONB         NOT NULL,
    rt_plan_path         TEXT,
    rt_dose_paths        TEXT,
    effective_dose_type  TEXT,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dvh_results_patient ON dvh_results (patient_id);
CREATE INDEX IF NOT EXISTS idx_dvh_results_study   ON dvh_results (study_uid);
"""





INSERT_QUERY_DICOM_META = """
    INSERT INTO dicom_insert (
        patient_id, study_instance_uid, series_instance_uid, modality, 
        sop_instance_uid, sop_class_uid, instance_number, 
        file_path,referenced_rt_plan_uid, referenced_sop_class_uid, modality_type,assoc_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (sop_instance_uid) DO NOTHING
"""

INSERT_QUERY_DICOM_ASS = """
    INSERT INTO associations (
        uuid, ae_title, ip_address, port, timestamp
    ) VALUES (%s, %s, %s, %s, %s)
"""

UNIQUE_UID_SELECT = """
    SELECT EXISTS (
        SELECT 1
        FROM public.dicom_insert
        WHERE study_instance_uid = %s
    );
"""
