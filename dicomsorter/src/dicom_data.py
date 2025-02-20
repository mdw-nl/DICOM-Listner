from pydicom import Dataset


def return_dicom_data(ds: Dataset):
    patient_id = ds.PatientID if "PatientID" in ds else "UNKNOWN"
    study_uid = ds.StudyInstanceUID if "StudyInstanceUID" in ds else "UNKNOWN"
    series_uid = ds.SeriesInstanceUID if "SeriesInstanceUID" in ds else "UNKNOWN"
    modality = ds.Modality if "Modality" in ds else "UNKNOWN"
    sop_uid = ds.SOPInstanceUID if "SOPInstanceUID" in ds else "UNKNOWN"
    sop_class_uid = ds.SOPClassUID if "SOPClassUID" in ds else "UNKNOWN"
    instance_number = int(ds.InstanceNumber) if "InstanceNumber" in ds else "UNKNOWN"
    modality_type = ds.get("ModalityType", "UNKNOWN")
    return patient_id, study_uid, series_uid, modality, sop_uid, sop_class_uid, \
        instance_number, modality_type
