import os
import pydicom
from dicomsorter.src.dicom_data import return_dicom_data

def get_unique_study_uid_from_folder(folder_path):
    study_uids = set()

    for root, _, files in os.walk(folder_path):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                study_uids.add(ds.StudyInstanceUID)
            except Exception:
                continue

    if not study_uids:
        raise ValueError("No DICOM files found")

    if len(study_uids) > 1:
        raise ValueError(f"Multiple StudyInstanceUIDs found: {study_uids}")

    return study_uids.pop()

ds = pydicom.dcmread("associationdata\Tom\99999.8088316119225601241627216725805872478376234007905444525746\RTSTRUCT\99999.1540316587304974106186984212863929165490277604377631946957.dcm")

print(return_dicom_data(ds))