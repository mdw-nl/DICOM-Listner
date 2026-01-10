import os
import pydicom
from pydicom import dcmread
import requests
from requests.auth import HTTPBasicAuth
import logging
import time
import zipfile
import uuid

from src.global_var import XNAT_USERNAME
from src.global_var import XNAT_PASSWORD
from src.global_var import XNAT_URL

class DICOMtoXNAT:
    def __init__(self):
        self.xnat_url = XNAT_URL
        username = XNAT_USERNAME
        password = XNAT_PASSWORD
        self.auth = HTTPBasicAuth(username, password)
    
        
    def check_for_subfolders(self, folder_path: str):
        """ Checks whether a folder contains subfolders. If so, return a list of paths of the subfolders."""
        
        entries = os.listdir(folder_path)
        if not entries:
            raise ValueError(f"Folder is empty: {folder_path}")

        has_files = False
        subfolders = []

        for entry in entries:
            full_path = os.path.join(folder_path, entry)
            if os.path.isfile(full_path):
                has_files = True
            elif os.path.isdir(full_path):
                subfolders.append(full_path)

        if has_files and subfolders:
            raise ValueError(
                f"Folder contains both files and subfolders: {folder_path}"
            )
            
        # Case 1: files exist, treat main folder as single dataset
        if has_files:
            return [folder_path]

        # Case 2: only subfolders exist
        if subfolders:
            return subfolders
    
    def checking_connectivity(self):
        """Ckecks the connection to xnat"""
        logging.info("Checking connectivity")
        connectivity = requests.get(self.xnat_url, auth=self.auth)
        logging.info(connectivity.status_code)
        return connectivity.status_code
    
    def adding_treatment_site(self, treatment_sites, data_folder):
        """Hardcode the treatment sides where we want sort files in the XNAT projects"""
        try:
            logging.info("Adding a fake treatment site to the dicom files to filter the projects.")
                   
            files = os.listdir(data_folder)
            for file in files:
                if file.endswith(".dcm"):
                    file_path = os.path.join(data_folder, file)
                    ds = dcmread(file_path)
                    treatment_site = treatment_sites[ds.PatientID]
                    ds.BodyPartExamined  = treatment_site
                    ds.save_as(file_path)
            
            logging.info("Added the treatment site")
        except Exception as e:
            logging.error(f"An error occurred adding the fake treatment site: {e}", exc_info=True)
    
    def dicom_to_xnat(self, data_folder):
        """Send the DICOM in a folder to XNAT"""
        
        # Temporary failure codes
        retry_status_codes = {408, 429, 500, 502, 503, 504}
        max_retries = 3
        timeout_seconds = 30
        
        # Create a zipfile
        first_iteration = True
        files = os.listdir(data_folder)
        if not files:
            raise ValueError(f"No files found in {data_folder}")
                
        os.makedirs("zip_folder", exist_ok=True)
        zip_path = os.path.join("zip_folder", f"{uuid.uuid4()}.zip")
        
        try:
            # Add the data to the zipfile
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in files:
                    file_path = os.path.join(data_folder, file)
                    
                    if file.lower().endswith('.dcm'):
                        zipf.write(file_path, arcname=file)
                        
                        if first_iteration:
                            ds = dcmread(os.path.join(data_folder, files[0]))
                            treatment_site = ds.BodyPartExamined
                            first_iteration = False
        
                    else:
                        continue              
                                
            upload_url = f"{self.xnat_url}/data/services/import?PROJECT_ID={treatment_site}&overwrite=append&prearchive=true&inbody=true"
            
            for attempt in range(1, max_retries + 1):
                try:
                    with open(zip_path, "rb") as f:
                        response = requests.post(
                            upload_url,
                            data=f,
                            headers={"Content-Type": "application/zip"},
                            auth=self.auth,
                            timeout=timeout_seconds,
                        )

                    if response.status_code in (200, 201):
                        logging.info("XNAT upload succeeded")
                        return True

                    if response.status_code not in retry_status_codes:
                        logging.error(
                            f"Non-retriable XNAT error "
                            f"{response.status_code}: {response.text}"
                        )
                        return False

                    logging.warning(
                        f"XNAT upload attempt {attempt}/{max_retries} failed "
                        f"({response.status_code}). Retrying..."
                        )

                except requests.exceptions.Timeout:
                    logging.warning(
                        f"XNAT upload attempt {attempt}/{max_retries} timed out"
                    )

                except requests.exceptions.ConnectionError as e:
                    logging.warning(
                        f"XNAT upload attempt {attempt}/{max_retries} "
                        f"connection error: {e}"
                        )

                time.sleep(2 ** attempt)

            logging.error("XNAT upload failed after maximum retries")
            return False

        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)
    
    def run(self, data_folder):
        treatment_sites = {"Tom": "LUNG", "Tim": "KIDNEY"}
        
        # Check if connection to xnat works
        connection = self.checking_connectivity()
        while connection != 200:
            logging.info(f"Connectivition check failed with status code: {connection}.")
            time.sleep(10)
            connection = self.checking_connectivity()

        logging.info("Connecting to XNAT works")         
        
        data_folders = self.check_for_subfolders(data_folder)
        
        for data_folder in data_folders:
            try:
                self.adding_treatment_site(treatment_sites, data_folder)        
                self.dicom_to_xnat(data_folder)
                logging.info(f"Send dicom file from: {data_folder} to XNAT")
                
            except Exception as e:
                logging.error(f"An error occurred in the run method: {e}", exc_info=True)