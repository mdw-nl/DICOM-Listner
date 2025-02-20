import os
from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, debug_logger
from pynetdicom.presentation import StoragePresentationContexts
import psycopg2

# Enable debug logging (optional)
debug_logger()

# Configuration
DICOM_FOLDER = "/Users/alessioromita/Documents/image test"
# Update with the folder containing DICOM files
AE_TITLE = "MY_SCU"  # Application Entity Title of this SCU
SCP_AE_TITLE = "MY_SCP"  # AE Title of the SCP (listener)
SCP_IP = "localhost"  # IP of the SCP (DICOM listener)
SCP_PORT = 104  # Port of the SCP

# Initialize Application Entity (AE)
ae = AE(ae_title=AE_TITLE)

# Add supported presentation contexts (all Storage SOP classes)
for context in StoragePresentationContexts:
    ae.add_requested_context(context.abstract_syntax, ExplicitVRLittleEndian)


def send_dicom(file_path):
    """Sends a single DICOM file to the DICOM SCP."""
    try:
        ds = dcmread(file_path)  # Load DICOM file

        # Establish association with the SCP
        assoc = ae.associate(SCP_IP, SCP_PORT, ae_title=SCP_AE_TITLE)

        if assoc.is_established:
            print(f"[INFO] Sending: {file_path}")
            status = assoc.send_c_store(ds)  # Send the DICOM file

            if status:
                print(f"[INFO] C-STORE Response: 0x{status.Status:04X}")
            else:
                print("[ERROR] Failed to send DICOM file.")

            assoc.release()  # Release the association
        else:
            print("[ERROR] Association with SCP failed.")

    except Exception as e:
        print(f"[ERROR] Failed to send {file_path}: {e}")


def send_all_dicoms(folder_path):
    """Send all DICOM files from a folder."""
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".dcm"):  # Ensure it's a DICOM file
                file_path = os.path.join(root, file)
                send_dicom(file_path)

#def test_query(query):
#    conn = psycopg2.connect(
#        host="postgres", database="postgres", user="postgres", password="postgres", port=5432
#    )
#
#    cursor = conn.cursor()
#    cursor.execute(query)
#    results = cursor.fetchall()
#    columns = [desc[0] for desc in cursor.description]
#
#    # Create DataFrame
#    #df = pd.DataFrame(results, columns=columns)
#    conn.commit()
if __name__ == "__main__":
    send_all_dicoms(DICOM_FOLDER)
