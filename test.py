import os
from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, debug_logger
from pynetdicom.presentation import StoragePresentationContexts
import psycopg2
import pika
import gc
import zipfile
import urllib.request

import logging

ZIP_PATH = 'dicomtestdata.zip'
DICOM_DATA_PATH = 'dicomdata'
DICOM_URL = 'https://github.com/mdw-nl/test-data/releases/download/dicom-data-1.0.0/dicomdata.zip'
RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"
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


def send_fold(folder_path):
    """Sends a single DICOM file to the DICOM SCP."""
    assoc = ae.associate(SCP_IP, SCP_PORT, ae_title=SCP_AE_TITLE)
    if assoc.is_established:

        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(".dcm"):
                    try:
                        file_path = os.path.join(root, file)
                        ds = dcmread(file_path)  # Load DICOM file

                        # Establish association with the SCP

                        # Send the DICOM file
                        print(f"[INFO] Sending: {file_path}")
                        status = assoc.send_c_store(ds)

                        if status:
                            print(f"[INFO] C-STORE Response: 0x{status.Status:04X}")
                        else:
                            print("[ERROR] Failed to send DICOM file.")

                    except Exception as e:
                        print(f"[ERROR] Failed to send : {e}")

        assoc.release()
        gc.collect()
    else:
        print("[ERROR] Association with SCP failed.")


def send_all_dicoms(folder_path):
    """Send all DICOM files from a folder."""
    send_fold(folder_path)





def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")


if __name__ == "__main__":

    if not os.path.exists(ZIP_PATH):
        urllib.request.urlretrieve(DICOM_URL, ZIP_PATH)
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall('')

    send_all_dicoms(DICOM_FOLDER)

    QUEUE_NAME = "DICOM_Processor"
#
    ## Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.URLParameters("amqp://user:password@localhost:5672/"))
    channel = connection.channel()
#
    ## Get queue information
    #
    queue = channel.queue_declare(queue=QUEUE_NAME, passive=True)
    message_count = queue.method.message_count
    print(f"Total messages in queue: {message_count}")
    #channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    #print("Waiting for messages. To exit, press CTRL+C")
    #channel.start_consuming()
#
    ## Close connection
    #connection.close()