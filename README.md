# DICOM Listener Tool

This repository contains a tool to deploy a **DICOM Listener (SCP)** which interacts with a **DICOM Sender (SCU)**. The tool uses a database (PostgreSQL) that is deployed using **Docker Compose**. 

The code is structured to:
- Run a **DICOM SCP** (Listener) using the `main.py` file. The listener stores incoming DICOMs under the configured base folder, writes metadata to PostgreSQL, and creates/uses a `patient_id_map` table to generate stable internal patient IDs.
- Run an **anonymizer worker** (`anonymizer_worker.py`) that consumes study UIDs from RabbitMQ, reads file locations from PostgreSQL, and anonymizes files **in-place** (same folder paths stored by the listener).
- Run an **XNAT worker** (`xnat_worker.py`) that consumes study UIDs from a dedicated queue and sends anonymized studies to an XNAT DICOM SCP (C-STORE).
- Simulate a **DICOM SCU** (Sender) using the `test.py` file to send DICOM files to the listener.
  
Since this is the first version, some important file paths and configurations (like the DICOM folder location to store and send files) are hardcoded into the code. These variables need to be adjusted in the code before it will function as intended.

## Prerequisites

Before running the tool, ensure you have the following installed:
- **Docker** and **Docker Compose** (for running the database container)
- **Python 3.x** (with required dependencies)
- **pydicom**, **pynetdicom**, and **psycopg2** libraries installed. You can install the required dependencies with:
    ```bash
    pip install -r requirements.txt
    ```

## Setup and Deployment

### 1. **Set up the database using Docker Compose**
This tool requires a PostgreSQL database to store DICOM metadata. The database container is managed via Docker Compose. To get the database up and running:

- Navigate to the root of the repository.
- Run the following command to bring up the database container:
    ```bash
    docker-compose up -d
    ```
    This will create and start the PostgreSQL container in the background.

- The database will be accessible at `postgres:5432` with the default credentials:
    - **Username:** `postgres`
    - **Password:** `postgres`
    - **Database:** `postgres`

### 2. **Running the DICOM Listener (SCP)**

Once the database is up and running, you can run the DICOM Listener (SCP). The **SCP** listens for incoming DICOM files and stores them in a specified folder.

To run the listener:

1. Edit the `main.py` file and adjust the following variables to your needs:
    - **BASE_DIR:** Specify where you want to store the received DICOM files (default is `dicom_storage`).
    - **Database Connection Parameters:** If needed, change the database connection details.

2. Run the listener:
    ```bash
    python main.py
    ```
   The listener will start and will listen on **port 11112** by default.

### 3. **Run background workers**

Start the two workers in separate terminals/containers after the listener is up:

```bash
python anonymizer_worker.py
python xnat_worker.py
```

For Docker deployment, the project now uses **one shared Dockerfile** and three services in `docker-compose.yaml` with different commands. The image is built once (`dicom-listner:latest`) and reused by all three services:
- `dicom-sorter` -> listener + API (`./start.sh`)
- `anonymizer-worker` -> `python anonymizer_worker.py`
- `xnat-worker` -> `python xnat_worker.py`

This is the recommended setup so all services share the same runtime/dependencies while scaling independently.
When anonymizer is enabled, the XNAT worker receives study UIDs only after in-place anonymization is completed.
Because anonymization is in-place, `dicom-sorter`, `anonymizer-worker`, and `xnat-worker` should all mount the same listener storage volume (`./associationdata:/dicomsorter/data`).

To reduce memory pressure in the anonymizer container, the worker processes study files in DB batches (instead of loading an entire study result set at once). You can tune batch size with `ANONYMIZER_DICOM_BATCH_SIZE` (default `100`) in `docker-compose.yaml`.

RabbitMQ queues are configured in `Config/config.yaml` under `rabbitMQ`:
- `queue_name` (listener output queue when anonymizer is disabled, e.g. `DICOM_Processor`)
- `anonymizer_queue_name` (listener -> anonymizer input queue)
- `xnat_queue_name` (anonymizer -> XNAT worker queue, defaults to `DICOM_XNAT`)
- `use_anonymizer` (if `true`, listener publishes to `anonymizer_queue_name`; if `false`, listener publishes directly to `queue_name`)

XNAT SCP settings are configured in `Config/config.yaml` under `Xnat`:
- `ae_title` (remote XNAT SCP AE title)
- `ip` (remote XNAT SCP host/IP)
- `port` (remote XNAT SCP port)
- `scu_ae_title` (local AE title used by this worker when opening the association)

The XNAT worker uses DICOM network send (C-STORE SCU -> SCP).

### 4. **Sending DICOM Files (SCU)**

You can use the **SCU (Service Class User)**, implemented in `test.py`, to send DICOM files to the listener. The SCU will send the contents of a specified folder (which you should provide the path to) to the listener.

To run the SCU:

1. Edit the `test.py` file and adjust the following variables to your needs:
    - **DICOM_FOLDER:** Specify the path to the folder containing the DICOM files you want to send.
  
2. Run the sender:
    ```bash
    python test.py
    ```

This will send all the DICOM files from the specified folder to the DICOM listener.

### 5. **Modifying the Folder Locations (Hardcoded Variables)**

Since this is the first version, the following variables are hardcoded in the `main.py` and `test.py` files:
- **DICOM_FOLDER**: This defines where the files will be stored by the listener.
- **SEND_FOLDER**: This defines the folder path from where DICOM files will be sent.

To make the tool work in your environment, **please edit these paths in the code**:
- For the **listener**, adjust the `BASE_DIR` in `main.py` to specify where received DICOM files should be saved.
- For the **SCU**, change the `DICOM_FOLDER` in `test.py` to point to the folder that contains the DICOM files you want to send.

## Structure

The codebase is structured as follows:
