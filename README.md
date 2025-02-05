# DICOM Listener Tool

This repository contains a tool to deploy a **DICOM Listener (SCP)** which interacts with a **DICOM Sender (SCU)**. The tool uses a database (PostgreSQL) that is deployed using **Docker Compose**. 

The code is structured to:
- Run a **DICOM SCP** (Listener) using the `main.py` file.
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

- The database will be accessible at `localhost:5432` with the default credentials:
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

### 3. **Sending DICOM Files (SCU)**

You can use the **SCU (Service Class User)**, implemented in `test.py`, to send DICOM files to the listener. The SCU will send the contents of a specified folder (which you should provide the path to) to the listener.

To run the SCU:

1. Edit the `test.py` file and adjust the following variables to your needs:
    - **DICOM_FOLDER:** Specify the path to the folder containing the DICOM files you want to send.
  
2. Run the sender:
    ```bash
    python test.py
    ```

This will send all the DICOM files from the specified folder to the DICOM listener.

### 4. **Modifying the Folder Locations (Hardcoded Variables)**

Since this is the first version, the following variables are hardcoded in the `main.py` and `test.py` files:
- **DICOM_FOLDER**: This defines where the files will be stored by the listener.
- **SEND_FOLDER**: This defines the folder path from where DICOM files will be sent.

To make the tool work in your environment, **please edit these paths in the code**:
- For the **listener**, adjust the `BASE_DIR` in `main.py` to specify where received DICOM files should be saved.
- For the **SCU**, change the `DICOM_FOLDER` in `test.py` to point to the folder that contains the DICOM files you want to send.

## Structure

The codebase is structured as follows:

