#!/bin/bash
# start.sh

# Start FastAPI server in background
uvicorn dicomsorter.api:app --host 0.0.0.0 --port 9000 &

# Start the DICOM listener in the foreground
python main.py
