FROM python:3.12-slim

WORKDIR /dicomsorter

COPY dicomsorter dicomsorter
COPY anonymization anonymization
COPY Config Config
COPY recipes recipes
COPY config_handler.py config_handler.py
COPY main.py main.py
COPY anonymizer_worker.py anonymizer_worker.py
COPY xnat_worker.py xnat_worker.py
COPY requirements.txt requirements.txt
COPY start.sh start.sh

ENV PYTHONPATH="${PYTHONPATH}:/dicomsorter"

RUN pip install --no-cache-dir -r requirements.txt \
    && chmod +x start.sh

EXPOSE 104
EXPOSE 9000

CMD ["./start.sh"]
