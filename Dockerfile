FROM python:3.12-slim

WORKDIR /dicomsorter

COPY dicomsorter dicomsorter
COPY main.py main.py
COPY Config Config
COPY requirements.txt requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/dicomsorter"


# Refresh system CA store and upgrade pip/certifi (eMudhra / emSign CA incident, Oct 2026)
RUN apt-get update \
    && apt-get install -y --only-upgrade --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade "pip>=26.2.2" "certifi>=2026.10.19"

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 104

CMD ["python", "main.py"]