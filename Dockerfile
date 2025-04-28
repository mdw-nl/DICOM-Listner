FROM python:3.12-slim

WORKDIR /dicomsorter

COPY dicomsorter dicomsorter
COPY main.py main.py
COPY requirements.txt requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/dicomsorter"


RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 104

CMD ["python", "main.py"]