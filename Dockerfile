FROM python:3.12-slim

WORKDIR /dicomsorter

COPY dicomsorter dicomsorter
COPY config_handler.py config_handler.py
COPY anonymization anonymization
COPY main.py main.py
COPY Config Config
COPY requirements.txt requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/dicomsorter"


RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 104
EXPOSE 9000


# Use a shell script to run both processes
COPY start.sh start.sh
RUN chmod +x start.sh

CMD ["./start.sh"]