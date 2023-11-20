FROM python:3.11

WORKDIR /app

# Python Scripts are in ETL folder
# So copy entire directory
COPY ETL/* ETL/
COPY ETL/data_insertion.sql /app/data_insertion.sql

# Run requirements.txt to install all necessary packages
RUN pip install -r ETL/requirements.txt

# Where to start on container build
ENTRYPOINT [ "python", "ETL/pipeline.py" ]
