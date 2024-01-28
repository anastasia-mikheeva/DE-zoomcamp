FROM python:3.9-alpine

RUN apk add --update wget
#RUN apt-get install wget
RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2-binary

ARG USER
ARG PW
ARG HOST
ARG PORT
ARG DB
ARG TN
ARG ZTN
ARG URLDATA
ARG URLZONES

ENV user=${USER}
ENV password=${PW}
ENV host=${HOST}
ENV port=${PORT}
ENV db=${DB}
ENV table_name=${TN}
ENV zones_table_name=${ZTN}
ENV urldata=${URLDATA}
ENV urlzones=${URLZONES}


WORKDIR /app
COPY ingest_data.py ingest_data.py

CMD python ingest_data.py --user=${user} --password=${password} --host=${host} --port=${port} --db=${db} --table_name=${table_name} --zones_table_name=${zones_table_name} --url_data=${urldata} --url_zones=${urlzones}