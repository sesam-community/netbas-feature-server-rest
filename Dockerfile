FROM python:3.6-alpine

COPY ./service /service

RUN apk update \
  && apk add gcc libc-dev g++ \
  && apk add libffi-dev libxml2 libffi-dev \
  && apk add unixodbc-dev mariadb-dev python3-dev

RUN pip install --upgrade pip

RUN pip install -r /service/requirements.txt

EXPOSE 5000/tcp

CMD ["python3", "-u", "./service/netbas-feature.py"]
