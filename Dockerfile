FROM python:3-alpine

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY Collectors /app
WORKDIR /app

EXPOSE 9930/udp
EXPOSE 8000/tcp
CMD [ "/app/DetailedCollector.py", "/configs/connection.conf" ]
