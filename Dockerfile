FROM python:3-alpine

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY Collectors /app
COPY configs /configs
COPY /scripts/run.sh /app/run.sh
WORKDIR /app

EXPOSE 9930/udp
EXPOSE 9931/udp
EXPOSE 8000/tcp
EXPOSE 8001/tcp
CMD [ "/app/run.sh" ]
