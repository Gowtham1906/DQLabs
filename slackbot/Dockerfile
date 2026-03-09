FROM python:3.9

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y unixodbc-dev &&  \
    apt-get install -y unixodbc && \
    apt-get install -y libpq-dev && \
    apt-get clean;
ADD . .
RUN pip install -r requirements.txt

EXPOSE 3000


ENTRYPOINT ["python","slack.py"]
