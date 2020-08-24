FROM ubuntu:18.04

ENV LANG=C.UTF-8

WORKDIR /usr/src/app
COPY . .

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -q -y openjdk-8-jdk && \
    apt-get install -y python3-pip python3.6
    
RUN java -version
RUN cd /usr/lib/jvm/java-8-openjdk-amd64 && ls -lrt

RUN pip3 install --upgrade pip && \
    pip3 install pipenv

RUN chmod 777 /usr/src/app

ENV PIPENV_CACHE_DIR = /usr/src/app
	PIPENV_VENV_IN_PROJECT = 1
	JAVA_HOME = /usr/lib/jvm/java-8-openjdk-amd64
