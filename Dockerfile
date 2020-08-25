FROM ubuntu:18.04

ENV LANG=C.UTF-8

WORKDIR /usr/src/app
COPY . .

RUN useradd jenkins -d /usr/src/app && echo "jenkins:jenkins" | chpasswd

RUN chown -R jenkins:jenkins /usr/src/app

RUN apt-get update && \
	apt-get -y install sudo

RUN apt-get install -q -y openjdk-8-jdk && \
    apt-get install -y python3-pip python3.6

RUN pip3 install --upgrade pip && \
    pip3 install pipenv

ENV JAVA_HOME  /usr/lib/jvm/java-8-openjdk-amd64
