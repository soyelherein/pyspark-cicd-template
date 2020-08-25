FROM ubuntu:18.04

ENV LANG=C.UTF-8

WORKDIR /usr/src/app
COPY . .

RUN useradd docker -d /usr/src/app && echo "docker:docker" | chpasswd

RUN chown -R docker:docker /usr/src/app

RUN whoami

RUN echo $HOME

RUN apt-get update && \
	apt-get -y install sudo

RUN apt-get install -q -y openjdk-8-jdk && \
    apt-get install -y python3-pip python3.6

RUN java -version
RUN cd /usr/lib/jvm/java-8-openjdk-amd64 && ls -lrt

RUN pip3 install --upgrade pip && \
    pip3 install pipenv

RUN chmod 777 /usr/src/app

RUN which python3



ENV PIPENV_CACHE_DIR  /usr/src/app
ENV HOME  /usr/src/app
ENV PIPENV_VENV_IN_PROJECT  1
ENV JAVA_HOME  /usr/lib/jvm/java-8-openjdk-amd64
ENV PYSPARK_SUBMIT_ARGS --master local pyspark-shell --conf spark.jars.ivy=/tmp/.ivy
ENV PATH="/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:${PATH}"
