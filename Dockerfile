FROM ubuntu:18.04

ENV LANG=C.UTF-8

WORKDIR /usr/src/app
COPY . .

RUN apt-get update && \
    apt-get install -y software-properties-common &&\ 
	add-apt-repository -y ppa:webupd8team/java && \
    apt-get upgrade -y && \
    apt-get install -q -y oracle-java8-installer && \
    apt-get install -y python3-pip python3.6
    
RUN java -version
RUN update-alternatives --config java
RUN cd /usr/lib/jvm/java-8-oracle && ls -lrt

RUN pip3 install --upgrade pip && \
    pip3 install pipenv

RUN chmod 777 /usr/src/app

RUN which python3

ENV PIPENV_CACHE_DIR  /usr/src/app
ENV PIPENV_VENV_IN_PROJECT  1
ENV JAVA_HOME  /usr/lib/jvm/java-8-oracle
ENV PYSPARK_DRIVER_PYTHON  /usr/bin/python3
ENV PYSPARK_SUBMIT_ARGS "--master local pyspark-shell"
ENV PATH="/usr/lib/jvm/java-8-oracle/bin:/usr/bin:${PATH}"