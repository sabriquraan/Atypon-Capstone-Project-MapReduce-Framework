FROM ubuntu:latest

RUN apt-get update -y
RUN apt install openjdk-11-jdk -y
RUN apt install default-jre -y
COPY ./runMapper.sh /
COPY ./runReducer.sh /

RUN echo 88888888888888

RUN chmod +x run*

CMD /bin/bash
