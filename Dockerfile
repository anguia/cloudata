FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update;   
  
RUN apt-get install -y -q git python

ADD . /app
WORKDIR /app

RUN mkdir -p /root/.ssh/ 

ADD ./config/id_rsa /root/.ssh/id_rsa

RUN cat /root/.ssh/id_rsa
RUN ls /root/.ssh/
RUN git clone ssh://git@clouddata.f3322.net:10025/cloud-data/webdemo.git 

CMD python -m SimpleHTTPServer 5000
EXPOSE 5000
