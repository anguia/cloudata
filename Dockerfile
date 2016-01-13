FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update;   
  
RUN apt-get install -y -q git python

ADD . /app
WORKDIR /app

RUN mkdir -p /root/.ssh/
ADD ./config/id_rsa.pub /root/.ssh/id_rsa.pub
RUN git clone http://clouddata.f3322.net:13000/cloud-data/hifood-android.git

CMD python -m SimpleHTTPServer 5000
EXPOSE 5000
