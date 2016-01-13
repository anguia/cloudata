FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update;   
  
RUN apt-get install -y -q git python-all

ADD . /app
WORKDIR /app

CMD python -m SimpleHTTPServer 5000
EXPOSE 5000
