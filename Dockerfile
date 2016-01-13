FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update; \  
	apt-get -y upgrade  
  
RUN apt-get install -y git


RUN mkdir -p /root/.ssh \
ADD ./id_rsa.pub /root/.ssh/id_rsa.pub
RUN chmod 700 /root/.ssh/id_rsa.pub

RUN sudo git clone http://clouddata.f3322.net:13000/cloud-data/hifood-android.git;\

