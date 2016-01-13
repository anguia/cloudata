FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update;   
  
RUN apt-get install -y -q git python openssh-client

ADD . /app
WORKDIR /app

RUN mkdir -p /root/.ssh/ 

ADD ./config/id_rsa /root/.ssh/id_rsa
RUN chmod 700 /root/.ssh/id_rsa
RUN chown -R root:root /root/.ssh

# Create known_hosts
RUN touch /root/.ssh/known_hosts
# Remove host checking
RUN echo "Host clouddata.f3322.net:10025\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config


RUN cat /root/.ssh/id_rsa
RUN ls /root/.ssh/
RUN git clone ssh://git@clouddata.f3322.net:10025/cloud-data/webdemo.git 

CMD python -m SimpleHTTPServer 5000
EXPOSE 5000
