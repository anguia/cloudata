FROM ubuntu:14.04  
  
MAINTAINER Wuxia <anguia@sina.com>  
  
RUN apt-get update; \  
	apt-get -y upgrade  
  
RUN apt-get install -y git python-virtualenv 
               git python-virtualenv  
# Get github resource
RUN mkdir /home/git; \  
	cd /home/git; \  
	sudo git clone https://github.com/anguia/test.git -b master; \  

# Add the install commands
ADD ./install.sh
ADD ./run.sh

# Run the install script
RUN /home/git/install.sh
RUN mkdir /home/project; \
	cd /home/project; \
	sudo git clone http://clouddata.f3322.net:13000/cloud-data/hifood-android.git;\
RUN /home/git/run.sh

