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
RUN ./install.sh
RUN ./run.sh

