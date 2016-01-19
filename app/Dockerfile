FROM tomcat:7-jre7
MAINTAINER Wuxia <anguia@sina.com>
RUN rm -rf /usr/local/tomcat/webapps/*
ADD ./WlanBIPortal.war /usr/local/tomcat/webapps/

RUN ls /usr/local/tomcat/webapps/

ENV POSTGRES_HOST **ChangeMe**
ENV POSTGRES_PORT 5432
ENV POSTGRES_DB_NAME test
ENV POSTGRES_USER root
ENV POSTGRES_PASS root

ADD run.sh /run.sh
ADD set_postgres_db.sh /set_postgres_db.sh
ADD set_tomcat.sh /set_tomcat.sh
RUN chmod +x /*.sh

CMD ["/run.sh"]
