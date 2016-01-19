#!/bin/bash
set -e
if [ -f /.tomcat_configured ]; then
	echo "=> Tomcat has been configured!"
	exit 0
fi

echo "=> unzip app War"

rm -rf /usr/local/tomcat/webapps/WlanBIPortal
unzip /usr/local/tomcat/webapps/WlanBIPortal.war -d /usr/local/tomcat/webapps/WlanBIPortal
rm -rf /usr/local/tomcat/webapps/WlanBIPortal.war

echo "=> Configure Tomcat JDBC Connection"

sed -i -e "s/<--POSTGRES_HOST-->/${POSTGRES_HOST}/g" \
	-e "s/<--POSTGRES_PORT-->/${POSTGRES_PORT}/g" \
	-e "s/<--POSTGRES_DB_NAME-->/${POSTGRES_DB_NAME}/g" \
	-e "s/<--POSTGRES_USER-->/${POSTGRES_USER}/g" \
	-e "s/<--POSTGRES_PASS-->/${POSTGRES_PASS}/g" /usr/local/tomcat/webapps/WlanBIPortal/WEB-INF/classes/jdbc.properties

touch /.tomcat_configured

echo "=> Configure Tomcat JDBC Connection as follows:"
echo "   jdbc.url=jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB_NAME}"
echo "   jdbc.username=${POSTGRES_USER}"
echo "   jdbc.password=${POSTGRES_PASS}"

echo "   ** Please check your environment variables if you find somethin is misconfigured.**"
echo "=> Done!"
